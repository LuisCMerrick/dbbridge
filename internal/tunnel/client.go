package tunnel

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"dbbridge/internal/transport"

	"github.com/gorilla/websocket"
	"github.com/xtaci/smux"
)

// Client is the database side of reverse mode. It keeps
// reverse.tunnels_per_endpoint tunnels to the public-side node and delivers
// every stream the public side opens to a Listener the egress HTTP server
// serves.
//
// Endpoints are used by priority: slots of an endpoint dial only while no
// endpoint with a better (lower) priority has a live tunnel. Tunnels that are
// already up stay up when a better endpoint returns; new tunnels go to it
// (like failover.failback: new_sessions).
type Client struct {
	cfg   *config.Config
	ln    *Listener
	codec *resource.Budget
	// keep is the codec this side's tunnel charges leave free (the local
	// ingress reserve of a bidirectional node).
	keep           func() int64
	streamsRefused atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	live    map[string]*clientTunnel // tunnel ID -> tunnel
	started bool
	// wake is closed and replaced whenever a tunnel goes down, so slots
	// waiting behind a better endpoint re-check.
	wake chan struct{}

	// modes holds each endpoint's current transport (auto falls back per
	// endpoint and retries WebSocket after upgradeRetry).
	modes map[string]*endpointMode
	// fallbacks counts transport fallbacks (tests, metrics).
	fallbacks atomic.Int64
	// established, lost and dialFailures count tunnel lifecycle events;
	// fallbackBy counts fallbacks by "from>to" (guarded by mu). Metrics.
	established, lost, dialFailures atomic.Int64
	fallbackBy                      map[string]int64
	hc                              *http.Client

	tlsConfig *tls.Config
	// dialHook replaces the WebSocket dial in tests.
	dialHook func(ctx context.Context, ep config.EndpointConfig, id string) (*websocket.Conn, error)
}

type clientTunnel struct {
	id        string
	endpoint  string
	priority  int
	transport string
	sess      *smux.Session
	conn      net.Conn
}

type endpointMode struct {
	mode  string
	since time.Time
}

// upgradeRetry is how long auto keeps an endpoint on a fallback transport
// before it tries WebSocket (then SSE) again for the next tunnel.
const upgradeRetry = 10 * time.Minute

// NewClient returns a client for cfg.Reverse (validated, enabled) that
// delivers streams to ln.
func NewClient(cfg *config.Config, ln *Listener) *Client {
	c := &Client{cfg: cfg, ln: ln, live: map[string]*clientTunnel{}, wake: make(chan struct{}), modes: map[string]*endpointMode{}}
	if cfg.Resources != nil {
		c.codec = cfg.Resources.Codec
		c.keep = cfg.Resources.IngressCodecReserve
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.tlsConfig = clientTLS(&cfg.Reverse.TLS)
	return c
}

func clientTLS(t *config.TLSClientConfig) *tls.Config {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: !t.IsVerify(), NextProtos: []string{"http/1.1"}}
	if t.CAFile != "" {
		pool := x509.NewCertPool()
		pem, err := os.ReadFile(t.CAFile)
		if err != nil || !pool.AppendCertsFromPEM(pem) {
			// Fail closed: an empty pool trusts nothing.
			slog.Error("cannot load reverse.tls.ca_file; tunnels will fail TLS verification", "ca_file", t.CAFile, "error", err)
			pool = x509.NewCertPool()
		}
		cfg.RootCAs = pool
	}
	return cfg
}

// Start begins dialing. It may be called once.
func (c *Client) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return errors.New("reverse tunnel client already started")
	}
	if c.ctx.Err() != nil {
		return errors.New("reverse tunnel client is closed")
	}
	switch c.cfg.Reverse.Transport {
	case "", "auto", modeWS, modeSSE, modePoll:
	default:
		return fmt.Errorf("reverse.transport %q is not supported", c.cfg.Reverse.Transport)
	}
	c.started = true
	if !c.cfg.Reverse.TLS.IsVerify() {
		slog.Warn("TLS certificate verification is DISABLED for reverse tunnels (reverse.tls.verify=false); tunnel credentials and all traffic are exposed to man-in-the-middle attacks")
	}
	for _, ep := range c.cfg.Reverse.Endpoints {
		for i := 0; i < c.cfg.Reverse.TunnelsPerEndpoint; i++ {
			c.wg.Add(1)
			go c.slot(ep, i)
		}
	}
	return nil
}

// Close stops dialing, closes every tunnel and waits for the slots.
func (c *Client) Close() {
	c.cancel()
	c.mu.Lock()
	ts := make([]*clientTunnel, 0, len(c.live))
	for _, t := range c.live {
		ts = append(ts, t)
	}
	c.mu.Unlock()
	for _, t := range ts {
		_ = t.sess.Close()
		_ = t.conn.Close()
	}
	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		slog.Warn("reverse tunnel client close timed out")
	}
	c.mu.Lock()
	hc := c.hc
	c.mu.Unlock()
	if hc != nil {
		hc.CloseIdleConnections()
	}
}

// Transports returns the number of live tunnels per transport.
func (c *Client) Transports() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := map[string]int{}
	for _, t := range c.live {
		m[t.transport]++
	}
	return m
}

// Fallbacks returns how many times auto fell back to a slower transport.
func (c *Client) Fallbacks() int64 { return c.fallbacks.Load() }

// modeFor returns the transport the next tunnel to ep starts with.
func (c *Client) modeFor(ep string) string {
	switch c.cfg.Reverse.Transport {
	case modeWS, modeSSE, modePoll:
		return c.cfg.Reverse.Transport
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.modes[ep]
	if m == nil {
		return modeWS
	}
	if time.Since(m.since) >= upgradeRetry {
		delete(c.modes, ep)
		slog.Info("reverse tunnel transport: retrying WebSocket", "endpoint", ep, "after", m.mode)
		return modeWS
	}
	return m.mode
}

// fallback returns the transport auto tries after mode failed with err, or
// false when err is not one that a slower transport can get past.
//
// WebSocket falls back on an answer that did not come from DBBridge
// (redirects, 404, 426, gateway errors, a 200 instead of the upgrade) and
// on DBB_TRANSPORT_UNSUPPORTED; never on 401, on a DBBridge refusal such as
// a verified 403, or on a network error. SSE falls back to long polling
// when its first event does not arrive in time (a buffering intermediary)
// or the event stream gets such a non-DBBridge answer.
func (c *Client) fallback(ep, mode string, err error) (string, bool) {
	if t := c.cfg.Reverse.Transport; t != "" && t != "auto" {
		return "", false
	}
	var re *refusedError
	plain := errors.As(err, &re) && re.status != 401 && (re.code == "" || re.code == protocol.ErrTransportUnsupported)
	next := ""
	switch {
	case mode == modeWS && plain:
		next = modeSSE
	case mode == modeSSE && (plain || errors.Is(err, errSSEBuffered)):
		next = modePoll
	default:
		return "", false
	}
	c.fallbacks.Add(1)
	c.mu.Lock()
	if c.fallbackBy == nil {
		c.fallbackBy = map[string]int64{}
	}
	c.fallbackBy[mode+">"+next]++
	c.modes[ep] = &endpointMode{mode: next, since: time.Now()}
	c.mu.Unlock()
	slog.Warn("reverse tunnel transport fallback", "endpoint", ep, "from", mode, "to", next, "reason", err)
	return next, true
}

// Live returns the number of live tunnels.
func (c *Client) Live() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.live)
}

// CloseTunnels closes every live tunnel (fault injection); slots reconnect.
func (c *Client) CloseTunnels() int {
	c.mu.Lock()
	ts := make([]*clientTunnel, 0, len(c.live))
	for _, t := range c.live {
		ts = append(ts, t)
	}
	c.mu.Unlock()
	for _, t := range ts {
		_ = t.sess.Close()
	}
	return len(ts)
}

// betterLive reports whether an endpoint with a lower priority value than
// prio has a live tunnel, and returns the channel that signals a change.
func (c *Client) betterLive(prio int) (bool, <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.live {
		if t.priority < prio {
			return true, c.wake
		}
	}
	return false, c.wake
}

func (c *Client) slot(ep config.EndpointConfig, index int) {
	defer c.wg.Done()
	failures := 0
	var lastErr string
	for c.ctx.Err() == nil {
		select {
		case <-c.ln.Done():
			// The egress server stopped serving tunnel streams.
			return
		default:
		}
		if better, wake := c.betterLive(ep.Priority); better {
			select {
			case <-wake:
			case <-c.ctx.Done():
				return
			}
			continue
		}
		t, err := c.dial(ep)
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			failures++
			c.dialFailures.Add(1)
			if msg := err.Error(); msg != lastErr || failures == 1 {
				slog.Warn("reverse tunnel dial failed", "endpoint", ep.ID, "slot", index, "err", err)
				lastErr = msg
			}
			if !c.sleep(backoff(c.cfg.Reverse.Reconnect, failures)) {
				return
			}
			continue
		}
		if failures > 0 {
			slog.Info("reverse tunnel reconnected", "endpoint", ep.ID, "slot", index, "after_failures", failures)
		}
		failures, lastErr = 0, ""
		c.serve(t)
		// The first reconnect after losing a live tunnel does not wait.
	}
}

func (c *Client) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-c.ctx.Done():
		return false
	}
}

// backoff is the wait after the n-th consecutive failed dial (n >= 1):
// min_backoff doubling per failure up to max_backoff, with jitter drawn
// from the upper half of the interval.
func backoff(r config.ReverseReconnectConfig, n int) time.Duration {
	lo, hi := r.MinBackoff, r.MaxBackoff
	if lo <= 0 {
		lo = config.DefaultReverseMinBackoff
	}
	if hi < lo {
		hi = lo
	}
	d := lo
	for i := 1; i < n && d < hi; i++ {
		d *= 2
	}
	d = min(d, hi)
	half := int64(d / 2)
	if half <= 0 {
		return d
	}
	j, err := rand.Int(rand.Reader, big.NewInt(half+1))
	if err != nil {
		return d
	}
	return time.Duration(half + j.Int64())
}

func newTunnelID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return "tn_" + hex.EncodeToString(b)
}

// dial opens one tunnel to ep, falling back to slower transports as auto
// allows.
func (c *Client) dial(ep config.EndpointConfig) (*clientTunnel, error) {
	mode := c.modeFor(ep.ID)
	for {
		t, err := c.dialMode(ep, mode)
		if err == nil {
			return t, nil
		}
		next, ok := c.fallback(ep.ID, mode, err)
		if !ok {
			return nil, err
		}
		mode = next
	}
}

// dialMode opens one tunnel to ep over transport mode and starts its
// multiplexer.
func (c *Client) dialMode(ep config.EndpointConfig, mode string) (*clientTunnel, error) {
	id := newTunnelID()
	// A WebSocket tunnel holds no codec memory while idle; an HTTP-carried
	// one keeps its two pipe buffers. Stream windows are charged per
	// accepted stream (serve).
	release := func() {}
	if mode != modeWS {
		var ok bool
		if release, ok = tryCodec(c.codec, httpTunnelBudget(), c.keep); !ok {
			return nil, errors.New("codec budget exhausted for a tunnel carrier buffer")
		}
	}
	var conn net.Conn
	var err error
	if mode != modeWS {
		conn, err = c.dialHTTP(ep, id, mode)
	} else {
		var ws *websocket.Conn
		if c.dialHook != nil {
			ws, err = c.dialHook(c.ctx, ep, id)
		} else {
			ws, err = c.dialWS(ep, id)
		}
		if err == nil {
			conn = newWSConn(ws, Addr("tunnel/"+id))
		}
	}
	if err != nil {
		release()
		return nil, err
	}
	sess, err := smux.Server(conn, smuxConfig(&c.cfg.Transport))
	if err != nil {
		_ = conn.Close()
		release()
		return nil, err
	}
	t := &clientTunnel{id: id, endpoint: ep.ID, priority: ep.Priority, transport: mode, sess: sess, conn: conn}
	c.mu.Lock()
	if c.ctx.Err() != nil {
		c.mu.Unlock()
		_ = sess.Close()
		_ = conn.Close()
		release()
		return nil, c.ctx.Err()
	}
	c.live[id] = t
	c.mu.Unlock()
	c.established.Add(1)
	go func() {
		<-sess.CloseChan()
		_ = conn.Close()
		release()
	}()
	slog.Info("reverse tunnel established", "endpoint", ep.ID, "tunnel", id, "transport", mode)
	return t, nil
}

func (c *Client) dialWS(ep config.EndpointConfig, id string) (*websocket.Conn, error) {
	token, terr := c.cfg.ReverseToken()
	if terr != nil {
		slog.Warn("reverse token source unreadable; using the token loaded at startup", "err", terr)
	}
	u := "ws" + strings.TrimPrefix(ep.URL, "http") + protocol.ReverseTunnelPath
	hs := c.cfg.Transport.WS.HandshakeTimeout
	if hs <= 0 {
		hs = 10 * time.Second
	}
	d := &websocket.Dialer{Subprotocols: []string{protocol.ReverseSubprotocol}, HandshakeTimeout: hs, TLSClientConfig: c.tlsConfig.Clone(), ReadBufferSize: 32 << 10, WriteBufferSize: 32 << 10}
	nd := &net.Dialer{Timeout: c.cfg.Transport.PeerDial(), KeepAlive: 30 * time.Second}
	d.NetDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := nd.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return transport.NewIdleWriteConn(conn), nil
	}
	if c.cfg.Transport.UseEnvironmentProxy {
		d.Proxy = http.ProxyFromEnvironment
	}
	h := http.Header{}
	transport.SetUserAgent(h, &c.cfg.Transport)
	h.Set(protocol.HeaderPeerAuth, "Bearer "+token)
	h.Set(protocol.HeaderNodeID, c.cfg.Node.ID)
	h.Set(protocol.HeaderInstanceID, c.cfg.Node.InstanceID)
	h.Set(protocol.HeaderExpectedNodeID, c.cfg.Reverse.PeerNodeID)
	h.Set(protocol.HeaderTunnelID, id)
	h.Set(protocol.HeaderTunnelPriority, strconv.Itoa(ep.Priority))
	ctx, cancel := context.WithTimeout(c.ctx, c.cfg.Transport.PeerDial()+hs)
	defer cancel()
	ws, resp, err := d.DialContext(ctx, u, h)
	if err != nil {
		if resp != nil {
			e := refusal(resp)
			resp.Body.Close()
			return nil, e
		}
		return nil, err
	}
	// The public side proves its identity (after TLS) with its node ID.
	if got := resp.Header.Get(protocol.HeaderNodeID); got != c.cfg.Reverse.PeerNodeID {
		ws.Close()
		return nil, fmt.Errorf("tunnel peer node mismatch: expected %q, got %q", c.cfg.Reverse.PeerNodeID, got)
	}
	if ws.Subprotocol() != protocol.ReverseSubprotocol || resp.Header.Get(protocol.HeaderInstanceID) == "" {
		ws.Close()
		return nil, fmt.Errorf("tunnel handshake: protocol mismatch (subprotocol %q)", ws.Subprotocol())
	}
	_ = ws.NetConn().SetDeadline(time.Time{})
	return ws, nil
}

// serve delivers the streams of t to the listener until t ends.
func (c *Client) serve(t *clientTunnel) {
	defer func() {
		_ = t.sess.Close()
		c.mu.Lock()
		delete(c.live, t.id)
		close(c.wake)
		c.wake = make(chan struct{})
		c.mu.Unlock()
		if c.ctx.Err() == nil {
			c.lost.Add(1)
		}
		slog.Info("reverse tunnel lost", "endpoint", t.endpoint, "tunnel", t.id)
	}()
	for {
		st, err := t.sess.AcceptStream()
		if err != nil {
			return
		}
		// The public side opens a stream per connection it needs; this
		// side's memory for it is the stream's receive window. Without
		// room (or past the per-tunnel stream bound) the stream is closed
		// and the public side's request fails as a connection error.
		release, ok := tryCodec(c.codec, ReceiveBuffer(&c.cfg.Transport), c.keep)
		if !ok || t.sess.NumStreams() > config.MaxStreamsPerTunnel {
			if ok {
				release()
			}
			c.streamsRefused.Add(1)
			slog.Debug("reverse tunnel stream refused: codec budget or stream bound", "tunnel", t.id, "streams", t.sess.NumStreams())
			_ = st.Close()
			continue
		}
		if err := c.ln.Deliver(c.ctx, resource.LeaseConn(ownStream(st), release)); err != nil {
			release()
			_ = st.Close()
			if errors.Is(err, net.ErrClosed) || c.ctx.Err() != nil {
				return
			}
		}
	}
}
