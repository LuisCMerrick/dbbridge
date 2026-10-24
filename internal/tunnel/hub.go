package tunnel

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"sort"
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

// TokenSource returns the current token of a reverse peer (the peer's
// outbound credential, which also authenticates its tunnels).
type TokenSource func(peerID string) string

// Hub is the public side of reverse mode. It accepts tunnels on
// GET /v1/reverse, verifies who dialed them and hands out streams on them to
// the ingress role's reverse peers.
type Hub struct {
	nodeID, instanceID string
	tcfg               *config.TransportConfig
	codec              *resource.Budget
	tokens             TokenSource
	upgrader           websocket.Upgrader

	mu     sync.Mutex
	peers  map[string]*hubPeer
	closed bool
	wg     sync.WaitGroup
	// rejected counts refused tunnel handshakes (tests, metrics).
	rejected atomic.Int64
	// accepted counts registered tunnels; rejectedBy counts refusals by
	// DBBridge error code (metrics). Both are guarded by statsMu.
	accepted   atomic.Int64
	statsMu    sync.Mutex
	rejectedBy map[string]int64
	// pollWait replaces defaultPollWait in tests.
	pollWait time.Duration
}

type hubPeer struct {
	id, expectedNode string
	tunnels          map[string]*Tunnel
}

// Tunnel is one registered tunnel on the public side.
type Tunnel struct {
	ID, Peer, Instance string
	Priority           int
	Remote             string
	// Transport is ws, http-sse or long-poll.
	Transport string
	digest    [32]byte
	sess      *smux.Session
	conn      net.Conn
	// pipe is the byte stream of a tunnel carried over HTTP requests; nil
	// for a WebSocket tunnel.
	pipe      *pipe
	release   func()
	closeOnce sync.Once

	readerMu     sync.Mutex
	readerCancel context.CancelFunc
}

// takeReader starts a request that reads the public side's bytes (an SSE
// stream or a poll). A tunnel has one such reader: a newer one ends the
// previous one, whose connection the database side has given up on.
func (t *Tunnel) takeReader(parent context.Context, life time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parent, life)
	t.readerMu.Lock()
	if t.readerCancel != nil {
		t.readerCancel()
	}
	t.readerCancel = cancel
	t.readerMu.Unlock()
	return ctx, cancel
}

func (t *Tunnel) close() {
	t.closeOnce.Do(func() {
		_ = t.sess.Close()
		_ = t.conn.Close()
		if t.release != nil {
			t.release()
		}
	})
}

// NewHub returns a hub for the reverse peers of cfg (validated). tokens
// supplies each peer's current token; nil uses the tokens in cfg.
func NewHub(cfg *config.Config, tokens TokenSource) *Hub {
	h := &Hub{nodeID: cfg.Node.ID, instanceID: cfg.Node.InstanceID, tcfg: &cfg.Transport, tokens: tokens, peers: map[string]*hubPeer{}}
	if cfg.Resources != nil {
		h.codec = cfg.Resources.Codec
	}
	if h.tokens == nil {
		static := map[string]string{}
		for _, p := range cfg.Peers {
			static[p.ID] = p.Token
		}
		h.tokens = func(id string) string { return static[id] }
	}
	for _, p := range cfg.Peers {
		if p.IsReverse() {
			h.peers[p.ID] = &hubPeer{id: p.ID, expectedNode: p.ExpectedNodeID, tunnels: map[string]*Tunnel{}}
		}
	}
	hs := cfg.Transport.WS.HandshakeTimeout
	if hs <= 0 {
		hs = 5 * time.Second
	}
	h.upgrader = websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096, Subprotocols: []string{protocol.ReverseSubprotocol}, HandshakeTimeout: hs, CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == "" }}
	return h
}

var tunnelIDPattern = regexp.MustCompile(`^tn_[0-9a-f]{32}$`)

// handshakeError is a refused tunnel handshake.
type handshakeError struct {
	status    int
	code, msg string
	retry     bool
}

func (e *handshakeError) Error() string { return fmt.Sprintf("%d %s: %s", e.status, e.code, e.msg) }

func refuse(status int, code, msg string, retry bool) *handshakeError {
	return &handshakeError{status: status, code: code, msg: msg, retry: retry}
}

// tunnelRequest is a verified handshake.
type tunnelRequest struct {
	peer             *hubPeer
	id, instance     string
	priority         int
	digest           [32]byte
	remote, nodeSeen string
}

// authenticate maps the bearer token to a reverse peer. Every peer's token
// is compared in constant time, so neither the match position nor token
// lengths leak.
func (h *Hub) authenticate(r *http.Request) (*hubPeer, [32]byte, *handshakeError) {
	p := strings.SplitN(r.Header.Get(protocol.HeaderPeerAuth), " ", 2)
	if len(p) != 2 || !strings.EqualFold(p[0], "Bearer") || p[1] == "" || strings.TrimSpace(p[1]) != p[1] {
		return nil, [32]byte{}, refuse(401, protocol.ErrUnauthorized, "unauthorized", false)
	}
	presented := sha256.Sum256([]byte(p[1]))
	var matched *hubPeer
	for _, id := range h.peerIDs() {
		tok := h.tokens(id)
		if tok == "" {
			continue
		}
		d := sha256.Sum256([]byte(tok))
		if subtle.ConstantTimeCompare(d[:], presented[:]) == 1 {
			matched = h.peers[id]
		}
	}
	if matched == nil {
		return nil, [32]byte{}, refuse(401, protocol.ErrUnauthorized, "unauthorized", false)
	}
	return matched, presented, nil
}

func (h *Hub) peerIDs() []string {
	ids := make([]string, 0, len(h.peers))
	for id := range h.peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// verify checks a WebSocket handshake request without changing any state.
func (h *Hub) verify(r *http.Request) (*tunnelRequest, *handshakeError) {
	if r.Method != http.MethodGet {
		return nil, refuse(405, protocol.ErrBadFrame, "method", false)
	}
	q, herr := h.verifyIdentity(r)
	if herr != nil {
		return q, herr
	}
	offered := websocket.Subprotocols(r)
	ok, other := false, false
	for _, v := range offered {
		if v == protocol.ReverseSubprotocol {
			ok = true
		} else if strings.HasPrefix(v, protocol.ReverseSubprotocolPrefix) {
			other = true
		}
	}
	if !ok {
		if other {
			return q, refuse(409, protocol.ErrProtocolMismatch, "protocol", false)
		}
		return q, refuse(409, protocol.ErrTransportUnsupported, "subprotocol", false)
	}
	if !websocket.IsWebSocketUpgrade(r) {
		// Something in between dropped the upgrade: the database side
		// may fall back to an HTTP carrier.
		return q, refuse(426, protocol.ErrTransportUnsupported, "not a WebSocket upgrade", false)
	}
	return q, nil
}

// verifyIdentity checks the token, both node identities, the tunnel ID,
// instance and priority of a tunnel handshake (WebSocket or HTTP open).
func (h *Hub) verifyIdentity(r *http.Request) (*tunnelRequest, *handshakeError) {
	peer, digest, herr := h.authenticate(r)
	if herr != nil {
		return nil, herr
	}
	q := &tunnelRequest{peer: peer, digest: digest, remote: r.RemoteAddr}
	q.nodeSeen = r.Header.Get(protocol.HeaderNodeID)
	if q.nodeSeen != peer.expectedNode {
		return q, refuse(403, protocol.ErrNodeMismatch, "node identity", false)
	}
	if r.Header.Get(protocol.HeaderExpectedNodeID) != h.nodeID {
		return q, refuse(403, protocol.ErrNodeMismatch, "expected node", false)
	}
	q.id = r.Header.Get(protocol.HeaderTunnelID)
	q.instance = r.Header.Get(protocol.HeaderInstanceID)
	if !tunnelIDPattern.MatchString(q.id) || q.instance == "" || len(q.instance) > 128 {
		return q, refuse(400, protocol.ErrBadFrame, "tunnel identity", false)
	}
	prio, err := strconv.Atoi(r.Header.Get(protocol.HeaderTunnelPriority))
	if err != nil || prio < 0 {
		return q, refuse(400, protocol.ErrBadFrame, "tunnel priority", false)
	}
	q.priority = prio
	return q, nil
}

// checkConflict refuses a duplicate tunnel ID, tunnels of a second instance
// of the peer while the first still has live tunnels, and tunnels beyond
// MaxTunnelsPerPeer. Callers hold h.mu.
func (h *Hub) checkConflict(q *tunnelRequest) *handshakeError {
	if h.closed {
		return refuse(503, protocol.ErrDraining, "stopped", true)
	}
	if _, dup := q.peer.tunnels[q.id]; dup {
		return refuse(409, protocol.ErrTunnelConflict, "tunnel id already registered", false)
	}
	for _, t := range q.peer.tunnels {
		if t.Instance != q.instance && !t.sess.IsClosed() {
			return refuse(409, protocol.ErrTunnelConflict, "another instance of this peer holds live tunnels", true)
		}
	}
	if len(q.peer.tunnels) >= config.MaxTunnelsPerPeer {
		return refuse(429, protocol.ErrQuotaExceeded, "tunnels per peer", true)
	}
	return nil
}

// Serve handles GET /v1/reverse (WebSocket tunnels) and
// /v1/reverse/{tunnel}/... (tunnels carried over HTTP requests). authenticated is called once the
// bearer token matched a reverse peer, before the upgrade (the HTTP server
// uses it to lift its pre-authentication limits for the socket).
func (h *Hub) Serve(w http.ResponseWriter, r *http.Request, authenticated func()) {
	if r.URL.Path != protocol.ReverseTunnelPath {
		h.serveHTTP(w, r, authenticated)
		return
	}
	q, herr := h.verify(r)
	if q != nil && authenticated != nil {
		// The token matched a reverse peer (even if the identity check
		// failed): the socket is no longer anonymous.
		authenticated()
	}
	if herr == nil {
		h.mu.Lock()
		herr = h.checkConflict(q)
		h.mu.Unlock()
	}
	var release func()
	if herr == nil {
		need := ReceiveBuffer(h.tcfg)
		if h.codec != nil {
			if !h.codec.Try(need) {
				herr = refuse(503, protocol.ErrBufferExhausted, "codec budget", true)
			} else {
				release = func() { h.codec.Put(need) }
			}
		}
	}
	if herr != nil {
		h.reject(w, r, q, herr)
		return
	}
	hdr := http.Header{"Cache-Control": []string{"no-store"}, protocol.HeaderNodeID: []string{h.nodeID}, protocol.HeaderInstanceID: []string{h.instanceID}}
	c, err := h.upgrader.Upgrade(transport.WebSocketResponseWriter{ResponseWriter: w}, r, hdr)
	if err != nil {
		if release != nil {
			release()
		}
		return
	}
	_ = c.NetConn().SetDeadline(time.Time{})
	conn := newWSConn(c, Addr("tunnel/"+q.id))
	sess, err := smux.Client(conn, smuxConfig(h.tcfg))
	if err != nil {
		_ = conn.Close()
		if release != nil {
			release()
		}
		return
	}
	t := &Tunnel{ID: q.id, Peer: q.peer.id, Instance: q.instance, Priority: q.priority, Remote: q.remote, Transport: "ws", digest: q.digest, sess: sess, conn: conn, release: release}
	if herr = h.register(q, t); herr != nil {
		slog.Warn("reverse tunnel refused after upgrade", "peer", q.peer.id, "tunnel", q.id, "code", herr.code, "reason", herr.msg)
	}
}

// register adds t unless a conflict appeared since the handshake was
// checked; then t is closed and the conflict returned.
func (h *Hub) register(q *tunnelRequest, t *Tunnel) *handshakeError {
	h.mu.Lock()
	if herr := h.checkConflict(q); herr != nil {
		h.mu.Unlock()
		t.close()
		return herr
	}
	q.peer.tunnels[q.id] = t
	h.wg.Add(1)
	h.mu.Unlock()
	h.accepted.Add(1)
	slog.Info("reverse tunnel registered", "peer", t.Peer, "tunnel", t.ID, "transport", t.Transport, "instance", t.Instance, "priority", t.Priority, "remote", t.Remote)
	go h.watch(t)
	return nil
}

// watch removes t once its session ends.
func (h *Hub) watch(t *Tunnel) {
	defer h.wg.Done()
	// smux does not close a session when its connection fails; it only
	// fails AcceptStream. The database side never opens streams, so
	// AcceptStream returns only on a read error or close, and that ends
	// the tunnel at once instead of after the keepalive timeout.
	go func() {
		for {
			st, err := t.sess.AcceptStream()
			if err != nil {
				t.close()
				return
			}
			_ = st.Close()
		}
	}()
	<-t.sess.CloseChan()
	t.close()
	h.mu.Lock()
	if p := h.peers[t.Peer]; p != nil && p.tunnels[t.ID] == t {
		delete(p.tunnels, t.ID)
	}
	h.mu.Unlock()
	slog.Info("reverse tunnel closed", "peer", t.Peer, "tunnel", t.ID, "instance", t.Instance)
}

func (h *Hub) reject(w http.ResponseWriter, r *http.Request, q *tunnelRequest, e *handshakeError) {
	h.rejected.Add(1)
	h.countRejected(e.code)
	attrs := []any{"remote", r.RemoteAddr, "code", e.code, "reason", e.msg}
	if q != nil && q.peer != nil {
		attrs = append(attrs, "peer", q.peer.id, "node_id", q.nodeSeen, "tunnel", q.id, "instance", q.instance)
	}
	if e.status == 401 || e.status == 403 || e.code == protocol.ErrTunnelConflict {
		slog.Warn("reverse tunnel rejected", attrs...)
	} else {
		slog.Debug("reverse tunnel rejected", attrs...)
	}
	if e.retry {
		w.Header().Set("Retry-After", "1")
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if e.status == 401 {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	w.WriteHeader(e.status)
	_ = json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: e.code, Message: e.msg, Retryable: e.retry})
}

// Opener returns the StreamOpener for reverse peer id: each call opens a
// stream on the best live tunnel (lowest priority value, then fewest open
// streams). With no live tunnel it fails with transport.ErrTunnelUnavailable.
func (h *Hub) Opener(id string) transport.StreamOpener {
	return func(ctx context.Context) (net.Conn, error) {
		for attempt := 0; attempt < 4; attempt++ {
			t := h.pick(id)
			if t == nil {
				return nil, transport.ErrTunnelUnavailable
			}
			s, err := openStream(ctx, t.sess)
			if err == nil {
				return ownStream(s), nil
			}
			if ctx.Err() != nil {
				return nil, err
			}
			// The tunnel is failing; let the next attempt pick another.
			t.close()
		}
		return nil, transport.ErrTunnelUnavailable
	}
}

func (h *Hub) pick(id string) *Tunnel {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := h.peers[id]
	if p == nil || h.closed {
		return nil
	}
	var best *Tunnel
	bestStreams := 0
	for _, t := range p.tunnels {
		if t.sess.IsClosed() {
			continue
		}
		n := t.sess.NumStreams()
		if best == nil || t.Priority < best.Priority || t.Priority == best.Priority && (n < bestStreams || n == bestStreams && t.ID < best.ID) {
			best, bestStreams = t, n
		}
	}
	return best
}

// openStream opens a stream bounded by ctx. smux's OpenStream has no
// context; a stream that opens after ctx ended is closed.
func openStream(ctx context.Context, s *smux.Session) (net.Conn, error) {
	type result struct {
		st  *smux.Stream
		err error
	}
	ch := make(chan result, 1)
	go func() {
		st, err := s.OpenStream()
		ch <- result{st, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		return r.st, nil
	case <-ctx.Done():
		go func() {
			if r := <-ch; r.st != nil {
				_ = r.st.Close()
			}
		}()
		return nil, ctx.Err()
	}
}

// Revalidate closes every tunnel whose token is no longer its peer's
// current token (rotation or revocation on a credentials reload).
func (h *Hub) Revalidate() {
	h.mu.Lock()
	var stale []*Tunnel
	for _, id := range h.peerIDs() {
		tok := h.tokens(id)
		cur := sha256.Sum256([]byte(tok))
		for _, t := range h.peers[id].tunnels {
			if tok == "" || subtle.ConstantTimeCompare(cur[:], t.digest[:]) != 1 {
				stale = append(stale, t)
			}
		}
	}
	h.mu.Unlock()
	for _, t := range stale {
		slog.Warn("reverse tunnel closed: its token was rotated or revoked", "peer", t.Peer, "tunnel", t.ID)
		t.close()
	}
}

// Tunnels returns the number of live tunnels of reverse peer id.
func (h *Hub) Tunnels(id string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	if p := h.peers[id]; p != nil {
		for _, t := range p.tunnels {
			if !t.sess.IsClosed() {
				n++
			}
		}
	}
	return n
}

// CloseTunnels closes every tunnel of reverse peer id (fault injection and
// operations) and reports how many were open.
func (h *Hub) CloseTunnels(id string) int {
	h.mu.Lock()
	var ts []*Tunnel
	if p := h.peers[id]; p != nil {
		for _, t := range p.tunnels {
			ts = append(ts, t)
		}
	}
	h.mu.Unlock()
	for _, t := range ts {
		t.close()
	}
	return len(ts)
}

// Rejected returns the number of refused tunnel handshakes.
func (h *Hub) Rejected() int64 { return h.rejected.Load() }

// Close refuses new tunnels, closes every registered one and waits for
// their watchers.
func (h *Hub) Close() {
	h.mu.Lock()
	h.closed = true
	var ts []*Tunnel
	for _, p := range h.peers {
		for _, t := range p.tunnels {
			ts = append(ts, t)
		}
	}
	h.mu.Unlock()
	for _, t := range ts {
		t.close()
	}
	done := make(chan struct{})
	go func() { h.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		slog.Warn("reverse tunnel hub close timed out")
	}
}
