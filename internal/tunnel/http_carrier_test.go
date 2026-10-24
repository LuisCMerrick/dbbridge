package tunnel

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/protocol"
)

func TestPipeOrderingRetransmitAndGap(t *testing.T) {
	p := newPipe(8, Addr("a"), Addr("b"))
	ctx := context.Background()
	if next, err := p.receive(ctx, 0, []byte("abc")); err != nil || next != 3 {
		t.Fatalf("receive: %d %v", next, err)
	}
	// A retransmission overlapping what arrived adds only the new tail.
	if next, err := p.receive(ctx, 1, []byte("bcde")); err != nil || next != 5 {
		t.Fatalf("overlap: %d %v", next, err)
	}
	if next, err := p.receive(ctx, 2, []byte("cd")); err != nil || next != 5 {
		t.Fatalf("duplicate: %d %v", next, err)
	}
	if _, err := p.receive(ctx, 9, []byte("x")); !errors.Is(err, errPipeGap) {
		t.Fatalf("gap: %v", err)
	}
	buf := make([]byte, 16)
	if n, _ := p.Read(buf); string(buf[:n]) != "abcde" {
		t.Fatalf("read %q", buf[:n])
	}
	// The inbound buffer is bounded: a receive beyond it waits.
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if next, err := p.receive(short, 5, []byte("0123456789")); !errors.Is(err, context.DeadlineExceeded) || next != 13 {
		t.Fatalf("full receive: %d %v", next, err)
	}

	if _, err := p.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if b, off := p.pending(canceledCtx, 0, 3); string(b) != "hel" || off != 0 {
		t.Fatalf("pending %q %d", b, off)
	}
	if err := p.ack(2); err != nil {
		t.Fatal(err)
	}
	if b, off := p.pending(canceledCtx, 0, 10); string(b) != "llo" || off != 2 {
		t.Fatalf("after ack %q %d", b, off)
	}
	if err := p.ack(1); err != nil {
		t.Fatalf("stale ack: %v", err)
	}
	if err := p.ack(6); !errors.Is(err, errPipeAck) {
		t.Fatalf("ack beyond: %v", err)
	}
	_ = p.Close()
	if _, err := p.Write([]byte("x")); err == nil {
		t.Fatal("write after close")
	}
}

// proxy sits between the database side and the hub like an HTTP
// intermediary that may drop WebSocket upgrades and buffer whole responses.
type proxy struct {
	srv       *httptest.Server
	refuseWS  atomic.Bool
	bufferSSE atomic.Bool
}

func startProxy(t *testing.T, upstream string) *proxy {
	t.Helper()
	u, _ := url.Parse(upstream)
	rp := httputil.NewSingleHostReverseProxy(u)
	rp.FlushInterval = -1
	px := &proxy{}
	px.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") != "" && px.refuseWS.Load() {
			http.Error(w, "upgrade not allowed", http.StatusBadGateway)
			return
		}
		if strings.HasSuffix(r.URL.Path, protocol.ReverseEventsSuffix) && px.bufferSSE.Load() {
			// Buffer the whole response before forwarding any of it.
			req, _ := http.NewRequestWithContext(r.Context(), r.Method, upstream+r.URL.Path, nil)
			req.Header = r.Header.Clone()
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			for k, v := range resp.Header {
				w.Header()[k] = v
			}
			w.WriteHeader(resp.StatusCode)
			_, _ = w.Write(body)
			return
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(px.srv.Close)
	return px
}

func hubHTTPURL(wsURL string) string {
	return strings.TrimSuffix(strings.Replace(wsURL, "ws", "http", 1), protocol.ReverseTunnelPath)
}

func echoListener(ln *Listener) {
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
}

// echoCheck sends n random bytes over a stream the hub opens and checks the
// echo.
func echoCheck(t *testing.T, h *Hub, n int, during func()) {
	t.Helper()
	s, err := h.Opener("plant")(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	data := make([]byte, n)
	_, _ = rand.Read(data)
	errc := make(chan error, 1)
	go func() {
		for off := 0; off < len(data); off += 16 << 10 {
			if _, err := s.Write(data[off:min(off+16<<10, len(data))]); err != nil {
				errc <- err
				return
			}
			if during != nil && off%(256<<10) == 0 {
				during()
			}
		}
		errc <- nil
	}()
	got := make([]byte, n)
	_ = s.SetReadDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.ReadFull(s, got); err != nil {
		t.Fatalf("echo read: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("echo differs")
	}
}

func startHTTPClient(t *testing.T, base, mode string) (*Client, *Listener) {
	t.Helper()
	cfg := clientConfig(t, base, "hq-b")
	cfg.Reverse.Transport = mode
	cfg.Transport.WS.HandshakeTimeout = 500 * time.Millisecond
	ln := NewListener("db")
	cl := NewClient(cfg, ln)
	if err := cl.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	echoListener(ln)
	return cl, ln
}

// Explicit http-sse and long-poll tunnels carry streams both ways.
func TestHTTPCarrierStreams(t *testing.T) {
	for _, mode := range []string{modeSSE, modePoll} {
		t.Run(mode, func(t *testing.T) {
			h, _, url := startHub(t)
			h.pollWait = 200 * time.Millisecond
			cl, _ := startHTTPClient(t, hubHTTPURL(url), mode)
			waitUntil(t, func() bool { return h.Tunnels("plant") == 2 && cl.Transports()[mode] == 2 })
			echoCheck(t, h, 2<<20, nil)
			// Idle past several poll waits, then still works.
			time.Sleep(700 * time.Millisecond)
			echoCheck(t, h, 1000, nil)
		})
	}
}

// auto falls back WS -> SSE when an intermediary refuses the upgrade, and
// on to long polling when it also buffers SSE responses.
func TestAutoFallbackThroughProxy(t *testing.T) {
	cases := []struct {
		name                string
		refuseWS, bufferSSE bool
		want                string
	}{
		{"plain", false, false, modeWS},
		{"ws refused", true, false, modeSSE},
		{"sse buffered too", true, true, modePoll},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, _, url := startHub(t)
			h.pollWait = 200 * time.Millisecond
			px := startProxy(t, hubHTTPURL(url))
			px.refuseWS.Store(tc.refuseWS)
			px.bufferSSE.Store(tc.bufferSSE)
			cl, _ := startHTTPClient(t, px.srv.URL, "auto")
			waitUntil(t, func() bool { return cl.Transports()[tc.want] == 2 && h.Tunnels("plant") == 2 })
			echoCheck(t, h, 512<<10, nil)
			if tc.want != modeWS && cl.Fallbacks() == 0 {
				t.Fatal("no fallback counted")
			}
		})
	}
}

// Dropping every carrier connection mid-transfer loses and duplicates
// nothing: requests are retried and the pipe resends unacknowledged bytes.
func TestHTTPCarrierSurvivesDroppedConnections(t *testing.T) {
	for _, mode := range []string{modeSSE, modePoll} {
		t.Run(mode, func(t *testing.T) {
			h, _, url := startHub(t)
			h.pollWait = 200 * time.Millisecond
			px := startProxy(t, hubHTTPURL(url))
			cl, _ := startHTTPClient(t, px.srv.URL, mode)
			waitUntil(t, func() bool { return cl.Transports()[mode] == 2 })
			drops := 0
			echoCheck(t, h, 4<<20, func() { px.srv.CloseClientConnections(); drops++ })
			if drops == 0 {
				t.Fatal("no drops injected")
			}
			if cl.Live() == 0 {
				t.Fatal("tunnels lost")
			}
		})
	}
}

// Requests for an HTTP tunnel are bound to the token and instance that
// opened it; the hub answers DBB_TUNNEL_NOT_FOUND once it is gone.
func TestHTTPTunnelRequestsBound(t *testing.T) {
	h, tokens, url := startHub(t)
	base := hubHTTPURL(url)
	cl, _ := startHTTPClient(t, base, modePoll)
	waitUntil(t, func() bool { return h.Tunnels("plant") >= 1 })
	var id string
	h.mu.Lock()
	for tid := range h.peers["plant"].tunnels {
		id = tid
	}
	h.mu.Unlock()
	do := func(token, instance string) int {
		req, _ := http.NewRequest(http.MethodGet, base+protocol.ReverseTunnelPath+"/"+id+protocol.ReversePollSuffix, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set(protocol.HeaderInstanceID, instance)
		req.Header.Set(protocol.HeaderAckDownOffset, "0")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if st := do("wrong-token-0123456789abcdef", cl.cfg.Node.InstanceID); st != 401 {
		t.Fatalf("wrong token: %d", st)
	}
	if st := do(hubToken, "pi_other"); st != 404 {
		t.Fatalf("wrong instance: %d", st)
	}
	// Rotating the token closes the tunnel; its requests then find nothing.
	tokens.set("plant", "rotated-token-0123456789abcdef")
	h.Revalidate()
	waitUntil(t, func() bool { return do(hubToken, cl.cfg.Node.InstanceID) == 401 })
	_ = net.ErrClosed
}

// A write deadline that fires while a chunk is in flight reports exactly the
// bytes that will reach the peer: a caller retrying the "unwritten" suffix
// (transport.IdleWriteConn) never duplicates bytes, and no chunk aliases the
// caller's buffer.
func TestOwnedStreamExactUnderDeadline(t *testing.T) {
	gc := &gateConn{release: make(chan struct{})}
	s := ownStream(gc)
	p := []byte("abc")
	if n, err := s.Write(p); err != nil || n != 3 {
		t.Fatalf("first write: %d %v", n, err)
	}
	p[0] = 'X' // the caller reuses its buffer
	_ = s.SetWriteDeadline(time.Now().Add(30 * time.Millisecond))
	n, err := s.Write([]byte("def"))
	var ne net.Error
	if n != 0 || !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("stalled write: %d %v", n, err)
	}
	// Retry the suffix exactly as IdleWriteConn does.
	_ = s.SetWriteDeadline(time.Time{})
	close(gc.release)
	if n, err := s.Write([]byte("def")); err != nil || n != 3 {
		t.Fatalf("retry: %d %v", n, err)
	}
	waitUntil(t, func() bool { return gc.got() == "abcdef" })
	_ = s.Close()
	if g := gc.got(); g != "abcdef" {
		t.Fatalf("peer got %q", g)
	}
}

// A failed background write fails the next Write.
func TestOwnedStreamReportsWriteError(t *testing.T) {
	fc := &failConn{fail: true}
	s := ownStream(fc)
	if _, err := s.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool {
		_, err := s.Write([]byte("d"))
		return err != nil
	})
}

type gateConn struct {
	net.Conn
	release chan struct{}
	mu      sync.Mutex
	buf     []byte
}

func (g *gateConn) Write(p []byte) (int, error) {
	<-g.release
	g.mu.Lock()
	g.buf = append(g.buf, p...)
	g.mu.Unlock()
	return len(p), nil
}

func (g *gateConn) got() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return string(g.buf)
}

func (g *gateConn) Close() error { return nil }

type failConn struct {
	net.Conn
	fail bool
}

func (f *failConn) Write(p []byte) (int, error) {
	if f.fail {
		return 0, errors.New("session dead")
	}
	return len(p), nil
}

// Two receives of the same bytes racing while the buffer is full (an
// abandoned POST and its retry) still deliver each byte once, in order.
func TestPipeConcurrentRetransmit(t *testing.T) {
	p := newPipe(4, Addr("a"), Addr("b"))
	data := []byte("0123456789abcdef")
	errc := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, err := p.receive(context.Background(), 0, data); errc <- err }()
	}
	got := make([]byte, 0, len(data))
	buf := make([]byte, 3)
	for len(got) < len(data) {
		n, _ := p.Read(buf)
		got = append(got, buf[:n]...)
	}
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
	if string(got) != string(data) {
		t.Fatalf("got %q", got)
	}
}
