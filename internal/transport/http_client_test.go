package transport

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"

	"github.com/gorilla/websocket"
)

func testCfg() *config.TransportConfig {
	return &config.TransportConfig{ControlRequestTimeout: 2 * time.Second, UpRequestTimeout: 7 * time.Second, DialTimeout: time.Second}
}

// Close of a wss connection whose peer stopped reading must not wait
// for TLS close_notify (5s) or the write-idle timeout.
func TestWSCloseBlackholeTLS(t *testing.T) {
	stop := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		<-stop // never read
		c.Close()
	}))
	defer srv.Close()
	defer close(stop)
	d := websocket.Dialer{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	d.NetDialContext = func(ctx context.Context, n, a string) (net.Conn, error) {
		c, e := (&net.Dialer{}).DialContext(ctx, n, a)
		if e != nil {
			return nil, e
		}
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetWriteBuffer(16 << 10)
		}
		return NewIdleWriteConn(c), nil
	}
	conn, _, err := d.Dial("wss"+strings.TrimPrefix(srv.URL, "https"), nil)
	if err != nil {
		t.Fatal(err)
	}
	w := &WSSessionConn{conn: conn, writeIdle: 2 * time.Second}
	payload := make([]byte, 64<<10)
	var werr error
	for i := uint64(0); i < 100000 && werr == nil; i++ {
		werr = w.WriteBinaryFrame(1, i*uint64(len(payload)), payload)
	}
	if werr == nil {
		t.Fatal("peer never stalled")
	}
	start := time.Now()
	_ = w.Close()
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Close took %v (> 1s)", d)
	}
}

// The control pool is not capped at 32 concurrent requests.
func TestControlPoolNotCappedAt32(t *testing.T) {
	var cur, peak atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := cur.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(150 * time.Millisecond)
		cur.Add(-1)
		w.WriteHeader(500)
	}))
	defer srv.Close()
	c := NewTripleTransportClientForURL(srv.URL, "tok", testCfg(), "ing", true)
	defer c.Close()
	var wg sync.WaitGroup
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = c.Probe(context.Background(), "B", "n") }()
	}
	wg.Wait()
	if peak.Load() <= 32 {
		t.Fatalf("peak concurrency %d, still capped at 32", peak.Load())
	}
}

// A large error body is drained so the HTTP/1.1 connection is reused.
func TestErrorBodyDrainedForReuse(t *testing.T) {
	var conns atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "40960")
		w.WriteHeader(502)
		_, _ = w.Write([]byte(strings.Repeat("x", 40960)))
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()
	c := NewTripleTransportClientForURL(srv.URL, "tok", testCfg(), "ing", true)
	defer c.Close()
	for i := 0; i < 3; i++ {
		var re *RemoteError
		if _, err := c.Probe(context.Background(), "B", "n"); !errors.As(err, &re) || re.Status != 502 {
			t.Fatalf("want 502, got %v", err)
		}
	}
	if n := conns.Load(); n != 1 {
		t.Fatalf("opened %d connections, want 1 (reuse)", n)
	}
}

// Retry-After is clamped to 2s rather than ignored; HTTP-date works.
func TestRetryAfterClamp(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := map[string]time.Duration{"": 0, "0": 0, "-1": 0, "1": time.Second, "2": 2 * time.Second, "5": 2 * time.Second, "999999999999": 2 * time.Second, "x": 0,
		now.Add(time.Second).Format(http.TimeFormat): time.Second, now.Add(time.Hour).Format(http.TimeFormat): 2 * time.Second, now.Add(-time.Hour).Format(http.TimeFormat): 0}
	for v, want := range cases {
		if got := parseRetryAfter(v, now); got != want {
			t.Errorf("Retry-After %q: got %v want %v", v, got, want)
		}
	}
}

// Peer-supplied session IDs are escaped in URLs and validated on create.
func TestSessionIDEscaped(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.RawPath
		if got == "" {
			got = r.URL.Path
		}
		got += "?" + r.URL.RawQuery
		w.WriteHeader(500)
	}))
	defer srv.Close()
	c := NewTripleTransportClientForURL(srv.URL, "tok", testCfg(), "ing", true)
	defer c.Close()
	_, _ = c.ResumeSession(context.Background(), "../peer/probe?x=", "st", "ing", protocol.ResumeRequest{})
	if got != "/v1/sessions/..%2Fpeer%2Fprobe%3Fx=/resume?" {
		t.Fatalf("server saw %q", got)
	}
	_ = c.DeleteSession(context.Background(), "a/b", "st", "ing", 3)
	if got != "/v1/sessions/a%2Fb?epoch=3" {
		t.Fatalf("server saw %q", got)
	}
	for _, sid := range []string{"", "..", "a/b", "a?b", "a b", strings.Repeat("a", 129)} {
		if validSessionID(sid) {
			t.Errorf("accepted %q", sid)
		}
	}
	if !validSessionID("0123abcdef-_.~XYZ") {
		t.Error("rejected a valid ID")
	}
}

// No empty bearer on SSE subscribe; marshal failures fail the request.
func TestNoEmptyBearerAndBodyError(t *testing.T) {
	var auth []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Values("Authorization")
		w.WriteHeader(500)
	}))
	defer srv.Close()
	c := NewTripleTransportClientForURL(srv.URL, "", testCfg(), "ing", true)
	defer c.Close()
	_, _ = c.SubscribeEvents(context.Background(), "sid", "st", "ing", 1, 1024)
	if len(auth) != 0 {
		t.Fatalf("sent Authorization %q with empty token", auth)
	}
	b, err := io.ReadAll(jsonBody(math.Inf(1)))
	if err == nil || len(b) != 0 {
		t.Fatalf("jsonBody hid marshal error: %q %v", b, err)
	}
	if _, err := io.ReadAll(jsonBody(map[string]int{"a": 1})); err != nil {
		t.Fatal(err)
	}
}

// An expected epoch of MaxUint64 cannot be followed by a wrapped "0".
func TestResumeEpochWrap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(protocol.ResumeResponse{SessionID: "s", InstanceID: "ing", Epoch: "0", ResumeRequestID: "r", Transport: protocol.TransportMode{Mode: "ws"}, DownReplayFrom: "0", AckUpOffset: "0", UpWrittenOffset: "0", DownProducedOffset: "0", DownRetainedFrom: "0", ResumeRemainingMs: 1000})
	}))
	defer srv.Close()
	c := NewTripleTransportClientForURL(srv.URL, "tok", testCfg(), "ing", true)
	defer c.Close()
	q := protocol.ResumeRequest{ExpectedEpoch: "18446744073709551615", ResumeRequestID: "r", Transport: protocol.TransportMode{Mode: "ws"}, DownDeliveredOffset: "0", UpNextOffset: "0", KnownUpAckOffset: "0"}
	if _, err := c.ResumeSession(context.Background(), "s", "st", "ing", q); !errors.Is(err, ErrProtocol) {
		t.Fatalf("wrapped epoch accepted: %v", err)
	}
}

// The data pool waits up_request_timeout for headers and h2
// connections are health-checked with PINGs.
func TestPoolTimeoutsAndH2Ping(t *testing.T) {
	cfg := testCfg()
	cfg.Heartbeat = 3 * time.Second
	c := NewTripleTransportClientForURL("http://127.0.0.1:1", "tok", cfg, "ing", true)
	defer c.Close()
	data := c.dataClient.Transport.(*http.Transport)
	ctrl := c.controlClient.Transport.(*http.Transport)
	if data.ResponseHeaderTimeout != cfg.UpRequestTimeout || ctrl.ResponseHeaderTimeout != cfg.ControlRequestTimeout {
		t.Fatalf("header timeouts data=%v control=%v", data.ResponseHeaderTimeout, ctrl.ResponseHeaderTimeout)
	}
	for _, tr := range []*http.Transport{data, ctrl, c.sseClient.Transport.(*http.Transport)} {
		if tr.MaxConnsPerHost != 0 || tr.HTTP2 == nil || tr.HTTP2.SendPingTimeout != cfg.Heartbeat {
			t.Fatalf("pool config: max=%d h2=%+v", tr.MaxConnsPerHost, tr.HTTP2)
		}
	}
}
