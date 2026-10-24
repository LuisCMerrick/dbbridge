package egress

import (
	"bufio"
	"bytes"
	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"dbbridge/internal/session"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func testEcho(t *testing.T) string {
	t.Helper()
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, e := ln.Accept()
			if e != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	return ln.Addr().String()
}

func testCfg(t *testing.T, peers []config.InboundPeerConfig) *config.Config {
	t.Helper()
	return &config.Config{
		Node:      config.NodeConfig{ID: "B", InstanceID: "bi"},
		HTTP:      config.HTTPConfig{Enabled: true, Listen: "127.0.0.1:0", AllowedTransports: []string{"ws", "http-sse"}},
		Auth:      config.AuthConfig{InboundPeers: peers},
		Targets:   map[string]config.TargetConfig{"db": {Network: "tcp", Address: testEcho(t)}},
		Resources: resource.NewManager(256<<20, 32<<20, 32<<20, 8),
		Transport: config.TransportConfig{MaxFrameSize: 1024, BufferPerDirection: 4096, UpWindowBytes: 2048, UpMaxInflightFrames: 8, DialTimeout: time.Second, UnattachedTimeout: 30 * time.Second, Heartbeat: time.Second, UpRequestTimeout: 30 * time.Second, ControlRequestTimeout: 400 * time.Millisecond},
	}
}

func testCreate(t *testing.T, url, token, id, mode string) (*http.Response, protocol.CreateSessionResponse) {
	t.Helper()
	b, _ := json.Marshal(protocol.CreateSessionRequest{Limits: protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 2048, DownBufferBytes: 4096}, ProtocolVersion: "1.5", IngressInstanceID: "ai", CreateRequestID: id, TargetID: "db", Transport: protocol.TransportMode{Mode: mode}})
	r, _ := http.NewRequest("POST", url+"/v1/sessions", bytes.NewReader(b))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	var c protocol.CreateSessionResponse
	_ = json.NewDecoder(resp.Body).Decode(&c)
	resp.Body.Close()
	return resp, c
}

func testStart(t *testing.T, cfg *config.Config) (*Server, string) {
	t.Helper()
	a := auth.NewAuthenticator(cfg)
	t.Cleanup(a.Close)
	s := NewServer(cfg, a, session.NewRegistry(1<<20, 1<<22))
	if e := s.Start(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s, s.listener.Addr().String()
}

// A declared-but-never-sent body must not pin a connection. Every request
// now carries a read deadline, GET bodies are refused up front, and early
// rejections ask net/http to close instead of draining.
func TestSlowBodyConnectionsAreReleased(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	_, addr := testStart(t, cfg)
	reqs := map[string]string{
		"unauth up":         "POST /v1/sessions/x/up HTTP/1.1\r\nHost: a\r\nContent-Length: 200000\r\nContent-Type: application/octet-stream\r\n\r\n",
		"GET ws with body":  "GET /v1/transport/ws HTTP/1.1\r\nHost: a\r\nContent-Length: 200000\r\n\r\n",
		"GET chunked body":  "GET /v1/sessions/x/events HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n",
		"authed up no body": "POST /v1/sessions/x/up HTTP/1.1\r\nHost: a\r\nAuthorization: Bearer tokA\r\nX-DBB-Instance-ID: bi\r\nContent-Length: 200000\r\n\r\n",
		"create slow body":  "POST /v1/sessions HTTP/1.1\r\nHost: a\r\nAuthorization: Bearer tokA\r\nX-DBB-Instance-ID: bi\r\nContent-Length: 2000\r\n\r\n{",
	}
	type result struct {
		name string
		line string
		err  error
		took time.Duration
	}
	out := make(chan result, len(reqs))
	for name, raw := range reqs {
		go func(name, raw string) {
			c, e := net.Dial("tcp", addr)
			if e != nil {
				out <- result{name: name, err: e}
				return
			}
			defer c.Close()
			start := time.Now()
			_, _ = c.Write([]byte(raw))
			_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
			br := bufio.NewReader(c)
			line, e := br.ReadString('\n')
			if e == nil {
				// The connection must then be closed by the server, not held.
				_, _ = http.ReadResponse(bufio.NewReader(io.MultiReader(strings.NewReader(line), br)), nil)
				_, e2 := br.ReadByte()
				for e2 == nil {
					_, e2 = br.ReadByte()
				}
				if ne, ok := e2.(net.Error); ok && ne.Timeout() {
					out <- result{name: name, line: line, err: fmt.Errorf("connection held open after response"), took: time.Since(start)}
					return
				}
			}
			out <- result{name: name, line: line, took: time.Since(start), err: func() error {
				if ne, ok := e.(net.Error); ok && ne.Timeout() {
					return e
				}
				return nil
			}()}
		}(name, raw)
	}
	for range reqs {
		r := <-out
		if r.err != nil {
			t.Errorf("%s: %v (line %q after %v)", r.name, r.err, r.line, r.took)
			continue
		}
		if r.took > 3*time.Second {
			t.Errorf("%s: released only after %v", r.name, r.took)
		}
		if strings.HasPrefix(r.name, "GET") && !strings.Contains(r.line, " 400 ") {
			t.Errorf("%s: GET with body must be rejected with 400, got %q", r.name, r.line)
		}
	}
	// The listener still serves a legitimate caller.
	c, e := net.Dial("tcp", addr)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	body := `{"protocol_version":"1.5","expected_node_id":"B","nonce":"n1"}`
	fmt.Fprintf(c, "POST /v1/peer/probe HTTP/1.1\r\nHost: a\r\nAuthorization: Bearer tokA\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, e := bufio.NewReader(c).ReadString('\n')
	if e != nil || !strings.Contains(line, " 200 ") {
		t.Fatalf("legitimate probe: %q %v", line, e)
	}
}

// The unauthenticated 401 is immediate and closes, rather than waiting
// for (or draining) a body that never comes.
func TestEarlyRejectClosesWithoutDrain(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	cfg.Transport.ControlRequestTimeout = 10 * time.Second
	_, addr := testStart(t, cfg)
	c, e := net.Dial("tcp", addr)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	start := time.Now()
	fmt.Fprintf(c, "POST /v1/sessions/x/up HTTP/1.1\r\nHost: a\r\nContent-Length: 200000\r\n\r\n")
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, e := http.ReadResponse(bufio.NewReader(c), nil)
	if e != nil {
		t.Fatalf("no response: %v", e)
	}
	resp.Body.Close()
	t.Logf("401 after %v", time.Since(start))
	if resp.StatusCode != 401 || !resp.Close || time.Since(start) > time.Second {
		t.Fatalf("status=%d close=%v after %v", resp.StatusCode, resp.Close, time.Since(start))
	}
	if resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatal("401 without WWW-Authenticate")
	}
}

// Long-lived SSE and WS streams must not inherit the request read deadline.
func TestStreamsOutliveRequestDeadline(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	cfg.Transport.ControlRequestTimeout = 200 * time.Millisecond
	cfg.Transport.Heartbeat = 700 * time.Millisecond
	_, addr := testStart(t, cfg)
	url := "http://" + addr

	t.Run("sse", func(t *testing.T) {
		resp, cr := testCreate(t, url, "tokA", "sse1", "http-sse")
		if resp.StatusCode != 201 {
			t.Fatalf("create %d", resp.StatusCode)
		}
		r, _ := http.NewRequest("GET", url+"/v1/sessions/"+cr.SessionID+"/events?epoch=1", nil)
		r.Header.Set("Authorization", "Bearer tokA")
		r.Header.Set(protocol.HeaderInstanceID, "bi")
		r.Header.Set(protocol.HeaderSessionToken, cr.SessionToken)
		ev, e := http.DefaultClient.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		defer ev.Body.Close()
		if ev.StatusCode != 200 {
			t.Fatalf("events %d", ev.StatusCode)
		}
		br := bufio.NewReader(ev.Body)
		pings := 0
		deadline := time.Now().Add(3 * time.Second)
		for pings < 3 && time.Now().Before(deadline) {
			line, e := br.ReadString('\n')
			if e != nil {
				t.Fatalf("SSE stream ended after %d pings: %v", pings, e)
			}
			if strings.HasPrefix(line, "event: ping") {
				pings++
			}
		}
		if pings < 3 {
			t.Fatalf("only %d pings", pings)
		}
	})
	t.Run("ws", func(t *testing.T) {
		resp, cr := testCreate(t, url, "tokA", "ws1", "ws")
		if resp.StatusCode != 201 {
			t.Fatalf("create %d", resp.StatusCode)
		}
		h := http.Header{}
		h.Set("Authorization", "Bearer tokA")
		h.Set(protocol.HeaderInstanceID, "bi")
		h.Set(protocol.HeaderWSPurpose, "session")
		h.Set(protocol.HeaderWSSessionID, cr.SessionID)
		h.Set(protocol.HeaderSessionToken, cr.SessionToken)
		h.Set(protocol.HeaderWSEpoch, "1")
		ws, _, e := (&websocket.Dialer{Subprotocols: []string{protocol.SecWebSocketProtocol}}).Dial("ws://"+addr+"/v1/transport/ws", h)
		if e != nil {
			t.Fatal(e)
		}
		defer ws.Close()
		pings := 0
		_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
		for pings < 3 {
			_, b, e := ws.ReadMessage()
			if e != nil {
				t.Fatalf("WS stream ended after %d pings: %v", pings, e)
			}
			if bytes.Contains(b, []byte(`"ping"`)) {
				pings++
			}
		}
	})
}

// Unauthenticated (and wrong-method) requests never consume the global
// creation budget, so they cannot crowd out authenticated creates.
func TestUnauthenticatedCreateDoesNotTakeCreates(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	a := auth.NewAuthenticator(cfg)
	defer a.Close()
	reg := session.NewRegistry(1<<20, 1<<22)
	defer reg.CloseAll()
	ts := httptest.NewServer(NewServer(cfg, a, reg).Handler())
	defer ts.Close()
	limit := cfg.Resources.Creates.Limit()
	if !cfg.Resources.Creates.Try(limit) {
		t.Fatal("setup")
	}
	defer cfg.Resources.Creates.Put(limit)
	r, _ := testCreate(t, ts.URL, "wrong", "u1", "ws")
	if r.StatusCode != 401 {
		t.Fatalf("unauthenticated create with Creates exhausted: got %d, want 401", r.StatusCode)
	}
	g, _ := http.Get(ts.URL + "/v1/sessions")
	g.Body.Close()
	if g.StatusCode != 405 || g.Header.Get("Allow") != "POST" {
		t.Fatalf("GET /v1/sessions: %d Allow=%q", g.StatusCode, g.Header.Get("Allow"))
	}
	if cfg.Resources.Creates.Used() != limit {
		t.Fatal("creation budget changed by rejected requests")
	}
}

// With several peers configured, one peer cannot hold the whole create
// (or probe/resume) budget; another peer still gets through.
func TestPeerCannotTakeWholeCreateBudget(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{
		{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5},
		{ID: "C", Tokens: []string{"tokC"}, AllowedTargets: []string{"db"}, MaxSessions: 5},
	})
	a := auth.NewAuthenticator(cfg)
	defer a.Close()
	reg := session.NewRegistry(1<<20, 1<<22)
	defer reg.CloseAll()
	ts := httptest.NewServer(NewServer(cfg, a, reg).Handler())
	defer ts.Close()
	global := cfg.Resources.Creates.Limit()
	share := auth.PeerShare(global, 2)
	if share >= global {
		t.Fatalf("share %d must leave room in %d", share, global)
	}
	// Peer A holds its whole share of in-flight creates.
	for i := int64(0); i < share; i++ {
		release, ok := a.ReserveFair("A", "create", global)
		if !ok {
			t.Fatalf("reservation %d refused", i)
		}
		defer release()
		if !cfg.Resources.Creates.Try(1) {
			t.Fatal("global budget")
		}
		defer cfg.Resources.Creates.Put(1)
	}
	if r, _ := testCreate(t, ts.URL, "tokA", "a-over", "ws"); r.StatusCode != 429 {
		t.Fatalf("peer A beyond its share: %d, want 429", r.StatusCode)
	}
	if r, _ := testCreate(t, ts.URL, "tokC", "c1", "ws"); r.StatusCode != 201 {
		t.Fatalf("peer C starved: %d", r.StatusCode)
	}
}

// Removing the
// Peer Token that created a live session does not close it while the identity
// stays enabled and the Target ACL still allows it. Every later request that
// presents the removed token (control, WS attach, POST, resume) is refused
// with 401, and the same session keeps working with the current token.
// Identity disable/removal and ACL tightening are covered in
// authz_rotation_test.go.
func TestTokenRotationKeepsLiveSession(t *testing.T) {
	t.Setenv("DBB_TEST_TOKEN_OLD", "test-old-token-0001")
	t.Setenv("DBB_TEST_TOKEN_NEW", "test-new-token-0002")
	p := filepath.Join(t.TempDir(), "auth.yaml")
	write := func(envs string) {
		t.Helper()
		if err := os.WriteFile(p, []byte("version: 1\ninbound_peers:\n- id: A\n  token_envs: ["+envs+"]\n  allowed_targets: [db]\n  max_sessions: 5\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("DBB_TEST_TOKEN_OLD, DBB_TEST_TOKEN_NEW")
	cfg := testCfg(t, nil)
	cfg.Auth = config.AuthConfig{CredentialsFile: p, ReloadInterval: 50 * time.Millisecond}
	a := auth.NewAuthenticator(cfg)
	defer a.Close()
	if err := a.Err(); err != nil {
		t.Fatal(err)
	}
	reg := session.NewRegistry(1<<20, 1<<22)
	defer reg.CloseAll()
	ts := httptest.NewServer(NewServer(cfg, a, reg).Handler())
	defer ts.Close()
	r1, old := testCreate(t, ts.URL, "test-old-token-0001", "o1", "ws")
	r2, kept := testCreate(t, ts.URL, "test-new-token-0002", "n1", "ws")
	if r1.StatusCode != 201 || r2.StatusCode != 201 {
		t.Fatalf("create %d %d", r1.StatusCode, r2.StatusCode)
	}
	rev := a.Revision()
	write("DBB_TEST_TOKEN_NEW")
	waitReload(t, a, rev)
	time.Sleep(100 * time.Millisecond) // let the reload sweep run
	for _, id := range []string{old.SessionID, kept.SessionID} {
		if s := reg.Get(id); s == nil || s.GetState() == session.StateClosed {
			t.Fatalf("session %s closed by a pure token rotation", id)
		}
	}
	for _, rq := range rotationRequests(ts.URL, old) {
		if code := doAuth(t, rq, "test-old-token-0001"); code != http.StatusUnauthorized {
			t.Fatalf("%s %s with removed token: %d, want 401", rq.method, rq.path, code)
		}
	}
	if r, _ := testCreate(t, ts.URL, "test-old-token-0001", "o2", "ws"); r.StatusCode != 401 {
		t.Fatalf("create with removed token: %d, want 401", r.StatusCode)
	}
	ack := rotationRequests(ts.URL, old)[0]
	if code := doAuth(t, ack, "test-new-token-0002"); code == 401 || code == 403 {
		t.Fatalf("control with current token on the old session: %d", code)
	}
	if s := reg.Get(old.SessionID); s == nil || s.GetState() == session.StateClosed {
		t.Fatal("session closed after control with current token")
	}
}

// The instance ID is not handed to anonymous callers that do not already
// present it; the ingress client (which does) still gets the echo it uses to
// trust error bodies.
func TestInstanceIDNotLeakedPreAuth(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	a := auth.NewAuthenticator(cfg)
	defer a.Close()
	ts := httptest.NewServer(NewServer(cfg, a, session.NewRegistry(1<<20, 1<<22)).Handler())
	defer ts.Close()
	r, _ := http.Post(ts.URL+"/v1/peer/probe", "application/json", strings.NewReader("{}"))
	r.Body.Close()
	if r.StatusCode != 401 || r.Header.Get(protocol.HeaderInstanceID) != "" {
		t.Fatalf("anonymous: %d instance=%q", r.StatusCode, r.Header.Get(protocol.HeaderInstanceID))
	}
	req, _ := http.NewRequest("POST", ts.URL+"/v1/sessions", strings.NewReader("{}"))
	req.Header.Set(protocol.HeaderInstanceID, "bi")
	r, _ = http.DefaultClient.Do(req)
	r.Body.Close()
	if r.StatusCode != 401 || r.Header.Get(protocol.HeaderInstanceID) != "bi" {
		t.Fatalf("bound client: %d instance=%q", r.StatusCode, r.Header.Get(protocol.HeaderInstanceID))
	}
}

// Close waits for handlers and refuses a restart.
func TestCloseWaitsAndStartAfterCloseFails(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	s, addr := testStart(t, cfg)
	resp, cr := testCreate(t, "http://"+addr, "tokA", "x1", "ws")
	if resp.StatusCode != 201 {
		t.Fatalf("create %d", resp.StatusCode)
	}
	h := http.Header{}
	h.Set("Authorization", "Bearer tokA")
	h.Set(protocol.HeaderInstanceID, "bi")
	h.Set(protocol.HeaderWSPurpose, "session")
	h.Set(protocol.HeaderWSSessionID, cr.SessionID)
	h.Set(protocol.HeaderSessionToken, cr.SessionToken)
	h.Set(protocol.HeaderWSEpoch, "1")
	ws, _, e := (&websocket.Dialer{Subprotocols: []string{protocol.SecWebSocketProtocol}}).Dial("ws://"+addr+"/v1/transport/ws", h)
	if e != nil {
		t.Fatal(e)
	}
	defer ws.Close()
	_, _, _ = ws.ReadMessage()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("tracked goroutines still running after Close returned")
	}
	if err := s.Start(); err == nil {
		t.Fatal("Start after Close must fail")
	}
}
