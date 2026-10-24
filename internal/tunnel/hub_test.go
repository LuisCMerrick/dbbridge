package tunnel

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"

	"github.com/gorilla/websocket"
)

const hubToken = "hub-test-token-0123456789abcdef"

type tokenTable struct {
	mu sync.Mutex
	m  map[string]string
}

func (t *tokenTable) get(id string) string { t.mu.Lock(); defer t.mu.Unlock(); return t.m[id] }
func (t *tokenTable) set(id, v string)     { t.mu.Lock(); t.m[id] = v; t.mu.Unlock() }

func hubConfig(t *testing.T) *config.Config {
	t.Helper()
	t.Setenv("DBB_HUB_TEST_TOKEN", hubToken)
	cfg := &config.Config{
		Node: config.NodeConfig{ID: "hq-b"},
		HTTP: config.HTTPConfig{Enabled: true, Listen: "127.0.0.1:0"},
		Peers: []config.PeerConfig{
			{ID: "plant", Mode: config.PeerModeReverse, ExpectedNodeID: "plant-a", CredentialRef: "plant", TokenEnv: "DBB_HUB_TEST_TOKEN"},
		},
		Transport: config.TransportConfig{HeartbeatRaw: "1s", FailureDetectTimeoutRaw: "3s"},
	}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func startHub(t *testing.T) (*Hub, *tokenTable, string) {
	t.Helper()
	cfg := hubConfig(t)
	tokens := &tokenTable{m: map[string]string{"plant": hubToken}}
	h := NewHub(cfg, tokens.get)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.ReverseTunnelPath && !strings.HasPrefix(r.URL.Path, protocol.ReverseTunnelPath+"/") {
			http.NotFound(w, r)
			return
		}
		h.Serve(w, r, nil)
	}))
	t.Cleanup(func() { h.Close(); srv.Close() })
	return h, tokens, "ws" + strings.TrimPrefix(srv.URL, "http") + protocol.ReverseTunnelPath
}

type handshake struct {
	token, node, expected, id, instance, priority string
	subprotocols                                  []string
}

func goodHandshake() handshake {
	return handshake{token: hubToken, node: "plant-a", expected: "hq-b", id: newTunnelID(), instance: "pi_db1", priority: "1", subprotocols: []string{protocol.ReverseSubprotocol}}
}

func dialHub(url string, h handshake) (*websocket.Conn, *http.Response, error) {
	hdr := http.Header{}
	if h.token != "" {
		hdr.Set("Authorization", "Bearer "+h.token)
	}
	hdr.Set(protocol.HeaderNodeID, h.node)
	hdr.Set(protocol.HeaderExpectedNodeID, h.expected)
	hdr.Set(protocol.HeaderTunnelID, h.id)
	hdr.Set(protocol.HeaderInstanceID, h.instance)
	hdr.Set(protocol.HeaderTunnelPriority, h.priority)
	d := websocket.Dialer{Subprotocols: h.subprotocols, HandshakeTimeout: 3 * time.Second}
	return d.Dial(url, hdr)
}

func wantRefused(t *testing.T, resp *http.Response, err error, status int, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("handshake accepted, want %d %s", status, code)
	}
	if resp == nil {
		t.Fatalf("no response: %v", err)
	}
	defer resp.Body.Close()
	var e protocol.ErrorResponse
	_ = protocol.DecodeJSON(resp.Body, 32<<10, &e)
	if resp.StatusCode != status || e.Code != code {
		t.Fatalf("got %d %s, want %d %s", resp.StatusCode, e.Code, status, code)
	}
}

func TestHubRejectsBadHandshakes(t *testing.T) {
	h, _, url := startHub(t)
	for _, tc := range []struct {
		name   string
		mutate func(*handshake)
		status int
		code   string
	}{
		{"no token", func(q *handshake) { q.token = "" }, 401, protocol.ErrUnauthorized},
		{"wrong token", func(q *handshake) { q.token = "wrong-token-0123456789abcdef" }, 401, protocol.ErrUnauthorized},
		{"impersonated node", func(q *handshake) { q.node = "plant-x" }, 403, protocol.ErrNodeMismatch},
		{"wrong public node", func(q *handshake) { q.expected = "hq-z" }, 403, protocol.ErrNodeMismatch},
		{"old protocol", func(q *handshake) { q.subprotocols = []string{"dbbridge.reverse.v1.4"} }, 409, protocol.ErrProtocolMismatch},
		{"no subprotocol", func(q *handshake) { q.subprotocols = nil }, 409, protocol.ErrTransportUnsupported},
		{"bad tunnel id", func(q *handshake) { q.id = "x" }, 400, protocol.ErrBadFrame},
		{"bad priority", func(q *handshake) { q.priority = "-1" }, 400, protocol.ErrBadFrame},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := goodHandshake()
			tc.mutate(&q)
			_, resp, err := dialHub(url, q)
			wantRefused(t, resp, err, tc.status, tc.code)
		})
	}
	if h.Tunnels("plant") != 0 || h.Rejected() == 0 {
		t.Fatalf("tunnels=%d rejected=%d", h.Tunnels("plant"), h.Rejected())
	}
}

func TestHubRefusesDuplicateAndForeignInstance(t *testing.T) {
	h, _, url := startHub(t)
	q := goodHandshake()
	c, _, err := dialHub(url, q)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	waitUntil(t, func() bool { return h.Tunnels("plant") == 1 })

	_, resp, err := dialHub(url, q) // same tunnel ID again
	wantRefused(t, resp, err, 409, protocol.ErrTunnelConflict)

	other := goodHandshake()
	other.instance = "pi_impostor"
	_, resp, err = dialHub(url, other)
	wantRefused(t, resp, err, 409, protocol.ErrTunnelConflict)

	// A second tunnel of the same instance is fine.
	second := goodHandshake()
	c2, _, err := dialHub(url, second)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	waitUntil(t, func() bool { return h.Tunnels("plant") == 2 })

	// Once the first instance's tunnels are gone, a new instance (a
	// restarted database-side node) may register.
	c.Close()
	c2.Close()
	waitUntil(t, func() bool { return h.Tunnels("plant") == 0 })
	c3, _, err := dialHub(url, other)
	if err != nil {
		t.Fatalf("new instance after old tunnels closed: %v", err)
	}
	c3.Close()
}

// Rotating or revoking the peer's token closes tunnels set up with the old
// one, and the old token no longer opens new tunnels.
func TestHubRevalidateClosesStaleTunnels(t *testing.T) {
	h, tokens, url := startHub(t)
	c, _, err := dialHub(url, goodHandshake())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	waitUntil(t, func() bool { return h.Tunnels("plant") == 1 })
	h.Revalidate()
	if h.Tunnels("plant") != 1 {
		t.Fatal("unchanged token closed the tunnel")
	}
	tokens.set("plant", "rotated-token-0123456789abcdef")
	h.Revalidate()
	waitUntil(t, func() bool { return h.Tunnels("plant") == 0 })
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		if _, _, err := c.ReadMessage(); err != nil {
			break
		}
	}
	_, resp, err := dialHub(url, goodHandshake())
	wantRefused(t, resp, err, 401, protocol.ErrUnauthorized)
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Streams opened by the hub reach the database side's listener; without a
// tunnel the opener fails with ErrTunnelUnavailable.
func TestHubClientStreams(t *testing.T) {
	h, _, url := startHub(t)
	if _, err := h.Opener("plant")(context.Background()); !errors.Is(err, transport.ErrTunnelUnavailable) {
		t.Fatalf("err = %v", err)
	}
	cfg := clientConfig(t, strings.TrimSuffix(strings.Replace(url, "ws", "http", 1), protocol.ReverseTunnelPath), "hq-b")
	ln := NewListener("db")
	cl := NewClient(cfg, ln)
	if err := cl.Start(); err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	waitUntil(t, func() bool { return h.Tunnels("plant") == 2 && cl.Live() == 2 })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	s, err := h.Opener("plant")(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, ok := s.RemoteAddr().(Addr); !ok {
		t.Fatalf("stream remote address %T", s.RemoteAddr())
	}
	if _, err := s.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(s, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("echo %q %v", buf, err)
	}
}

func clientConfig(t *testing.T, url, peerNode string) *config.Config {
	t.Helper()
	t.Setenv("DBB_HUB_TEST_TOKEN", hubToken)
	cfg := &config.Config{
		Node:      config.NodeConfig{ID: "plant-a"},
		Targets:   map[string]config.TargetConfig{"db": {Address: "127.0.0.1:3306"}},
		Reverse:   config.ReverseConfig{Enabled: true, PeerNodeID: peerNode, TokenEnv: "DBB_HUB_TEST_TOKEN", Endpoints: []config.EndpointConfig{{ID: "p", URL: url, Priority: 1}}, Reconnect: config.ReverseReconnectConfig{MinBackoffRaw: "20ms", MaxBackoffRaw: "100ms"}},
		Transport: config.TransportConfig{HeartbeatRaw: "1s", FailureDetectTimeoutRaw: "3s"},
	}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// The database side refuses a public side that answers with another node ID
// and never delivers its streams.
func TestClientRejectsWrongPublicNode(t *testing.T) {
	_, _, url := startHub(t)
	cfg := clientConfig(t, strings.TrimSuffix(strings.Replace(url, "ws", "http", 1), protocol.ReverseTunnelPath), "hq-other")
	cl := NewClient(cfg, NewListener("db"))
	// The hub refuses the expected-node header; check the client-side check
	// directly as well against a server that lies about its identity.
	if _, err := cl.dial(cfg.Reverse.Endpoints[0]); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("err = %v", err)
	}
	liar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{Subprotocols: []string{protocol.ReverseSubprotocol}}
		c, err := up.Upgrade(w, r, http.Header{protocol.HeaderNodeID: []string{"hq-b"}, protocol.HeaderInstanceID: []string{"pi_x"}})
		if err == nil {
			c.Close()
		}
	}))
	defer liar.Close()
	cfg2 := clientConfig(t, liar.URL, "hq-other")
	cl2 := NewClient(cfg2, NewListener("db"))
	if _, err := cl2.dial(cfg2.Reverse.Endpoints[0]); err == nil || !strings.Contains(err.Error(), "node mismatch") {
		t.Fatalf("err = %v", err)
	}
}

func TestBackoff(t *testing.T) {
	r := config.ReverseReconnectConfig{MinBackoff: 100 * time.Millisecond, MaxBackoff: time.Second}
	for n, want := range map[int]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 3: 400 * time.Millisecond, 5: time.Second, 20: time.Second} {
		for i := 0; i < 20; i++ {
			d := backoff(r, n)
			if d < want/2 || d > want {
				t.Fatalf("backoff(%d) = %v, want in [%v, %v]", n, d, want/2, want)
			}
		}
	}
}

// Slots of a lower-priority endpoint stay idle while a better endpoint has
// a live tunnel.
func TestClientPriority(t *testing.T) {
	h, _, url := startHub(t)
	good := strings.TrimSuffix(strings.Replace(url, "ws", "http", 1), protocol.ReverseTunnelPath)
	cfg := clientConfig(t, good, "hq-b")
	cfg.Reverse.Endpoints = append(cfg.Reverse.Endpoints, config.EndpointConfig{ID: "backup", URL: "http://127.0.0.1:1", Priority: 2})
	cfg.Reverse.TunnelsPerEndpoint = 1
	var backupDials sync.Map
	cl := NewClient(cfg, NewListener("db"))
	cl.dialHook = func(ctx context.Context, ep config.EndpointConfig, id string) (*websocket.Conn, error) {
		if ep.ID == "backup" {
			backupDials.Store(id, true)
			return nil, errors.New("backup unreachable")
		}
		return cl.dialWS(ep, id)
	}
	if err := cl.Start(); err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	waitUntil(t, func() bool { return h.Tunnels("plant") == 1 })
	time.Sleep(200 * time.Millisecond)
	n := 0
	backupDials.Range(func(any, any) bool { n++; return true })
	// The backup slot may have raced the first dial once, but not more.
	if n > 1 {
		t.Fatalf("backup endpoint dialed %d times while primary was up", n)
	}
}
