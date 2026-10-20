package ingress

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
	"github.com/gorilla/websocket"
)

func TestCleanupMustNotGuessEpoch(t *testing.T) {
	var mu sync.Mutex
	var epochs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ep := r.URL.Query().Get("epoch")
		mu.Lock()
		epochs = append(epochs, ep)
		mu.Unlock()
		w.Header().Set(protocol.HeaderInstanceID, "bi")
		if ep == "1" {
			w.WriteHeader(409)
			_ = json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: protocol.ErrStaleEpoch})
			return
		}
		_ = json.NewEncoder(w).Encode(protocol.DeleteSessionResponse{SessionID: "s", InstanceID: "bi", Epoch: ep, Closed: true})
	}))
	defer srv.Close()
	c := transport.NewTripleTransportClientForURL(srv.URL, "test-credential", &config.TransportConfig{ControlRequestTimeout: time.Second, DialTimeout: time.Second}, "ai", true)
	defer c.Close()
	s := &ingressSession{sid: "s", token: "session-token", owner: "bi"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := s.deleteVia(ctx, c, 1, 1, true)
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(epochs, ",") != "1" || result != cleanupUnknown {
		t.Fatalf("contract violation: unknown resume cleanup guessed epoch; requests=%v result=%v", epochs, result)
	}
}

func TestBindingMismatchMustBeFatal(t *testing.T) {
	err := protocol.ValidateBinding("wrong-session", "bi", "1", "stream", &protocol.TransportMode{Mode: "ws"}, "s", "bi", 1, "stream", "ws")
	if err == nil {
		t.Fatal("fixture: invalid binding accepted")
	}
	if !fatalError(err) {
		t.Fatalf("contract violation: binding validation error does not enter fatal protocol classification: %v", err)
	}
}

func TestAutoMustRejectBadSubprotocol(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/probe" {
			var q protocol.ProbeRequest
			_ = json.NewDecoder(r.Body).Decode(&q)
			_ = json.NewEncoder(w).Encode(protocol.ProbeResponse{ProtocolVersion: "1.4", NodeID: "B", InstanceID: "bi", Nonce: q.Nonce, RecoveryAvailable: true, AcceptingNewSessions: true, Transports: []string{"ws", "http-sse"}})
			return
		}
		c, e := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer c.Close()
		_ = c.WriteJSON(protocol.WSHelloMessage{Type: "hello", ProtocolVersion: "1.4", NodeID: "B", InstanceID: "bi", Nonce: r.Header.Get(protocol.HeaderWSNonce), Transport: "ws"})
	}))
	defer srv.Close()
	p := config.PeerConfig{ID: "B", ExpectedNodeID: "B", Token: "test-credential", Endpoints: []config.EndpointConfig{{ID: "e", URL: srv.URL}}, Failover: config.FailoverConfig{HealthcheckTimeout: time.Second, HealthyThreshold: 1, MaxConcurrentProbes: 2}}
	cfg := &config.Config{Node: config.NodeConfig{ID: "A", InstanceID: "ai"}, Peers: []config.PeerConfig{p}, Transport: config.TransportConfig{ControlRequestTimeout: time.Second, DialTimeout: time.Second}}
	m := NewIngressManager(cfg, nil)
	defer m.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, mode, err := m.choose(ctx, p, "", "auto", true)
	if !errors.Is(err, transport.ErrProtocol) {
		t.Fatalf("contract violation: auto selected mode=%q error=%v after WS subprotocol mismatch", mode, err)
	}
}

// Exercise the actual handleConn failure loop using the project's loopback
// fixture. The local FIN state represents a lost final confirmation; all
// subsequent ACK responses are blackholed. The fault is explicitly injected.
func TestFinalAckMustNotDelayFailureClose(t *testing.T) {
	var block atomic.Bool
	releaseHandlers := make(chan struct{})
	defer close(releaseHandlers)
	e, _ := dataplaneSetup(t, "ws", true, func(c *config.Config) { c.Transport.Recovery.EnabledRaw = false }, nil, func(w http.ResponseWriter, r *http.Request, base http.Handler) bool {
		if block.Load() && strings.HasSuffix(r.URL.Path, "/ack") {
			select {
			case <-r.Context().Done():
			case <-releaseHandlers:
			}
			return true
		}
		return false
	})
	if err := testEcho2(t, e.conn, "test-credential"); err != nil {
		t.Fatal(err)
	}
	s := testSession(t, e.m)
	time.Sleep(50 * time.Millisecond)
	block.Store(true)
	s.mu.Lock()
	g := s.g
	s.upEOF = true
	s.upFINApplied = true
	s.downFIN = true
	s.downFINAcked = false
	s.mu.Unlock()
	start := time.Now()
	s.fault(g, context.DeadlineExceeded, false)
	_ = e.conn.SetReadDeadline(start.Add(4 * time.Second))
	_, err := e.conn.Read(make([]byte, 1))
	d := time.Since(start)
	if err == nil {
		t.Fatal("fixture: client socket did not close")
	}
	if d > time.Second {
		t.Fatalf("contract violation: recovery=false local client closed %v after detected control failure (>1s)", d)
	}
}
