package ingress

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
	"github.com/gorilla/websocket"
)

// precheckSelect runs auto selection against one endpoint whose probe offers
// ws+http-sse and whose /v1/transport/ws answers with ws.
func precheckSelect(t *testing.T, ws http.HandlerFunc) (string, int32, error) {
	t.Helper()
	var probes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/probe" {
			probes.Add(1)
			var q protocol.ProbeRequest
			_ = json.NewDecoder(r.Body).Decode(&q)
			_ = json.NewEncoder(w).Encode(protocol.ProbeResponse{ProtocolVersion: "1.4", NodeID: "B", InstanceID: "bi", Nonce: q.Nonce, RecoveryAvailable: true, AcceptingNewSessions: true, Transports: []string{"ws", "http-sse"}})
			return
		}
		ws(w, r)
	}))
	t.Cleanup(srv.Close)
	p := config.PeerConfig{ID: "B", ExpectedNodeID: "B", Token: "tok", Endpoints: []config.EndpointConfig{{ID: "e", URL: srv.URL}}, Failover: config.FailoverConfig{HealthcheckTimeout: time.Second, HealthyThreshold: 1, MaxConcurrentProbes: 2}}
	cfg := &config.Config{Node: config.NodeConfig{ID: "A", InstanceID: "ai"}, Peers: []config.PeerConfig{p}, Transport: config.TransportConfig{ControlRequestTimeout: time.Second, DialTimeout: time.Second}}
	m := NewIngressManager(cfg, nil)
	t.Cleanup(func() { _ = m.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, mode, err := m.choose(ctx, p, "", "auto", true)
	return mode, probes.Load(), err
}

// A raw gateway refusal of the Upgrade (426 here) still falls back to
// SSE in auto, after a second authenticated probe of the same instance.
func TestPrecheckGatewayRefusalStillFallsBack(t *testing.T) {
	for _, status := range []int{400, 404, 405, 426, 501} {
		mode, probes, err := precheckSelect(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) })
		if err != nil || mode != "http-sse" || probes < 2 {
			t.Fatalf("raw %d: mode=%q err=%v probes=%d, want http-sse after re-probe", status, mode, err, probes)
		}
	}
}

// A wrong hello version or illegal hello JSON after
// the 101 excludes the candidate; auto must not use SSE on it.
func TestPrecheckBadHelloRejectsCandidate(t *testing.T) {
	for name, msg := range map[string]string{"version": `{"type":"hello","protocol_version":"1.3","node_id":"B","instance_id":"bi","nonce":"%s","transport":"ws"}`, "duplicate key": `{"type":"hello","type":"hello"}`} {
		mode, _, err := precheckSelect(t, func(w http.ResponseWriter, r *http.Request) {
			c, e := (&websocket.Upgrader{Subprotocols: []string{protocol.SecWebSocketProtocol}}).Upgrade(w, r, nil)
			if e != nil {
				return
			}
			defer c.Close()
			_ = c.WriteMessage(websocket.TextMessage, []byte(msg))
		})
		if !errors.Is(err, transport.ErrProtocol) {
			t.Fatalf("%s: mode=%q err=%v, want ErrProtocol", name, mode, err)
		}
	}
}
