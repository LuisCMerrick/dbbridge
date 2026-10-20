package ingress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/link"
	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
)

// gatewayPrecheckManager serves a healthy probe and answers every WS
// precheck with status.
func gatewayPrecheckManager(t *testing.T, status int) (*IngressManager, config.PeerConfig) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/probe" {
			var q protocol.ProbeRequest
			_ = json.NewDecoder(r.Body).Decode(&q)
			_ = json.NewEncoder(w).Encode(protocol.ProbeResponse{ProtocolVersion: "1.4", NodeID: "B", InstanceID: "bi", Nonce: q.Nonce, RecoveryAvailable: true, AcceptingNewSessions: true, Transports: []string{"ws", "http-sse"}})
			return
		}
		w.Header().Set("Location", "https://sso.example.com/")
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	p := config.PeerConfig{ID: "B", ExpectedNodeID: "B", Token: "tok", Endpoints: []config.EndpointConfig{{ID: "e", URL: srv.URL}}, Failover: config.FailoverConfig{HealthcheckTimeout: time.Second, HealthyThreshold: 2, MaxConcurrentProbes: 2, Cooldown: 5 * time.Second, CooldownRaw: "5s"}}
	cfg := &config.Config{Node: config.NodeConfig{ID: "A", InstanceID: "ai"}, Peers: []config.PeerConfig{p}, Transport: config.TransportConfig{ControlRequestTimeout: time.Second, DialTimeout: time.Second}}
	m := NewIngressManager(cfg, nil)
	t.Cleanup(func() { _ = m.Close() })
	return m, p
}

// An ordinary gateway answer to the WS precheck (redirect, proxy auth, WAF
// limits) makes only WS unavailable: auto falls back to SSE on the same
// endpoint, the address is not isolated, and SSE selections keep working.
func TestPrecheckGatewayAnswerFallsBackToSSE(t *testing.T) {
	for _, status := range []int{301, 302, 307, 308, 407, 410, 413, 414, 429, 431, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			m, p := gatewayPrecheckManager(t, status)
			ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
			defer cancel()
			_, _, mode, err := m.choose(ctx, p, "", "auto", true)
			ep := m.linkManagers["B"].GetEndpoints()[0]
			if err != nil || mode != "http-sse" {
				t.Fatalf("auto: mode=%q err=%v, want http-sse", mode, err)
			}
			if ep.GetState() == link.StateMismatched || !ep.Available(time.Now(), "", true) {
				t.Fatalf("endpoint isolated after a gateway %d: state=%s", status, ep.GetState())
			}
			if ep.TransportAvailable(time.Now(), "ws") {
				t.Fatalf("WS carrier not cooled after a gateway %d", status)
			}
			_, _, mode, err = m.choose(ctx, p, "", "http-sse", true)
			if err != nil || mode != "http-sse" {
				t.Fatalf("http-sse right after: mode=%q err=%v", mode, err)
			}
		})
	}
}

// A raw 401 on the authenticated Upgrade is not tolerated with SSE, but it
// does not mark the address as an identity mismatch either.
func TestPrecheckRaw401NoFallbackNoIsolation(t *testing.T) {
	m, p := gatewayPrecheckManager(t, 401)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_, _, mode, err := m.choose(ctx, p, "", "auto", true)
	if err == nil || mode != "" {
		t.Fatalf("raw 401: mode=%q err=%v, want no selection", mode, err)
	}
	if st := m.linkManagers["B"].GetEndpoints()[0].GetState(); st == link.StateMismatched {
		t.Fatalf("raw 401 marked the endpoint %s", st)
	}
}

func TestClassifyPrecheck(t *testing.T) {
	ctx := context.Background()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name string
		ctx  context.Context
		done bool
		err  error
		want precheckVerdict
	}{
		{"hello mismatch", ctx, false, fmt.Errorf("%w: node ID mismatch", transport.ErrProtocol), precheckUnsafe},
		{"trusted instance mismatch", ctx, false, &transport.RemoteError{Status: 409, Code: protocol.ErrInstanceMismatch, Trusted: true}, precheckUnsafe},
		{"trusted 401", ctx, false, &transport.RemoteError{Status: 401, Code: protocol.ErrUnauthorized, Trusted: true}, precheckAuthRejected},
		{"trusted 403", ctx, false, &transport.RemoteError{Status: 403, Code: protocol.ErrUnauthorized, Trusted: true}, precheckAuthRejected},
		{"raw 401", ctx, false, &transport.RemoteError{Status: 401}, precheckAuthRejected},
		{"raw 403", ctx, false, &transport.RemoteError{Status: 403}, precheckWSUnavailable},
		{"raw 302", ctx, false, &transport.RemoteError{Status: 302}, precheckWSUnavailable},
		{"raw 407", ctx, false, &transport.RemoteError{Status: 407}, precheckWSUnavailable},
		{"raw 500", ctx, false, &transport.RemoteError{Status: 500}, precheckWSUnavailable},
		{"trusted unsupported", ctx, false, fmt.Errorf("%w: %w", transport.ErrWSUpgradeRejected, &transport.RemoteError{Status: 400, Code: protocol.ErrTransportUnsupported, Trusted: true}), precheckWSUnavailable},
		{"trusted 500", ctx, false, &transport.RemoteError{Status: 500, Code: protocol.ErrInternal, Trusted: true}, precheckTemporary},
		{"trusted 429", ctx, false, &transport.RemoteError{Status: 429, Code: protocol.ErrQuotaExceeded, Trusted: true, Retryable: true}, precheckTemporary},
		{"upgrade rejected", ctx, false, fmt.Errorf("%w: status 426", transport.ErrWSUpgradeRejected), precheckWSUnavailable},
		{"reset", ctx, false, errors.New("read: connection reset by peer"), precheckWSUnavailable},
		{"check timeout", ctx, true, context.DeadlineExceeded, precheckTemporary},
		{"caller canceled", canceled, false, context.Canceled, precheckTemporary},
		{"local busy", ctx, false, fmt.Errorf("%w: x", errLocalBusy), precheckTemporary},
	}
	for _, c := range cases {
		if got := classifyPrecheck(c.ctx, c.done, c.err); got != c.want {
			t.Errorf("%s: verdict %d want %d", c.name, got, c.want)
		}
	}
}

type transientRefusal struct{}

func (transientRefusal) Error() string   { return "websocket upgrade rejected: status 429" }
func (transientRefusal) Transient() bool { return true }

func TestPrecheckTransientRefusalNotCached(t *testing.T) {
	if precheckCacheable(transientRefusal{}) {
		t.Fatal("a refusal that declares itself transient must not be cached")
	}
	if !precheckCacheable(fmt.Errorf("%w: status 426", transport.ErrWSUpgradeRejected)) {
		t.Fatal("a definitive upgrade refusal is cached")
	}
}
