package link

import (
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
)

func newTestManager(f config.FailoverConfig) *PeerLinkManager {
	return NewPeerLinkManager(config.PeerConfig{ID: "B", Failover: f, Endpoints: []config.EndpointConfig{{ID: "e1", URL: "http://127.0.0.1:1"}}})
}

// unhealthy_threshold<=0 uses the same effective threshold for the
// cooldown and for the UNHEALTHY state.
func TestThresholdConsistentWhenUnset(t *testing.T) {
	m := newTestManager(config.FailoverConfig{UnhealthyThreshold: 0, Cooldown: time.Minute})
	ep := m.GetEndpoints()[0]
	m.RecordFailure("e1", false)
	if ep.GetState() != StateUnhealthy || ep.ProbeEligible(time.Now()) {
		t.Fatalf("one failure with threshold<=0: state=%s eligible=%v", ep.GetState(), ep.ProbeEligible(time.Now()))
	}
	if ep.Failures() != 1 {
		t.Fatalf("Failures()=%d", ep.Failures())
	}
}

// healthy_threshold consecutive successes end the
// UNHEALTHY state but never cut a running failure cooldown short.
func TestSuccessDoesNotClearCooldown(t *testing.T) {
	m := newTestManager(config.FailoverConfig{UnhealthyThreshold: 1, HealthyThreshold: 2, Cooldown: time.Hour})
	ep := m.GetEndpoints()[0]
	m.RecordFailure("e1", false)
	if ep.Available(time.Now(), "", true) {
		t.Fatal("fixture: endpoint should be cooling down")
	}
	pr := &protocol.ProbeResponse{InstanceID: "i", AcceptingNewSessions: true, Transports: []string{"ws", "http-sse"}}
	m.RecordSuccess("e1", pr)
	m.RecordSuccess("e1", pr)
	if ep.GetState() != StateHealthy {
		t.Fatalf("healthy_threshold successes did not end UNHEALTHY: state=%s", ep.GetState())
	}
	if ep.Available(time.Now(), "", true) || ep.ProbeEligible(time.Now()) {
		t.Fatal("successes cleared an unexpired failure cooldown")
	}
	if next := ep.NextEligible(time.Now(), "ws", "http-sse"); next.IsZero() {
		t.Fatal("NextEligible must report the running cooldown")
	}
	// Once the cooldown has run out the healthy endpoint is selectable again.
	later := time.Now().Add(time.Hour + time.Second)
	if !ep.Available(later, "", true) || !ep.NextEligible(later, "ws").IsZero() {
		t.Fatal("endpoint not available after its cooldown expired")
	}
	// The probe response slice is copied, not aliased.
	pr.Transports[0] = "changed"
	ep.mu.RLock()
	defer ep.mu.RUnlock()
	if ep.SupportedTransports[0] != "ws" {
		t.Fatal("SupportedTransports aliases the probe response")
	}
}

// The first carrier drop cools that (Endpoint, transport) pair
// down for the fault cycle; other carriers and the address stay eligible.
// Attach failures cool down at once as well.
func TestTransportDropCooldown(t *testing.T) {
	m := newTestManager(config.FailoverConfig{Cooldown: time.Minute})
	ep := m.GetEndpoints()[0]
	m.RecordTransportDrop("e1", "http-sse")
	now := time.Now()
	if ep.TransportAvailable(now, "http-sse") {
		t.Fatal("the first SSE carrier drop must start the SSE cooldown")
	}
	if !ep.TransportAvailable(now, "ws") || !ep.ProbeEligible(now) || !ep.Available(now, "", true) {
		t.Fatal("a single-carrier drop must cool only that (endpoint, transport) pair")
	}
	if !ep.NextEligible(now, "ws", "http-sse").IsZero() {
		t.Fatal("WS is still eligible, so the endpoint has an eligible carrier")
	}
	if ep.NextEligible(now, "http-sse").IsZero() {
		t.Fatal("SSE-only selection must see the SSE cooldown")
	}
	m.RecordTransportFailure("e1", "ws")
	if ep.TransportAvailable(time.Now(), "ws") {
		t.Fatal("RecordTransportFailure must cool the carrier down immediately")
	}
	if !ep.TransportAvailable(now.Add(time.Minute+time.Second), "http-sse") {
		t.Fatal("the carrier cooldown must expire")
	}
}

// A whole-path failure of a real request cools the address at
// once, regardless of unhealthy_threshold, and successes cannot shorten it.
func TestPathFailureCoolsAddress(t *testing.T) {
	m := newTestManager(config.FailoverConfig{UnhealthyThreshold: 2, HealthyThreshold: 1, Cooldown: 5 * time.Second})
	ep := m.GetEndpoints()[0]
	m.RecordPathFailure("e1")
	now := time.Now()
	if ep.ProbeEligible(now) || ep.Available(now, "", true) {
		t.Fatal("a whole-path failure must cool the address down")
	}
	if ep.GetState() == StateUnhealthy {
		t.Fatal("UNHEALTHY stays governed by probe thresholds")
	}
	m.RecordSuccess("e1", nil)
	if ep.Available(time.Now(), "", true) {
		t.Fatal("a success cleared the path cooldown")
	}
	if next := ep.NextEligible(now, "ws"); next.Sub(now) < 4*time.Second {
		t.Fatalf("address cooldown too short: %v", next.Sub(now))
	}
}
