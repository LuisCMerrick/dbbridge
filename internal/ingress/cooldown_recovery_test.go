package ingress

import (
	"net"
	"testing"
	"time"

	"dbbridge/internal/config"
)

// Recovery records the carrier drop before its first candidate
// selection. With auto, a dropped WS carrier is cooled down for the fault
// cycle, so the same address's SSE is chosen instead of reusing the WS path
// that just failed.
func TestCooldownRecoveryRecordsDropBeforeSelection(t *testing.T) {
	e, _ := dataplaneSetup(t, "auto", true, nil, nil, nil)
	e.conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := testEcho2(t, e.conn, "before"); err != nil {
		t.Fatal(err)
	}
	s := testSession(t, e.m)
	s.mu.Lock()
	g, mode := s.g, s.mode
	s.mu.Unlock()
	if mode != "ws" || g == nil || g.ws == nil {
		t.Fatalf("fixture: auto should start on ws, got %q", mode)
	}
	start := time.Now()
	g.ws.Close()
	if err := testEcho2(t, e.conn, "after"); err != nil {
		t.Fatalf("session did not recover: %v", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("recovery onto the same address's SSE took %v", d)
	}
	s.mu.Lock()
	mode = s.mode
	s.mu.Unlock()
	if mode != "http-sse" {
		t.Fatalf("recovery reused the WS carrier that just dropped: mode=%q", mode)
	}
	ep := e.m.linkManagers[s.p.ID].GetEndpoints()[0]
	if ep.TransportAvailable(time.Now(), "ws") {
		t.Fatal("the dropped WS carrier is not cooling down")
	}
	if !ep.ProbeEligible(time.Now()) {
		t.Fatal("a single-carrier drop must not cool the whole address")
	}
}

// Forced ws, failover=false, one endpoint whose WS carrier is
// cooling down: the new session waits for the cooldown but never beyond
// create_retry_timeout, then fails.
func TestCooldownCreateEndsAtCreateRetryTimeout(t *testing.T) {
	e, _ := dataplaneSetup(t, "ws", false, func(c *config.Config) {
		c.Transport.CreateRetryTimeoutRaw = "700ms"
		off := false
		c.Peers[0].Failover.Enabled = &off
	}, nil, nil)
	if err := e.m.Start(); err != nil {
		t.Fatal(err)
	}
	mgr := e.m.linkManagers["B"]
	mgr.RecordTransportFailure(mgr.GetEndpoints()[0].ID, "ws")
	c, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	start := time.Now()
	c.Write([]byte("x"))
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("a session was created over the cooled-down WS carrier")
	}
	if d := time.Since(start); d < 500*time.Millisecond || d > 2500*time.Millisecond {
		t.Fatalf("create ended after %v, want about create_retry_timeout (700ms)", d)
	}
}

// Forced ws, one endpoint, the WS carrier drops and its 5s
// cooldown outlasts recovery.grace: recovery waits but ends at the recovery
// deadline instead of reusing the carrier or hanging.
func TestCooldownRecoveryEndsAtGrace(t *testing.T) {
	e, _ := dataplaneSetup(t, "ws", true, func(c *config.Config) {
		c.Transport.Recovery = config.RecoveryConfig{EnabledRaw: true, GraceRaw: "1s", BarrierTimeoutRaw: "500ms"}
		off := false
		c.Peers[0].Failover.Enabled = &off
	}, nil, nil)
	e.conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := testEcho2(t, e.conn, "x"); err != nil {
		t.Fatal(err)
	}
	s := testSession(t, e.m)
	s.mu.Lock()
	g := s.g
	s.mu.Unlock()
	start := time.Now()
	g.ws.Close()
	if _, err := e.conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the local connection to close at the recovery deadline")
	}
	if d := time.Since(start); d < 800*time.Millisecond || d > 3*time.Second {
		t.Fatalf("recovery ended after %v, want about recovery.grace (1s)", d)
	}
	s.mu.Lock()
	attempts := s.recoveryAttempts
	s.mu.Unlock()
	if attempts != 0 {
		t.Fatalf("a resume was sent over the cooled-down carrier (%d attempts)", attempts)
	}
}
