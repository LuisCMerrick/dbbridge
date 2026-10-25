package ingress

import (
	"context"
	"errors"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/transport"
)

func TestAuditLocalControlConnectionContentionDoesNotCoolEndpoint(t *testing.T) {
	e := testSetup(t, "auto", false, func(c *config.Config) {
		c.Peers[0].Failover.HealthcheckIntervalRaw = "500ms"
		c.Peers[0].Failover.HealthcheckTimeoutRaw = "50ms"
	})
	b := e.m.cfg.Resources.ControlConnections
	if !b.Try(b.Limit()) {
		t.Fatal("cannot occupy local connection budget")
	}
	defer b.Put(b.Limit())
	p := e.m.cfg.Peers[0]
	for i := 0; i < 2; i++ {
		_, err := e.m.probe(context.Background(), p.ID, e.m.clients[p.ID], p.ExpectedNodeID)
		if !errors.Is(err, transport.ErrLocalConnBudget) {
			t.Fatalf("want local connection contention, got %v", err)
		}
	}
	if e.probes.Load() != 0 {
		t.Fatalf("fixture made %d remote probes", e.probes.Load())
	}
	ep := e.m.linkManagers[p.ID].GetEndpoints()[0]
	if ep.Failures() != 0 || !ep.ProbeEligible(time.Now()) {
		t.Fatalf("purely local contention recorded as remote path failures: failures=%d state=%s eligible=%t remote_probes=%d", ep.Failures(), ep.GetState(), ep.ProbeEligible(time.Now()), e.probes.Load())
	}
}
