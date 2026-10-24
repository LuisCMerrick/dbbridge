package node

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// Tunnel metrics on both sides follow the live tunnels and count the
// lifecycle events.
func TestReverseTunnelMetrics(t *testing.T) {
	p := startReversePair(t, "{enabled: false}")
	render := func(n *Node) string {
		var b bytes.Buffer
		n.writeMetrics(&b)
		return b.String()
	}
	for _, want := range []string{
		`dbb_reverse_hub_tunnels{peer="plant",transport="ws"} 2`,
		`dbb_reverse_hub_tunnels{peer="plant",transport="http-sse"} 0`,
		"dbb_reverse_hub_accepted_total 2",
		"# TYPE dbb_reverse_hub_rejected_total counter",
	} {
		if m := render(p.public); !strings.Contains(m, want) {
			t.Errorf("public metrics lack %q:\n%s", want, m)
		}
	}
	for _, want := range []string{
		`dbb_reverse_tunnels{endpoint="primary",transport="ws"} 2`,
		"dbb_reverse_tunnel_established_total 2",
		"dbb_reverse_tunnel_lost_total 0",
		"# TYPE dbb_reverse_tunnel_fallback_total counter",
	} {
		if m := render(p.db); !strings.Contains(m, want) {
			t.Errorf("database-side metrics lack %q:\n%s", want, m)
		}
	}
	if strings.Contains(render(p.db), "dbb_reverse_hub_") || strings.Contains(render(p.public), "dbb_reverse_tunnels{") {
		t.Error("a side renders the other side's tunnel metrics")
	}
	p.db.tunnelCli.CloseTunnels()
	waitFor(t, 5*time.Second, "tunnels back", func() bool {
		return strings.Contains(render(p.db), "dbb_reverse_tunnel_established_total 4") && p.public.hub.Tunnels("plant") == 2 && p.db.tunnelCli.Live() == 2
	})
	m := render(p.db)
	if !strings.Contains(m, "dbb_reverse_tunnel_lost_total 2") || !strings.Contains(m, "dbb_reverse_tunnel_established_total 4") {
		t.Errorf("reconnect not counted:\n%s", m)
	}
}

// /readyz: a database side that is reachable only through its tunnels is
// not ready without one; the public side stays ready and names the peer.
func TestReverseReadiness(t *testing.T) {
	p := startReversePair(t, "{enabled: false}")
	if ok, detail := p.db.reverseReadiness(); !ok || detail != "" {
		t.Fatalf("database side with tunnels: ready=%v detail=%q", ok, detail)
	}
	if ok, detail := p.public.reverseReadiness(); !ok || detail != "" {
		t.Fatalf("public side with tunnels: ready=%v detail=%q", ok, detail)
	}
	p.public.Stop()
	waitFor(t, 5*time.Second, "tunnels gone", func() bool { return p.db.tunnelCli.Live() == 0 })
	if ok, detail := p.db.reverseReadiness(); ok || !strings.Contains(detail, "no tunnel to hq-b") {
		t.Fatalf("database side without tunnels: ready=%v detail=%q", ok, detail)
	}
}

// The public side never goes not-ready for a missing tunnel: a load balancer
// would otherwise keep the database side from dialing in.
func TestReversePublicSideReadyWithoutTunnel(t *testing.T) {
	p := startReversePair(t, "{enabled: false}")
	p.db.Stop()
	waitFor(t, 5*time.Second, "tunnels gone", func() bool { return p.public.hub.Tunnels("plant") == 0 })
	ok, detail := p.public.reverseReadiness()
	if !ok || !strings.Contains(detail, "reverse peers without a tunnel: plant") {
		t.Fatalf("public side without tunnels: ready=%v detail=%q", ok, detail)
	}
}
