package link

import (
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"testing"
	"time"
)

func TestAudit101811FailureCooldownMustNotBeBypassed(t *testing.T) {
	t.Run("first_transport_drop", func(t *testing.T) {
		m := NewPeerLinkManager(config.PeerConfig{ID: "B", Endpoints: []config.EndpointConfig{{ID: "e", URL: "http://127.0.0.1:1"}}, Failover: config.FailoverConfig{Cooldown: 5 * time.Second}})
		m.RecordTransportDrop("e", "ws")
		if m.GetEndpoints()[0].TransportAvailable(time.Now(), "ws") {
			t.Fatal("contract violation: failed WS carrier remains immediately eligible after its first drop")
		}
	})
	t.Run("success_cannot_clear_cooldown", func(t *testing.T) {
		m := NewPeerLinkManager(config.PeerConfig{ID: "B", Endpoints: []config.EndpointConfig{{ID: "e", URL: "http://127.0.0.1:1"}}, Failover: config.FailoverConfig{Cooldown: 5 * time.Second, UnhealthyThreshold: 1, HealthyThreshold: 2}})
		m.RecordFailure("e", false)
		p := &protocol.ProbeResponse{InstanceID: "bi", AcceptingNewSessions: true, Transports: []string{"ws", "http-sse"}}
		m.RecordSuccess("e", p)
		m.RecordSuccess("e", p)
		if m.GetEndpoints()[0].Available(time.Now(), "bi", true) {
			t.Fatal("contract violation: two immediate probes cleared the 5s endpoint cooldown")
		}
	})
}
