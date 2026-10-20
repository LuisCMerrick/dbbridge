package ingress

import (
	"log/slog"
	"time"

	"dbbridge/internal/config"
)

// peerCooldown is the failure cooldown the link manager applies for p.
func peerCooldown(p config.PeerConfig) time.Duration {
	if p.Failover.Cooldown == 0 && p.Failover.CooldownRaw == nil {
		return 5 * time.Second
	}
	return p.Failover.Cooldown
}

// warnGraceBelowCooldown logs once per peer when the recovery grace a
// session got (possibly lowered by the peer) cannot outlast the cooldown of
// the only carrier recovery could use: such sessions end at the recovery
// deadline without ever sending a resume.
func (m *IngressManager) warnGraceBelowCooldown(p config.PeerConfig, strategy string, grace time.Duration) {
	if !config.SingleRecoveryCarrier(p, strategy) {
		return
	}
	cooldown := peerCooldown(p)
	if grace > cooldown+m.cfg.Transport.Recovery.BarrierTimeout {
		return
	}
	if _, warned := m.graceWarned.LoadOrStore(p.ID, true); warned {
		return
	}
	slog.Warn("recovery grace does not exceed the failover cooldown of the only usable carrier; broken carriers of this peer cannot be resumed", "peer", p.ID, "grace", grace, "cooldown", cooldown, "barrier_timeout", m.cfg.Transport.Recovery.BarrierTimeout, "transport", strategy)
}
