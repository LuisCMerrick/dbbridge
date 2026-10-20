package config

import "fmt"

// SingleRecoveryCarrier reports whether every recovery of peer p can only use
// one (endpoint, carrier) pair: the transport is forced to ws or http-sse and
// failover offers no second endpoint. strategy is the global
// transport.protocol; a peer override takes precedence.
func SingleRecoveryCarrier(p PeerConfig, strategy string) bool {
	if p.Transport != nil && p.Transport.Protocol != "" {
		strategy = p.Transport.Protocol
	}
	if strategy != "ws" && strategy != "http-sse" {
		return false
	}
	return len(p.Endpoints) <= 1 || !p.Failover.IsEnabled(len(p.Endpoints))
}

// warnRecoveryCooldown reports peers whose recovery can never resume a
// broken carrier: the carrier that just failed is the only candidate, it
// cools down for failover.cooldown, and the recovery deadline
// (recovery.grace, counted from the fault) ends before that cooldown and the
// write barrier are over.
func (c *Config) warnRecoveryCooldown() {
	if !c.Transport.Recovery.Enabled {
		return
	}
	grace := c.Transport.Recovery.Grace
	for i := range c.Peers {
		p := c.Peers[i]
		if !p.EffectiveRecoveryEnabled(true) || !SingleRecoveryCarrier(p, c.Transport.Protocol) {
			continue
		}
		need := p.Failover.Cooldown + c.Transport.Recovery.BarrierTimeout
		if grace <= need {
			c.Warnings = append(c.Warnings, fmt.Sprintf("peers[%s]: transport.recovery.grace (%v) does not exceed failover.cooldown (%v) plus recovery.barrier_timeout (%v); with a forced transport and a single endpoint the carrier that failed stays cooled down until the recovery deadline, so recovery cannot succeed; raise recovery.grace, lower failover.cooldown, use protocol auto or add an endpoint", p.ID, grace, p.Failover.Cooldown, c.Transport.Recovery.BarrierTimeout))
		}
	}
}
