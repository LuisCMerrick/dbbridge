package link

import (
	"sort"
	"sync"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
)

type EndpointState string

const (
	StateUnknown    EndpointState = "UNKNOWN"
	StateHealthy    EndpointState = "HEALTHY"
	StateUnhealthy  EndpointState = "UNHEALTHY"
	StateMismatched EndpointState = "MISMATCHED"
)

type Endpoint struct {
	mu                   sync.RWMutex
	ID                   string
	URL                  string
	Priority             int
	State                EndpointState
	CooldownUntil        time.Time
	WSCooldownUntil      time.Time
	SSECooldownUntil     time.Time
	ConsecutiveFailures  int
	ConsecutiveSuccesses int
	LastProbedInstanceID string
	AcceptingNewSessions bool
	RecoveryAvailable    bool
	SupportedTransports  []string // from probe response
}

// Failures returns the number of consecutive failed probes.
func (ep *Endpoint) Failures() int {
	ep.mu.RLock()
	defer ep.mu.RUnlock()
	return ep.ConsecutiveFailures
}

func (ep *Endpoint) GetState() EndpointState {
	ep.mu.RLock()
	defer ep.mu.RUnlock()
	return ep.State
}

type PeerLinkManager struct {
	mu             sync.RWMutex
	peerID         string
	expectedNodeID string
	failoverCfg    config.FailoverConfig
	endpoints      []*Endpoint
}

func NewPeerLinkManager(pCfg config.PeerConfig) *PeerLinkManager {
	mgr := &PeerLinkManager{
		peerID:         pCfg.ID,
		expectedNodeID: pCfg.ExpectedNodeID,
		failoverCfg:    pCfg.Failover,
		endpoints:      make([]*Endpoint, 0, len(pCfg.Endpoints)),
	}

	for _, ep := range pCfg.Endpoints {
		mgr.endpoints = append(mgr.endpoints, &Endpoint{
			ID:                   ep.ID,
			URL:                  ep.URL,
			Priority:             ep.Priority,
			State:                StateUnknown,
			AcceptingNewSessions: true,
			RecoveryAvailable:    true,
			SupportedTransports:  []string{protocol.TransportModeWS, protocol.TransportModeSSE},
		})
	}

	// Sort endpoints by priority ascending (lower number = higher priority)
	sort.SliceStable(mgr.endpoints, func(i, j int) bool {
		return mgr.endpoints[i].Priority < mgr.endpoints[j].Priority
	})

	return mgr
}

func (m *PeerLinkManager) GetEndpoints() []*Endpoint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Endpoint, len(m.endpoints))
	copy(out, m.endpoints)
	return out
}

func (m *PeerLinkManager) RecordSuccess(epID string, probe *protocol.ProbeResponse) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, ep := range m.endpoints {
		if ep.ID == epID {
			ep.mu.Lock()
			ep.ConsecutiveSuccesses++
			ep.ConsecutiveFailures = 0
			if probe != nil {
				ep.LastProbedInstanceID = probe.InstanceID
				ep.AcceptingNewSessions = probe.AcceptingNewSessions
				ep.RecoveryAvailable = probe.RecoveryAvailable
				if len(probe.Transports) > 0 {
					ep.SupportedTransports = append([]string(nil), probe.Transports...)
				}
			}
			if ep.ConsecutiveSuccesses >= max(m.failoverCfg.HealthyThreshold, 1) {
				// healthy_threshold successes end the UNHEALTHY state, but a
				// running failure cooldown is never cut short: a cheap probe
				// must not send sessions straight back to a failing path.
				ep.State = StateHealthy
			}
			ep.mu.Unlock()
			return
		}
	}
}

func (m *PeerLinkManager) RecordFailure(epID string, isIdentityMismatch bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	for _, ep := range m.endpoints {
		if ep.ID == epID {
			ep.mu.Lock()
			ep.ConsecutiveFailures++
			ep.ConsecutiveSuccesses = 0
			cooldownDur := m.cooldown()
			// unhealthy_threshold decides when an endpoint stops being selected:
			// failures below it keep the endpoint eligible, so one lost probe
			// cannot black out a (single-endpoint) peer for the whole cooldown.
			threshold := m.failoverCfg.UnhealthyThreshold
			if threshold <= 0 {
				threshold = 1
			}
			if isIdentityMismatch || ep.ConsecutiveFailures >= threshold {
				ep.CooldownUntil = now.Add(cooldownDur)
			}
			if isIdentityMismatch {
				ep.State = StateMismatched
			} else if ep.ConsecutiveFailures >= threshold {
				ep.State = StateUnhealthy
			}
			ep.mu.Unlock()
			return
		}
	}
}

// RecordTransportFailure marks failure for a specific transport on an endpoint.
func (m *PeerLinkManager) RecordTransportFailure(epID string, transportMode string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	cooldownDur := m.cooldown()

	for _, ep := range m.endpoints {
		if ep.ID == epID {
			ep.mu.Lock()
			ep.coolTransportLocked(transportMode, now.Add(cooldownDur))
			ep.mu.Unlock()
			return
		}
	}
}

// coolTransportLocked extends (never shortens) the cooldown of one carrier.
func (ep *Endpoint) coolTransportLocked(mode string, until time.Time) {
	p := &ep.SSECooldownUntil
	if mode == protocol.TransportModeWS {
		p = &ep.WSCooldownUntil
	}
	if until.After(*p) {
		*p = until
	}
}

// RecordPathFailure records a whole-path transport error of a real request
// (not a probe) on an endpoint: the address enters the failure cooldown for
// this fault cycle at once, independent of unhealthy_threshold, which only
// governs the probe-driven UNHEALTHY state.
func (m *PeerLinkManager) RecordPathFailure(epID string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	until := time.Now().Add(m.cooldown())
	for _, ep := range m.endpoints {
		if ep.ID == epID {
			ep.mu.Lock()
			if until.After(ep.CooldownUntil) {
				ep.CooldownUntil = until
			}
			ep.mu.Unlock()
			return
		}
	}
}

func (m *PeerLinkManager) cooldown() time.Duration {
	if m.failoverCfg.Cooldown == 0 && m.failoverCfg.CooldownRaw == nil {
		return 5 * time.Second
	}
	return m.failoverCfg.Cooldown
}

// RecordTransportDrop records that an established carrier of transportMode
// was lost. The first drop already cools that (Endpoint, transport) pair down
// for this fault cycle, so recovery does not return to the carrier that just
// failed. Other carriers and the address itself
// are not affected.
func (m *PeerLinkManager) RecordTransportDrop(epID string, transportMode string) {
	m.RecordTransportFailure(epID, transportMode)
}

func (m *PeerLinkManager) ExpectedNodeID() string {
	return m.expectedNodeID
}

// Available consults endpoint health without exposing mutable state to session workers.
func (ep *Endpoint) Available(now time.Time, owner string, forNew bool) bool {
	ep.mu.RLock()
	defer ep.mu.RUnlock()
	return !now.Before(ep.CooldownUntil) && ep.State != StateUnhealthy && ep.State != StateMismatched && (!forNew || ep.AcceptingNewSessions) && (owner == "" || ep.LastProbedInstanceID == "" || ep.LastProbedInstanceID == owner)
}

func (ep *Endpoint) ProbeEligible(now time.Time) bool {
	ep.mu.RLock()
	defer ep.mu.RUnlock()
	return !now.Before(ep.CooldownUntil)
}

// NextEligible returns when the endpoint leaves its address cooldown with at
// least one of modes out of its transport cooldown, or the zero time if that
// is already the case at now.
func (ep *Endpoint) NextEligible(now time.Time, modes ...string) time.Time {
	ep.mu.RLock()
	defer ep.mu.RUnlock()
	var carrier time.Time
	for i, mode := range modes {
		t := ep.SSECooldownUntil
		if mode == protocol.TransportModeWS {
			t = ep.WSCooldownUntil
		}
		if i == 0 || t.Before(carrier) {
			carrier = t
		}
	}
	next := ep.CooldownUntil
	if carrier.After(next) {
		next = carrier
	}
	if !next.After(now) {
		return time.Time{}
	}
	return next
}

func (ep *Endpoint) TransportAvailable(now time.Time, mode string) bool {
	ep.mu.RLock()
	defer ep.mu.RUnlock()
	if mode == "ws" {
		return !now.Before(ep.WSCooldownUntil)
	}
	return !now.Before(ep.SSECooldownUntil)
}
