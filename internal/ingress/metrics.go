package ingress

import (
	"fmt"
	"io"
	"sort"
	"time"

	"dbbridge/internal/link"
	"dbbridge/internal/observability"
)

type phaseKey struct {
	peer, endpoint, phase string
	success               bool
}
type phaseValue struct {
	count   int64
	seconds float64
}

// endpointID maps a configured endpoint URL to its ID without allocating
// (observePhase runs on every ACK).
func (m *IngressManager) endpointID(peer, url string) (string, bool) {
	for _, p := range m.cfg.Peers {
		if p.ID != peer {
			continue
		}
		for _, ep := range p.Endpoints {
			if ep.URL == url {
				return ep.ID, true
			}
		}
	}
	return "", false
}

// Only configured Peer/Endpoint identities and internal phase constants enter
// labels. URLs, payloads, session IDs and error text never enter this map.
func (m *IngressManager) observePhase(peer, url, phase string, start time.Time, success bool) {
	observability.ObserveDuration("ingress", phase, time.Since(start))
	m.observeEndpointPhase(peer, url, phase, start, success)
}

// observeEndpointPhase records one phase only in the per-endpoint statistics.
func (m *IngressManager) observeEndpointPhase(peer, url, phase string, start time.Time, success bool) {
	elapsed := time.Since(start)
	id, ok := m.endpointID(peer, url)
	if !ok {
		return
	}
	m.diagMu.Lock()
	if m.phases == nil {
		m.phases = map[phaseKey]phaseValue{}
	}
	k := phaseKey{peer, id, phase, success}
	v := m.phases[k]
	v.count++
	v.seconds += elapsed.Seconds()
	m.phases[k] = v
	m.diagMu.Unlock()
}

var endpointStates = []link.EndpointState{link.StateUnknown, link.StateHealthy, link.StateUnhealthy, link.StateMismatched}

// WriteMetrics writes the ingress families in Prometheus text format: each
// family has HELP/TYPE and contiguous samples in a stable order, and the
// endpoint state is exported as a 0/1 series for every state.
func (m *IngressManager) WriteMetrics(w io.Writer) {
	peers := make([]string, 0, len(m.linkManagers))
	for peer := range m.linkManagers {
		peers = append(peers, peer)
	}
	sort.Strings(peers)
	fmt.Fprintf(w, "# HELP dbb_endpoint_state Endpoint health state (1 for the current state)\n# TYPE dbb_endpoint_state gauge\n")
	for _, peer := range peers {
		for _, ep := range m.linkManagers[peer].GetEndpoints() {
			current := ep.GetState()
			for _, st := range endpointStates {
				v := 0
				if st == current {
					v = 1
				}
				fmt.Fprintf(w, "dbb_endpoint_state{peer=%q,endpoint=%q,state=%q} %d\n", peer, ep.ID, st, v)
			}
		}
	}
	m.diagMu.Lock()
	keys := make([]phaseKey, 0, len(m.phases))
	values := make(map[phaseKey]phaseValue, len(m.phases))
	for k, v := range m.phases {
		keys = append(keys, k)
		values[k] = v
	}
	m.diagMu.Unlock()
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.peer != b.peer {
			return a.peer < b.peer
		}
		if a.endpoint != b.endpoint {
			return a.endpoint < b.endpoint
		}
		if a.phase != b.phase {
			return a.phase < b.phase
		}
		return !a.success && b.success
	})
	fmt.Fprintf(w, "# HELP dbb_phase_duration_seconds Duration of ingress phases per endpoint\n# TYPE dbb_phase_duration_seconds summary\n")
	for _, k := range keys {
		fmt.Fprintf(w, "dbb_phase_duration_seconds_sum{peer=%q,endpoint=%q,phase=%q,success=%q} %g\n", k.peer, k.endpoint, k.phase, fmt.Sprint(k.success), values[k].seconds)
		fmt.Fprintf(w, "dbb_phase_duration_seconds_count{peer=%q,endpoint=%q,phase=%q,success=%q} %d\n", k.peer, k.endpoint, k.phase, fmt.Sprint(k.success), values[k].count)
	}
	type binding struct {
		peer, endpoint, mode string
		recovery             bool
	}
	bound := map[binding]int{}
	var upPending, retained, recovering uint64
	m.activeConns.Range(func(_, v any) bool {
		s := v.(*activeConnEntry).session
		if s == nil {
			return true
		}
		s.mu.Lock()
		// Only an active generation is bound; a session in recovery or in its
		// first attach is counted as recovering or not at all.
		if g := s.g; g != nil && g.endpoint != nil && g.active {
			bound[binding{s.p.ID, g.endpoint.ID, g.mode, s.recovery}]++
		} else if !s.deadline.IsZero() && !s.closed.Load() {
			recovering++
		}
		upPending += s.upNext - s.upAck
		retained += uint64(len(s.frames))
		s.mu.Unlock()
		return true
	})
	bindings := make([]binding, 0, len(bound))
	for k := range bound {
		bindings = append(bindings, k)
	}
	sort.Slice(bindings, func(i, j int) bool {
		return fmt.Sprint(bindings[i]) < fmt.Sprint(bindings[j])
	})
	fmt.Fprintf(w, "# HELP dbb_ingress_bound_sessions Ingress sessions by bound endpoint and carrier\n# TYPE dbb_ingress_bound_sessions gauge\n")
	for _, k := range bindings {
		fmt.Fprintf(w, "dbb_ingress_bound_sessions{peer=%q,endpoint=%q,transport=%q,recovery=%q} %d\n", k.peer, k.endpoint, k.mode, fmt.Sprint(k.recovery), bound[k])
	}
	fmt.Fprintf(w, "# HELP dbb_ingress_recovering_sessions Ingress sessions inside a recovery window without an active carrier\n# TYPE dbb_ingress_recovering_sessions gauge\ndbb_ingress_recovering_sessions %d\n", recovering)
	fmt.Fprintf(w, "# HELP dbb_ingress_up_pending_bytes Upstream bytes sent but not yet acknowledged\n# TYPE dbb_ingress_up_pending_bytes gauge\ndbb_ingress_up_pending_bytes %d\n", upPending)
	fmt.Fprintf(w, "# HELP dbb_ingress_retained_frames Upstream frames retained for replay\n# TYPE dbb_ingress_retained_frames gauge\ndbb_ingress_retained_frames %d\n", retained)
}
