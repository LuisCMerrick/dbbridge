package ingress

import (
	"context"
	"errors"
	"fmt"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
)

// errLocalBusy reports that a probe or precheck could not obtain a local
// probe slot or budget. The endpoint was never contacted, so it must not be
// recorded as an endpoint failure.
var errLocalBusy = errors.New("local probe capacity unavailable")

// probeCall is one shared, in-flight on-demand probe of one endpoint.
type probeCall struct {
	done   chan struct{}
	result *protocol.ProbeResponse
	err    error
	rtt    time.Duration // how long the endpoint took to answer
}

// probeCacheEntry is the latest successful probe answer of one endpoint.
type probeCacheEntry struct {
	pr  *protocol.ProbeResponse
	at  time.Time
	rtt time.Duration
}

// probeReuseMax caps how old a probe answer a new session may build on.
const probeReuseMax = 15 * time.Second

// probeReuseTTL is how long a successful probe answer may stand in for a
// fresh one when a new session is created: one and a half health-check
// intervals (the health loop refreshes it every interval), at most
// probeReuseMax. A stale answer costs at most one failed create, after which
// the cache entry is dropped and the next selection probes for real.
func (m *IngressManager) probeReuseTTL(peer string) time.Duration {
	interval := 10 * time.Second
	for _, p := range m.cfg.Peers {
		if p.ID == peer && p.Failover.HealthcheckInterval > 0 {
			interval = p.Failover.HealthcheckInterval
		}
	}
	return min(interval+interval/2, probeReuseMax)
}

func probeKey(peer string, c *transport.TripleTransportClient, node string) string {
	return peer + "|" + c.URL() + "|" + node
}

// cachedProbe returns a recent successful probe answer for the endpoint, or
// nil. Only new-session selection uses it: it saves the probe round trip on
// every connection while the health loop keeps the answer fresh.
func (m *IngressManager) cachedProbe(peer string, c *transport.TripleTransportClient, node string) *protocol.ProbeResponse {
	m.preMu.Lock()
	defer m.preMu.Unlock()
	e, ok := m.probeCache[probeKey(peer, c, node)]
	if !ok || time.Since(e.at) > m.probeReuseTTL(peer) {
		return nil
	}
	return e.pr
}

// guardTimeout bounds how long a create built on a cached probe answer
// waits for its guard probe: a few times the time the endpoint last took to
// answer a probe, at least guardMin, at most the probe timeout.
func (m *IngressManager) guardTimeout(peer string, c *transport.TripleTransportClient, node string) time.Duration {
	limit := m.probeTimeout(peer)
	m.preMu.Lock()
	rtt := m.probeCache[probeKey(peer, c, node)].rtt
	m.preMu.Unlock()
	if rtt <= 0 {
		return limit
	}
	return min(limit, max(guardMin, guardFactor*rtt))
}

// guardDelay is how long a create built on a cached probe answer runs before
// its guard probe starts: twice the endpoint's last probe time, at least
// guardDelayMin, at most guardTimeout.
func (m *IngressManager) guardDelay(peer string, c *transport.TripleTransportClient, node string) time.Duration {
	m.preMu.Lock()
	rtt := m.probeCache[probeKey(peer, c, node)].rtt
	m.preMu.Unlock()
	return min(m.guardTimeout(peer, c, node), max(guardDelayMin, 2*rtt))
}

const (
	guardDelayMin = 500 * time.Millisecond
	guardMin      = time.Second
	guardFactor   = 4
)

// invalidateProbe drops the cached answer, so the next selection probes.
func (m *IngressManager) invalidateProbe(peer string, c *transport.TripleTransportClient, node string) {
	m.preMu.Lock()
	delete(m.probeCache, probeKey(peer, c, node))
	m.preMu.Unlock()
}

// probeTimeout is the per-probe deadline: healthcheck_timeout, never longer
// than control_request_timeout.
func (m *IngressManager) probeTimeout(peer string) time.Duration {
	to := m.cfg.Transport.ControlRequestTimeout
	for _, p := range m.cfg.Peers {
		if p.ID == peer && p.Failover.HealthcheckTimeout > 0 && (to <= 0 || p.Failover.HealthcheckTimeout < to) {
			to = p.Failover.HealthcheckTimeout
		}
	}
	if to <= 0 {
		to = 3 * time.Second
	}
	return to
}

// probe asks one endpoint for its identity. Concurrent probes of the same
// endpoint share one request (singleflight), so N sessions created at once
// cost one probe and one probe slot. The shared request runs under the
// manager's context with probeTimeout; each caller waits with its own ctx.
func (m *IngressManager) probe(ctx context.Context, peer string, c *transport.TripleTransportClient, node string) (*protocol.ProbeResponse, error) {
	key := probeKey(peer, c, node)
	m.preMu.Lock()
	call := m.probeFlights[key]
	if call == nil {
		call = &probeCall{done: make(chan struct{})}
		m.probeFlights[key] = call
		m.wg.Add(1)
		go m.runProbe(key, call, peer, c, node)
	}
	m.preMu.Unlock()
	select {
	case <-call.done:
		return call.result, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *IngressManager) runProbe(key string, call *probeCall, peer string, c *transport.TripleTransportClient, node string) {
	defer m.wg.Done()
	defer func() {
		m.preMu.Lock()
		delete(m.probeFlights, key)
		switch {
		case call.err == nil && call.result != nil:
			m.probeCache[key] = probeCacheEntry{call.result, time.Now(), call.rtt}
		case call.err != nil && !errors.Is(call.err, errLocalBusy) && !localResourceError(call.err):
			delete(m.probeCache, key)
		}
		m.preMu.Unlock()
		close(call.done)
	}()
	timeout := m.probeTimeout(peer)
	// Waiting for a local slot is bounded separately so that local
	// contention never eats into the endpoint's own response time.
	slotCtx, slotCancel := context.WithTimeout(m.ctx, timeout)
	release, err := m.acquireProbeSlot(slotCtx, peer)
	slotCancel()
	if err != nil {
		call.err = err
		return
	}
	defer release()
	start := time.Now()
	pctx, cancel := context.WithTimeout(m.ctx, timeout)
	defer cancel()
	call.result, call.err = c.Probe(pctx, node, generateID(16))
	call.rtt = time.Since(start)
	if m.ctx.Err() != nil {
		if call.err != nil {
			// Shutting down: says nothing about the endpoint.
			call.err = fmt.Errorf("%w: %v", errLocalBusy, call.err)
		}
		return
	}
	if call.err != nil && localResourceError(call.err) {
		// The request never left this node (a local connection or request
		// budget wait ran out): local contention, not an endpoint failure.
		return
	}
	m.observePhase(peer, c.URL(), "probe", start, call.err == nil)
	// Every real probe is recorded exactly once, however many callers share it.
	if id, ok := m.endpointID(peer, c.URL()); ok {
		if call.err != nil {
			m.linkManagers[peer].RecordFailure(id, errors.Is(call.err, transport.ErrProtocol))
		} else {
			m.linkManagers[peer].RecordSuccess(id, call.result)
		}
	}
}

// acquireProbeSlot takes the global probe budget and a per-peer probe slot.
// Failure to obtain either is local contention and is reported as errLocalBusy.
func (m *IngressManager) acquireProbeSlot(ctx context.Context, peer string) (func(), error) {
	if err := m.cfg.Resources.Probes.Take(ctx, 1); err != nil {
		return nil, fmt.Errorf("%w: %v", errLocalBusy, err)
	}
	select {
	case m.probeSlots[peer] <- struct{}{}:
		return func() { <-m.probeSlots[peer]; m.cfg.Resources.Probes.Put(1) }, nil
	case <-ctx.Done():
		m.cfg.Resources.Probes.Put(1)
		return nil, fmt.Errorf("%w: %v", errLocalBusy, ctx.Err())
	}
}

// localOrCanceled reports errors that say nothing about the endpoint: local
// slot contention, or the caller's own context ending.
func localOrCanceled(ctx context.Context, e error) bool {
	return errors.Is(e, errLocalBusy) || ctx.Err() != nil
}
func (m *IngressManager) healthLoop(p config.PeerConfig) {
	defer m.wg.Done()
	interval := p.Failover.HealthcheckInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	// The first round runs at once, so the first new session after a start
	// can already reuse its answer instead of probing (see cachedProbe).
	first := make(chan time.Time, 1)
	first <- time.Now()
	for {
		select {
		case <-m.stopCh:
			return
		case <-first:
		case <-t.C:
		}
		for _, ep := range m.linkManagers[p.ID].GetEndpoints() {
			// The probe is bounded by healthcheck_timeout and records its
			// own outcome on the endpoint; local contention is not
			// recorded because the endpoint was never contacted.
			c := m.clients[p.ID].WithURL(ep.URL)
			pr, err := m.probe(m.ctx, p.ID, c, p.ExpectedNodeID)
			if m.ctx.Err() != nil {
				return
			}
			if err == nil && m.peerAllowsWS(p) && contains(pr.Transports, "ws") && ep.TransportAvailable(time.Now(), "ws") {
				m.refreshPrecheck(p.ID, c, p.ExpectedNodeID, pr.InstanceID, interval+interval/2)
			}
		}
	}
}

// peerAllowsWS reports whether the peer's transport policy can select WS.
func (m *IngressManager) peerAllowsWS(p config.PeerConfig) bool {
	strategy := m.cfg.Transport.Protocol
	if p.Transport != nil && p.Transport.Protocol != "" {
		strategy = p.Transport.Protocol
	}
	return strategy != "http-sse"
}
