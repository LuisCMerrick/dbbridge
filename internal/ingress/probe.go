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
	key := peer + "|" + c.URL() + "|" + node
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
	for {
		select {
		case <-m.stopCh:
			return
		case <-t.C:
			for _, ep := range m.linkManagers[p.ID].GetEndpoints() {
				// The probe is bounded by healthcheck_timeout and records its
				// own outcome on the endpoint; local contention is not
				// recorded because the endpoint was never contacted.
				_, _ = m.probe(m.ctx, p.ID, m.clients[p.ID].WithURL(ep.URL), p.ExpectedNodeID)
				if m.ctx.Err() != nil {
					return
				}
			}
		}
	}
}
