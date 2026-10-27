package ingress

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"dbbridge/internal/transport"
)

// pathCheck asks, with a probe bounded by guardTimeout, whether the
// endpoint's path still answers. It returns nil when the endpoint answered
// (any HTTP answer, even an error status, proves the path) or when nothing
// can be concluded (local contention, ctx ended). When the path is mute it
// closes the endpoint's connections that received nothing during the check,
// so every request still waiting on them fails now instead of after its own
// timeout, keeps new sessions off the address for the failure cooldown when
// another endpoint is usable, and returns the reason.
func (m *IngressManager) pathCheck(ctx context.Context, peer string, c *transport.TripleTransportClient, node string) error {
	limit := m.guardTimeout(peer, c, node)
	start := time.Now()
	gctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	_, e := m.probe(gctx, peer, c, node)
	if e == nil || errors.Is(e, errLocalBusy) || localResourceError(e) || ctx.Err() != nil {
		return nil
	}
	var remote *transport.RemoteError
	if errors.As(e, &remote) || errors.Is(e, transport.ErrProtocol) {
		return nil
	}
	if gctx.Err() != nil {
		// The endpoint answered probes within a fraction of this before.
		// The shared probe keeps running to its own timeout and records the
		// endpoint outcome.
		e = fmt.Errorf("no probe answer within %v", limit)
	}
	n := c.CloseStalled(start)
	m.invalidateProbe(peer, c, node)
	id, _ := m.endpointID(peer, c.URL())
	if m.otherEndpointAvailable(peer, id) {
		m.linkManagers[peer].RecordPathFailure(id)
	}
	if n > 0 {
		slog.Warn("peer endpoint not answering; closed its stalled connections", "peer", peer, "endpoint", id, "connections", n, "err", e)
	} else {
		slog.Debug("peer endpoint not answering", "peer", peer, "endpoint", id, "err", e)
	}
	return e
}

// otherEndpointAvailable reports whether a new session could use another
// endpoint of peer than id right now. A check that misjudges a slow but
// working path (an overloaded host) must not cool down the only usable
// address: then the requests just fail and are retried.
func (m *IngressManager) otherEndpointAvailable(peer, id string) bool {
	lm := m.linkManagers[peer]
	if lm == nil {
		return false
	}
	now := time.Now()
	for _, ep := range lm.GetEndpoints() {
		if ep.ID != id && ep.Available(now, "", true) {
			return true
		}
	}
	return false
}

// watchSlow runs one pathCheck per endpoint at a time when a request has
// waited longer than guardTimeout for its response headers.
func (m *IngressManager) watchSlow(peer string, c *transport.TripleTransportClient, node string) {
	key := probeKey(peer, c, node)
	m.preMu.Lock()
	if m.pathChecks[key] || m.ctx.Err() != nil {
		m.preMu.Unlock()
		return
	}
	m.pathChecks[key] = true
	m.wg.Add(1)
	m.preMu.Unlock()
	go func() {
		defer m.wg.Done()
		defer func() {
			m.preMu.Lock()
			delete(m.pathChecks, key)
			m.preMu.Unlock()
		}()
		m.pathCheck(m.ctx, peer, c, node)
	}()
}
