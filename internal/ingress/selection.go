package ingress

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/link"
	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
)

func contains(v []string, s string) bool {
	for _, x := range v {
		if x == s {
			return true
		}
	}
	return false
}
func (m *IngressManager) choose(ctx context.Context, p config.PeerConfig, owner, policy string, forNew bool) (*transport.TripleTransportClient, *link.Endpoint, string, error) {
	sel, err := m.chooseUntried(ctx, p, owner, policy, forNew, nil)
	return sel.c, sel.ep, sel.mode, err
}

// selection is one candidate chosen for a create or resume attempt.
type selection struct {
	c    *transport.TripleTransportClient
	ep   *link.Endpoint
	mode string
	// pr is the probe answer the selection was based on.
	pr *protocol.ProbeResponse
	// fellBack is true when auto chose SSE on an endpoint that offers WS,
	// because WS was not usable there.
	fellBack bool
}

// Attempts belong to one immutable create/resume operation. A successful probe
// cannot erase a failure on that operation's HTTP path, even with cooldown=0.
// The returned probe response is the one the selection was based on, so callers
// reuse it instead of probing the chosen endpoint a second time.
//
// A failure cooldown is never bypassed (requirements 3.2(4), 3.8): the pass
// skips cooled-down addresses and carriers. When every remaining candidate is
// cooling down, the selection waits (with jitter) until the earliest one
// becomes eligible or ctx, the create retry / recovery deadline, ends, and
// then runs one more pass; callers keep their own jittered backoff.
func (m *IngressManager) chooseUntried(ctx context.Context, p config.PeerConfig, owner, policy string, forNew bool, attempted map[string]bool) (selection, error) {
	probed := map[string]*protocol.ProbeResponse{}
	var sel selection
	var err error
	sel.c, sel.ep, sel.mode, sel.pr, sel.fellBack, err = m.choosePass(ctx, p, owner, policy, forNew, attempted, false, probed)
	if err == nil || fatalError(err) || ctx.Err() != nil {
		return sel, err
	}
	next := cooldownEnd(m.linkManagers[p.ID], p, policy, attempted, time.Now())
	if next.IsZero() || waitCooldown(ctx, time.Until(next)) != nil {
		return selection{}, err
	}
	// Probe again: the earlier answers predate the wait.
	clear(probed)
	sel.c, sel.ep, sel.mode, sel.pr, sel.fellBack, err = m.choosePass(ctx, p, owner, policy, forNew, attempted, false, probed)
	return sel, err
}

// cooldownEnd returns the earliest time a remaining candidate leaves its
// failure cooldown, or zero when some candidate is not cooling down (then a
// retry is decided by the caller's backoff, not by a cooldown).
func cooldownEnd(mgr *link.PeerLinkManager, p config.PeerConfig, policy string, attempted map[string]bool, now time.Time) time.Time {
	modes := []string{protocol.TransportModeWS, protocol.TransportModeSSE}
	if policy == protocol.TransportModeWS || policy == protocol.TransportModeSSE {
		modes = []string{policy}
	}
	var first time.Time
	for _, ep := range orderedEndpoints(mgr, p.Failover.IsEnabled(len(p.Endpoints))) {
		if attempted[ep.ID] {
			continue
		}
		t := ep.NextEligible(now, modes...)
		if t.IsZero() {
			return time.Time{}
		}
		if first.IsZero() || t.Before(first) {
			first = t
		}
	}
	return first
}

// waitCooldown sleeps d plus a small random jitter, bounded by ctx.
func waitCooldown(ctx context.Context, d time.Duration) error {
	if d < 0 {
		d = 0
	}
	d += time.Duration(rand.Int64N(int64(min(d/10, 100*time.Millisecond)) + int64(time.Millisecond)))
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// orderedEndpoints returns the candidates in selection order. With failover
// enabled, endpoints whose latest probe failed go after the ones that did
// not fail, so a black-holed primary is not probed (and waited for) by every
// new session while a healthy backup exists; the health loop's next success
// restores the priority order (failback=new_sessions).
func orderedEndpoints(mgr *link.PeerLinkManager, failover bool) []*link.Endpoint {
	eps := mgr.GetEndpoints()
	if !failover {
		return eps[:min(1, len(eps))]
	}
	out := make([]*link.Endpoint, 0, len(eps))
	for _, ep := range eps {
		if ep.Failures() == 0 {
			out = append(out, ep)
		}
	}
	for _, ep := range eps {
		if ep.Failures() != 0 {
			out = append(out, ep)
		}
	}
	return out
}

func (m *IngressManager) choosePass(ctx context.Context, p config.PeerConfig, owner, policy string, forNew bool, attempted map[string]bool, ignoreCooldown bool, probed map[string]*protocol.ProbeResponse) (*transport.TripleTransportClient, *link.Endpoint, string, *protocol.ProbeResponse, bool, error) {
	mgr := m.linkManagers[p.ID]
	var unsafe error
	var mismatch error
	var temporary error
	pending := false
	ownerSeen := false
	usable := func(ep *link.Endpoint, mode string) bool {
		return ignoreCooldown || ep.TransportAvailable(time.Now(), mode)
	}
	for _, ep := range orderedEndpoints(mgr, p.Failover.IsEnabled(len(p.Endpoints))) {
		if attempted[ep.ID] {
			pending = true
			continue
		}
		if !ep.ProbeEligible(time.Now()) {
			pending = true
			continue
		}
		c := m.clients[p.ID].WithURL(ep.URL)
		pr, e := probed[ep.ID], error(nil)
		if pr == nil {
			pr, e = m.probe(ctx, p.ID, c, p.ExpectedNodeID)
		}
		if e != nil {
			if localOrCanceled(ctx, e) {
				// Not an endpoint failure: local slot contention or our own deadline.
				pending = true
				continue
			}
			// The probe itself recorded the failure (once per real request).
			if errors.Is(e, transport.ErrProtocol) {
				unsafe = e
			} else {
				pending = true
			}
			continue
		}
		if owner != "" && pr.InstanceID != owner {
			// A different instance (e.g. an HA backup) is simply not a candidate
			// for this session; it is fatal only if no endpoint reports the owner.
			mismatch = fmt.Errorf("%w: endpoint instance mismatch", transport.ErrProtocol)
			continue
		}
		if owner != "" {
			ownerSeen = true
		}
		probed[ep.ID] = pr
		if !ep.Available(time.Now(), owner, forNew) {
			pending = true
			continue
		}
		mode := "http-sse"
		if policy != "http-sse" && contains(pr.Transports, "ws") && usable(ep, "ws") {
			checkCtx, cancel := context.WithTimeout(ctx, m.cfg.Transport.ControlRequestTimeout)
			e = m.precheck(checkCtx, p.ID, c, p.ExpectedNodeID, pr.InstanceID)
			checkDone := checkCtx.Err() != nil
			cancel()
			if e == nil {
				mode = "ws"
			} else {
				switch classifyPrecheck(ctx, checkDone, e) {
				case precheckUnsafe:
					// 3.7.3(1,3): TLS, identity, subprotocol, version or illegal
					// hello data isolates this candidate; never an SSE fallback here.
					mgr.RecordFailure(ep.ID, true)
					unsafe = e
					continue
				case precheckAuthRejected:
					var remote *transport.RemoteError
					if errors.As(e, &remote) && remote.Trusted {
						// A trusted DBBridge 401/403 is a verdict on this node's
						// credentials, final for the session (3.7.3(2)).
						return nil, nil, "", nil, false, e
					}
					// A raw 401 on the authenticated Upgrade is not tolerated by
					// falling back to SSE either; the address itself stays healthy.
					unsafe = fmt.Errorf("%w: WS precheck refused: %v", transport.ErrProtocol, e)
					continue
				case precheckTemporary:
					var remote *transport.RemoteError
					if errors.As(e, &remote) && remote.Trusted && remote.Status >= 500 && !safeRetry(e) {
						// An egress-side server error is not an identity problem.
						mgr.RecordFailure(ep.ID, false)
					}
					pending = true
					temporary = e
					continue
				}
				// WS is unavailable on this endpoint for now (an upgrade
				// refusal, any ordinary HTTP answer of a gateway, proxy or WAF,
				// or a transport-level failure of the WS path). Only the
				// (endpoint, ws) pair cools down (3.8); the address and its SSE
				// carrier stay eligible.
				mgr.RecordTransportFailure(ep.ID, "ws")
				if policy == "ws" || !contains(pr.Transports, "http-sse") {
					// WS cooldown is temporary; the endpoint stays a future candidate.
					pending = true
					continue
				}
				again, pe := m.probe(ctx, p.ID, c, p.ExpectedNodeID)
				if pe != nil {
					pending = true
					continue
				}
				if again.InstanceID != pr.InstanceID || !contains(again.Transports, "http-sse") {
					pending = true
					continue
				}
				pr = again
			}
		}
		want := mode
		if policy == "ws" {
			want = "ws"
		}
		if want != mode || !contains(pr.Transports, mode) || !usable(ep, mode) {
			if contains(pr.Transports, want) {
				// The endpoint supports the required transport but it is not
				// usable right now (e.g. transport cooldown after a carrier drop).
				pending = true
			}
			continue
		}
		// auto wanted WS here and it was not usable (precheck failed now, or
		// earlier and the WS carrier is still cooling down).
		fellBack := policy == "auto" && mode == "http-sse" && contains(pr.Transports, "ws")
		return c, ep, mode, pr, fellBack, nil
	}
	if unsafe != nil && !pending {
		return nil, nil, "", nil, false, unsafe
	}
	if mismatch != nil && !pending && !ownerSeen {
		return nil, nil, "", nil, false, mismatch
	}
	if temporary != nil {
		return nil, nil, "", nil, false, temporary
	}
	return nil, nil, "", nil, false, errors.New("no eligible endpoint")
}
