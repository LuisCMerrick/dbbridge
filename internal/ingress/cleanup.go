package ingress

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
)

// cleanupResult is the outcome of one remote DELETE attempt.
type cleanupResult int

const (
	cleanupFailed cleanupResult = iota
	cleanupConfirmed
	cleanupUnknown
)

// startRemoteCleanup releases the remote session after a local termination.
//
//   - A normal close needs no DELETE: egress closed the session in the same
//     step in which it acknowledged both FINs, or it already told us
//     so with a trusted 410 SESSION_CLOSED.
//   - Outside shutdown the cleanup budget is only tried; during CloseContext
//     every session waits for the budget, bounded by the shutdown deadline,
//     so a shutdown with more sessions than the budget still releases all of
//     them instead of leaving them to the peer's grace timer.
//   - All cleanup requests derive from the manager's cleanup context, which
//     CloseContext cancels at its deadline.
func (s *ingressSession) startRemoteCleanup(reason error, mode string) {
	s.mu.Lock()
	sid := s.sid
	normal := s.upEOF && s.upFINApplied && s.downFIN && s.downFINAcked || s.ownerClosedNormal
	s.mu.Unlock()
	if sid == "" || s.m.cfg.Resources == nil {
		return
	}
	if normal && reasonCategory(reason) == "normal" {
		observability.RecordEvent("ingress", "cleanup", "not_needed", mode, "none")
		return
	}
	budget := s.m.cfg.Resources.Cleanup
	shutdown := s.m.shutdownContext()
	if shutdown == nil && !budget.Try(1) {
		s.cleanupSkipped(mode, "cleanup budget exhausted")
		return
	}
	// Tracked so Close waits for the remote DELETE before closing clients.
	// Not m.wg: a session timer can stop a session after CloseContext began
	// waiting, and WaitGroup.Add from zero concurrent with Wait is a race.
	s.m.cleanups.add()
	go func() {
		defer s.m.cleanups.done()
		base, cancel := context.WithCancel(s.m.cleanupCtx)
		defer cancel()
		if shutdown != nil {
			stop := context.AfterFunc(shutdown, cancel)
			defer stop()
			if err := budget.Take(base, 1); err != nil {
				s.cleanupSkipped(mode, "shutdown deadline reached")
				return
			}
		}
		defer budget.Put(1)
		started := time.Now()
		defer func() { observability.ObserveDuration("ingress", "cleanup", time.Since(started)) }()
		ctx, cancelTimeout := context.WithTimeout(base, s.m.cfg.Transport.ControlRequestTimeout)
		defer cancelTimeout()
		switch s.remoteCleanup(ctx) {
		case cleanupConfirmed:
			observability.RecordEvent("ingress", "cleanup", "success", mode, "none")
		case cleanupUnknown:
			observability.RecordEvent("ingress", "cleanup", "unknown", mode, "none")
			atomic.AddInt64(&observability.DefaultMetrics.RemoteCleanupUnknownTotal, 1)
		default:
			observability.RecordEvent("ingress", "cleanup", "failed", mode, "none")
			atomic.AddInt64(&observability.DefaultMetrics.RemoteCleanupFailedTotal, 1)
		}
	}()
}

func (s *ingressSession) cleanupSkipped(mode, why string) {
	observability.RecordEvent("ingress", "cleanup", "skipped", mode, "none")
	atomic.AddInt64(&observability.DefaultMetrics.RemoteCleanupFailedTotal, 1)
	slog.Warn("session cleanup skipped", "session", shortID(s.sid), "peer", s.p.ID, "reason", why)
}

// remoteCleanup DELETEs the remote session. The client the session is bound
// to (or was created through) is used directly: the DELETE carries owner and
// epoch, and egress verifies its instance, so no probe is needed.
// With failover enabled the other endpoints are asked as well, each after a
// probe confirmed it reaches the owner. A session that was recovering is
// bound to the carrier that just failed, so its backups are asked first.
//
// The probes run inside ctx (the cleanup context), not as shared manager
// probes: cleanup also runs during shutdown, after the manager context was
// cancelled, and it is tracked by the cleanup tracker, not the manager's
// WaitGroup.
func (s *ingressSession) remoteCleanup(ctx context.Context) cleanupResult {
	s.mu.Lock()
	epoch, owner, unknown := s.epoch, s.owner, s.outcomeUnknown
	recovering := !s.deadline.IsZero()
	var primary *transport.TripleTransportClient
	if s.g != nil {
		primary = s.g.c
	} else if s.createClient != nil {
		primary = s.createClient
	}
	s.mu.Unlock()
	failover := s.p.Failover.IsEnabled(len(s.p.Endpoints))
	var others []*transport.TripleTransportClient
	for _, endpoint := range s.m.linkManagers[s.p.ID].GetEndpoints() {
		c := s.m.clients[s.p.ID].WithURL(endpoint.URL)
		if primary == nil && !failover {
			primary = c
			break
		}
		if failover && (primary == nil || endpoint.URL != primary.URL()) {
			others = append(others, c)
		}
	}
	type candidate struct {
		c      *transport.TripleTransportClient
		backup bool
	}
	order := make([]candidate, 0, len(others)+1)
	if primary != nil && !recovering {
		order = append(order, candidate{primary, false})
	}
	for _, c := range others {
		order = append(order, candidate{c, true})
	}
	if primary != nil && recovering {
		order = append(order, candidate{primary, false})
	}
	for i, cand := range order {
		attemptCtx, attemptCancel := shareDeadline(ctx, len(order)-i)
		if cand.backup {
			pr, err := s.m.probeDirect(attemptCtx, s.p.ID, cand.c, s.p.ExpectedNodeID)
			if err != nil || pr.InstanceID != owner {
				attemptCancel()
				continue
			}
		}
		r := s.deleteVia(attemptCtx, cand.c, 1, epoch, unknown)
		attemptCancel()
		if r != cleanupFailed {
			s.logCleanup(r, cand.backup)
			return r
		}
	}
	return cleanupFailed
}

// shareDeadline returns a context with 1/n of ctx's remaining time.
func shareDeadline(ctx context.Context, n int) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok || n <= 1 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, time.Until(deadline)/time.Duration(n))
}

// probeDirect asks one endpoint for its identity within ctx. Unlike probe it
// is not shared and not bound to the manager context; it still takes the
// probe budget and the peer's probe slot, waiting for them within ctx.
func (m *IngressManager) probeDirect(ctx context.Context, peer string, c *transport.TripleTransportClient, node string) (*protocol.ProbeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, m.probeTimeout(peer))
	defer cancel()
	release, err := m.acquireProbeSlot(ctx, peer)
	if err != nil {
		return nil, err
	}
	defer release()
	return c.Probe(ctx, node, generateID(16))
}

// deleteVia sends one DELETE with a share of the remaining time, using only
// the last fully verified epoch (10.3.3). A trusted STALE_EPOCH means egress
// may have advanced while the last resume's outcome was lost: the result is
// cleanupUnknown and the task ends there. No newer epoch is guessed, at most
// one DELETE is sent per candidate, and egress's original recovery deadline
// reclaims the session. unknown is kept for callers; it no
// longer changes the request.
func (s *ingressSession) deleteVia(ctx context.Context, c *transport.TripleTransportClient, share int, epoch uint64, _ bool) cleanupResult {
	if deadline, ok := ctx.Deadline(); ok && share > 1 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Until(deadline)/time.Duration(share))
		defer cancel()
	}
	err := c.DeleteSession(ctx, s.sid, s.token, s.owner, epoch)
	if err == nil {
		return cleanupConfirmed
	}
	var remote *transport.RemoteError
	if errors.As(err, &remote) && remote.Trusted && remote.Code == protocol.ErrStaleEpoch {
		return cleanupUnknown
	}
	return cleanupFailed
}

func (s *ingressSession) logCleanup(r cleanupResult, viaBackup bool) {
	result := "confirmed"
	if r == cleanupUnknown {
		result = "unknown"
	}
	slog.Info("session cleanup", "session", shortID(s.sid), "peer", s.p.ID, "result", result, "via_backup", viaBackup)
}

// cleanupTracker counts running remote cleanups. Unlike sync.WaitGroup it
// allows a new cleanup to start while someone waits for idle.
type cleanupTracker struct {
	mu     sync.Mutex
	n      int
	idleCh chan struct{}
}

func (c *cleanupTracker) add() {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
}

func (c *cleanupTracker) done() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n--; c.n == 0 && c.idleCh != nil {
		close(c.idleCh)
		c.idleCh = nil
	}
}

// idle returns a channel closed once no cleanup is running.
func (c *cleanupTracker) idle() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == 0 {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	if c.idleCh == nil {
		c.idleCh = make(chan struct{})
	}
	return c.idleCh
}
