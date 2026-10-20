package ingress

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"dbbridge/internal/transport"
)

func (s *ingressSession) recover(cause error, strategy string) (success bool) {
	started := time.Now()
	// The whole recovery is timed here; each resume attempt is also timed
	// against the endpoint it was sent to.
	defer func() { observability.ObserveDuration("ingress", "recovery", time.Since(started)) }()
	s.mu.Lock()
	old := s.g
	if old != nil {
		old.active = false
	}
	if s.deadline.IsZero() {
		// The recovery window and the retention lease start at the fault
		// decision, not here: a final ACK may have run in between (3.5(1)).
		if s.faultTime.IsZero() {
			s.faultTime = started
		}
		base := s.faultTime
		s.deadline = base.Add(s.grace)
		var lease *resource.RetentionLease
		lease = resource.NewRetentionLease(base, s.retainedBytes.Load, func() { s.stopRecovery(lease, errors.New("recovery buffer quota")) })
		s.recoveryLease = lease
		s.deadlineTimer = time.AfterFunc(time.Until(s.deadline), func() { s.stopRecovery(lease, context.DeadlineExceeded) })
	}
	deadline := s.deadline
	lease := s.recoveryLease
	s.wake()
	s.mu.Unlock()
	s.m.cfg.Resources.Suspended.Register(lease)
	if s.closed.Load() {
		return false
	}
	if old != nil {
		// Cancel the old generation now (its goroutines leave at once) but
		// finish closing the carrier in the background: a TLS close can block
		// for seconds and must not delay the barrier or the resume.
		old.cancel()
		go old.close()
	}
	ctx, cancel := context.WithDeadline(s.ctx, deadline)
	defer func() { cancel() }()
	barrierStart := time.Now()
	barrierObserved := false
	defer func() {
		if !barrierObserved {
			observability.ObserveDuration("ingress", "barrier", time.Since(barrierStart))
		}
	}()
	barrier := min(s.m.cfg.Transport.Recovery.BarrierTimeout, time.Until(deadline))
	bt := time.NewTimer(barrier)
	defer bt.Stop()
	select {
	case <-ctx.Done():
		s.stop(ctx.Err())
		return false
	case <-bt.C:
		s.stop(errors.New("local write barrier timeout"))
		return false
	case <-s.gate:
		s.gate <- struct{}{}
	}
	if old != nil {
		done := make(chan struct{})
		go func() { old.wg.Wait(); close(done) }()
		// The barrier (gate plus old-generation drain) is bounded by barrier_timeout.
		select {
		case <-done:
		case <-ctx.Done():
			s.stop(ctx.Err())
			return false
		case <-bt.C:
			s.stop(errors.New("local write barrier timeout"))
			return false
		}
		s.mu.Lock()
		for i, g := range s.all {
			if g == old {
				s.all = append(s.all[:i], s.all[i+1:]...)
				break
			}
		}
		old.sent = nil
		s.mu.Unlock()
		old.c.UnbindStream(s.sid, old.ep)
	}
	observability.ObserveDuration("ingress", "barrier", time.Since(barrierStart))
	barrierObserved = true
	if err := s.m.cfg.Resources.Recovery.Take(ctx, 1); err != nil {
		s.stop(err)
		return false
	}
	defer s.m.cfg.Resources.Recovery.Put(1)
	if old != nil && s.carrierFailure(nil, cause) {
		// Record the fault before the first candidate selection: the carrier
		// that just failed cools down for this fault cycle, so the selector
		// prefers the same address's other carrier or another address and
		// never returns straight to it (requirements 3.2(4), 3.8).
		s.m.linkManagers[s.p.ID].RecordTransportDrop(old.endpoint.ID, old.mode)
		if old.mode == "ws" {
			s.m.invalidatePrecheck(s.p.ID, old.c.URL(), s.owner)
		}
	}
	var req *protocol.ResumeRequest
	attempted := map[string]bool{}
	for attempt := 0; ctx.Err() == nil; attempt++ {
		policy := strategy
		if req != nil {
			policy = req.Transport.Mode
		}
		sel, e := s.m.chooseUntried(ctx, s.p, s.owner, policy, false, attempted)
		c, ep, mode := sel.c, sel.ep, sel.mode
		if e != nil {
			if fatalError(e) {
				s.stop(e)
				return false
			}
			clear(attempted)
			if pause(ctx, attempt, e) != nil {
				break
			}
			continue
		}
		if req == nil {
			s.mu.Lock()
			q := protocol.ResumeRequest{ResumeRequestID: generateID(16), ExpectedEpoch: strconv.FormatUint(s.epoch, 10), IngressInstanceID: s.m.cfg.Node.InstanceID, Transport: protocol.TransportMode{Mode: mode}, KnownUpAckOffset: strconv.FormatUint(s.upAck, 10), UpNextOffset: strconv.FormatUint(s.upNext, 10), DownDeliveredOffset: strconv.FormatUint(s.downStable, 10), UpFIN: protocol.DirectionFIN{Seen: s.upEOF, Applied: s.upFINApplied}, DownFIN: protocol.DirectionFIN{Seen: s.downFIN, Applied: s.downFIN}}
			if s.upEOF {
				v := strconv.FormatUint(s.upNext, 10)
				q.UpFIN.FinalOffset = &v
			}
			if s.downFIN {
				v := strconv.FormatUint(s.downStable, 10)
				q.DownFIN.FinalOffset = &v
			}
			req = &q
			s.mu.Unlock()
		}
		if req.Transport.Mode != mode {
			mode = req.Transport.Mode
		}
		s.mu.Lock()
		s.recoveryAttempts++
		s.outcomeUnknown = true
		s.mu.Unlock()
		resumeStarted := time.Now()
		r, e := c.ResumeSession(ctx, s.sid, s.token, s.owner, *req)
		if e != nil {
			s.m.observeEndpointPhase(s.p.ID, c.URL(), "recovery", resumeStarted, false)
			// A trusted, deliberate refusal is final, as on the create path.
			if fatalError(e) || terminalRemote(e) {
				s.stopRefused(e)
				return false
			}
			if !safeRetry(e) {
				attempted[ep.ID] = true
			}
			if pathFailure(ctx, e) {
				// The HTTP control path of this address failed as a whole.
				s.m.linkManagers[s.p.ID].RecordPathFailure(ep.ID)
			}
			if pause(ctx, attempt, e) != nil {
				break
			}
			continue
		}
		s.mu.Lock()
		s.outcomeUnknown = false
		if s.closed.Load() {
			s.mu.Unlock()
			return false
		}
		// The cached peer snapshot cannot extend this fault cycle. Anchor the
		// hint before the request so response transit time is not added to it.
		peerDeadline := resumeStarted.Add(time.Duration(r.ResumeRemainingMs) * time.Millisecond)
		if peerDeadline.Before(s.deadline) {
			s.deadline = peerDeadline
			if s.deadlineTimer != nil {
				s.deadlineTimer.Stop()
			}
			s.deadlineTimer = time.AfterFunc(time.Until(peerDeadline), func() { s.stopRecovery(lease, context.DeadlineExceeded) })
		}
		deadline = s.deadline
		// transport.ResumeSession validated both; a parse failure here is a
		// protocol violation, never a silent zero.
		newEpoch, pe := protocol.Offset(r.Epoch)
		ack, ae := protocol.Offset(r.AckUpOffset)
		if e = errors.Join(pe, ae); e != nil {
			e = transport.ErrProtocol
		} else {
			e = s.advanceUpLocked(ack)
		}
		if e != nil {
			s.mu.Unlock()
			s.stop(e)
			return false
		}
		s.epoch = newEpoch
		s.mode = mode
		s.mu.Unlock()
		if !time.Now().Before(deadline) {
			s.stopRecovery(lease, context.DeadlineExceeded)
			return false
		}
		if current, ok := ctx.Deadline(); ok && deadline.Before(current) {
			next, nextCancel := context.WithDeadline(s.ctx, deadline)
			cancel()
			ctx, cancel = next, nextCancel
		}
		g, e := s.attach(ctx, c, ep, newEpoch, mode, req.ResumeRequestID)
		s.m.observeEndpointPhase(s.p.ID, c.URL(), "recovery", resumeStarted, e == nil)
		if e == nil {
			observability.RecordEvent("ingress", "resume", "success", oldMode(old), mode)
			slog.Info("session recovered", "session", shortID(s.sid), "role", "ingress", "peer", s.p.ID, "endpoint", ep.ID, "owner", shortID(s.owner), "epoch", newEpoch, "from", oldMode(old), "transport", mode, "resume_id", shortID(req.ResumeRequestID), "elapsed_ms", time.Since(started).Milliseconds())
			s.mu.Lock()
			s.faultTime = time.Time{}
			s.recoveryAttempts = 0
			s.mu.Unlock()
			s.startGeneration(g)
			return true
		}
		if fatalError(e) {
			s.stopRefused(e)
			return false
		}
		// A confirmed resume consumed this epoch's stream slot. Cool down the
		// failed carrier before selecting the next path under the same
		// deadline, unless the attach ended for a local reason.
		if s.carrierFailure(ctx, e) {
			s.coolCarrier(ep, c, mode)
		}

		req = nil
		clear(attempted)
		if pause(ctx, attempt, e) != nil {
			break
		}
	}
	s.stop(fmt.Errorf("recovery exhausted: %w", cause))
	return false
}

// stopRefused terminates after the owner refused a resume or attach. A
// SESSION_CLOSED answer for a session whose both directions already
// finished locally is the normal-close race, not a failure.
func (s *ingressSession) stopRefused(e error) {
	var r *transport.RemoteError
	if errors.As(e, &r) && r.Trusted && r.Code == protocol.ErrSessionClosed {
		s.mu.Lock()
		// The owner also says so explicitly when both FINs completed on its
		// side. That covers a fault that lost the ack telling us the
		// upstream FIN was applied.
		done := s.upEOF && s.upFINApplied && s.downFIN || r.Message == protocol.SessionClosedNormalMessage
		s.ownerClosedNormal = done
		s.mu.Unlock()
		if done {
			s.stop(errClosedNormal)
			return
		}
	}
	s.stop(e)
}

func oldMode(g *generation) string {
	if g == nil {
		return "none"
	}
	return g.mode
}

// pathFailure reports a transport-level failure of a real request: no answer
// from the peer at all, or an unverified gateway 502/503/504, and not our own
// cancellation or local contention. It cools the whole address down (3.2(4)).
func pathFailure(ctx context.Context, e error) bool {
	if e == nil || localOrCanceled(ctx, e) || localResourceError(e) {
		return false
	}
	var r *transport.RemoteError
	if errors.As(e, &r) {
		return !r.Trusted && r.Status >= 502 && r.Status <= 504
	}
	return true
}
