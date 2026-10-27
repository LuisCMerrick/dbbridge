package ingress

import (
	"context"
	"errors"
	"fmt"
	"time"

	"dbbridge/internal/link"
	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
)

// create selects an endpoint and creates the remote session, retrying until
// ctx (the create retry deadline) ends. On failure the session is stopped
// and ok is false.
func (s *ingressSession) create(ctx context.Context, strategy string) (c *transport.TripleTransportClient, ep *link.Endpoint, mode string, ok bool) {
	m := s.m
	var err error
	crID := generateID(16)
	lim := protocol.SessionLimits{MaxFrameSize: m.cfg.Transport.MaxFrameSize, UpMaxInflightFrames: m.cfg.Transport.UpMaxInflightFrames, UpWindowBytes: m.cfg.Transport.UpWindowBytes, DownBufferBytes: m.cfg.Transport.BufferPerDirection}
	var resp *protocol.CreateSessionResponse
	var requested time.Time
	fellBack := false
	attempted := map[string]bool{}
	for attempt := 0; ctx.Err() == nil; attempt++ {
		policy := strategy
		if s.owner != "" {
			policy = mode
		}
		ownerFromSel := s.owner == ""
		sel, chooseErr := m.chooseUntried(ctx, s.p, s.owner, policy, ownerFromSel, true, attempted)
		err = chooseErr
		if err == nil {
			c, ep = sel.c, sel.ep
			if s.owner == "" {
				mode = sel.mode
				fellBack = sel.fellBack
			}
		}
		if err != nil {
			if fatalError(err) {
				s.stop(err)
				return nil, nil, "", false
			}
			clear(attempted)
			if pause(ctx, attempt, err) != nil {
				break
			}
			continue
		}
		// Reuse the probe the selection (and its WS precheck) was based on, so
		// ownership and the transport decision come from the same answer.
		if s.owner == "" {
			s.mu.Lock()
			s.owner = sel.pr.InstanceID
			s.mu.Unlock()
		}
		rec := protocol.RecoveryNegotiation{Enabled: s.recovery}
		if s.recovery {
			rec.GraceMs = s.grace.Milliseconds()
		}
		requested = time.Now()
		var guardFailed bool
		if sel.cachedProbe {
			resp, err, guardFailed = m.guardedCreate(ctx, s, c, lim, crID, rec, mode)
		} else {
			resp, err = c.CreateSession(ctx, s.l.Target, lim, crID, rec, s.owner, mode)
		}
		if err == nil {
			break
		}
		if guardFailed {
			// The live probe of this endpoint failed while the create was
			// still waiting: the path is down (e.g. a black-holed h2
			// connection). Go to the next candidate now instead of waiting
			// out control_request_timeout; the create, if it arrived at all,
			// is reaped by the owner's unattached timeout.
			// pathCheck already dropped the cached answer, closed the mute
			// connections and, with another endpoint configured, cooled
			// this address down.
			attempted[ep.ID] = true
			if ownerFromSel {
				s.mu.Lock()
				s.owner = ""
				s.mu.Unlock()
			}
			continue
		}
		var remote *transport.RemoteError
		if sel.cachedProbe {
			// The selection reused a health-loop answer: never build a
			// second attempt on it.
			m.invalidateProbe(s.p.ID, c, s.p.ExpectedNodeID)
			if ownerFromSel && errors.As(err, &remote) && remote.Trusted && remote.Code == protocol.ErrInstanceMismatch {
				// The endpoint restarted (or failed over) since that answer:
				// the create was refused before anything was created, so
				// choose again from a real probe, with no owner yet.
				s.mu.Lock()
				s.owner = ""
				s.mu.Unlock()
				continue
			}
		}
		if fatalError(err) || (errors.As(err, &remote) && remote.Trusted && !safeRetry(err)) {
			s.stop(err)
			return nil, nil, "", false
		}
		if !safeRetry(err) {
			attempted[ep.ID] = true
		}
		if pause(ctx, attempt, err) != nil {
			break
		}
	}
	if err != nil || resp == nil {
		s.stop(fmt.Errorf("create failed: %w", err))
		return nil, nil, "", false
	}
	s.mu.Lock()
	s.sid, s.token, s.epoch, s.mode, s.limits = resp.SessionID, resp.SessionToken, 1, mode, resp.Limits
	s.createClient = c
	if resp.UnattachedTimeoutMs > 0 {
		// The owner reaps a session that is not attached within this time,
		// counted from its creation; the request start is a safe lower bound.
		s.attachBy = requested.Add(time.Duration(resp.UnattachedTimeoutMs) * time.Millisecond)
	}
	if s.recovery {
		s.grace = time.Duration(resp.Recovery.GraceMs) * time.Millisecond
	}
	closed := s.closed.Load()
	s.mu.Unlock()
	if fellBack {
		// Counted once per created session, only when auto actually fell
		// back from a failed WS precheck to SSE.
		observability.RecordEvent("ingress", "fallback", "transport", "ws", "http-sse")
	}
	if s.recovery {
		m.warnGraceBelowCooldown(s.p, strategy, s.grace)
	}
	if closed {
		// Stopped while the create was in flight: the termination ran without
		// a session ID, so release the remote session now.
		s.startRemoteCleanup(context.Canceled, mode)
		return nil, nil, "", false
	}
	return c, ep, mode, true
}

// guardedCreate runs a create whose selection reused a health-loop probe
// answer. When the create has not answered after guardDelay, the skipped
// probe is run alongside it (singleflight, bounded by guardTimeout, see
// pathCheck): it costs no latency when the path is fine,
// and when the path died since the cached answer (the connection the create
// rides is black-holed, so the create would only fail after
// control_request_timeout) the failed check cancels the create and
// guardFailed reports it.
func (m *IngressManager) guardedCreate(ctx context.Context, s *ingressSession, c *transport.TripleTransportClient, lim protocol.SessionLimits, crID string, rec protocol.RecoveryNegotiation, mode string) (resp *protocol.CreateSessionResponse, err error, guardFailed bool) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		resp *protocol.CreateSessionResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		r, e := c.CreateSession(cctx, s.l.Target, lim, crID, rec, s.owner, mode)
		done <- result{r, e}
	}()
	guard := make(chan error, 1)
	// A create normally answers within about one probe time; the check
	// starts only when it has not, so a healthy path costs no extra probe
	// (and over HTTP/1.1 no extra connection racing the create for the idle
	// one).
	wait := time.NewTimer(m.guardDelay(s.p.ID, c, s.p.ExpectedNodeID))
	defer wait.Stop()
	go func() {
		select {
		case <-wait.C:
		case <-cctx.Done():
			guard <- nil
			return
		}
		guard <- m.pathCheck(cctx, s.p.ID, c, s.p.ExpectedNodeID)
	}()
	for {
		select {
		case r := <-done:
			if r.err != nil && guard != nil {
				// A failed create may be the guard closing the mute
				// connection it rode: let the guard's verdict name it.
				if e := <-guard; e != nil {
					return nil, fmt.Errorf("create abandoned: endpoint path not answering: %w", e), true
				}
			}
			return r.resp, r.err, false
		case e := <-guard:
			guard = nil
			if e == nil {
				continue
			}
			select {
			case r := <-done:
				// The create answered in the meantime; its answer decides.
				if r.err == nil {
					return r.resp, nil, false
				}
			default:
			}
			cancel()
			r := <-done
			if r.err == nil {
				// Lost the race to a successful answer: keep it.
				return r.resp, nil, false
			}
			return nil, fmt.Errorf("create abandoned: endpoint path not answering: %w", e), true
		}
	}
}
