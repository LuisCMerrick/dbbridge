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
		resp, err = c.CreateSession(ctx, s.l.Target, lim, crID, rec, s.owner, mode)
		if err == nil {
			break
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
