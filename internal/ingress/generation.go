package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"dbbridge/internal/link"
	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
	"github.com/gorilla/websocket"
)

type generation struct {
	faultOnce              sync.Once
	lastActivity           time.Time
	ctx                    context.Context
	cancel                 context.CancelFunc
	mu                     sync.Mutex
	ws                     *transport.WSSessionConn
	sub                    *transport.SSESubscriber
	c                      *transport.TripleTransportClient
	ep                     uint64
	mode, stream, resumeID string
	wg                     sync.WaitGroup
	sent                   map[*retainedChunk]bool
	pending                []wireMessage
	active                 bool
	endpoint               *link.Endpoint
	// readyAckPending: the initial attach went active on an implicit-ready
	// egress before its ready ACK was sent; the control writer's first ACK
	// carries it (protocol.HeaderImplicitReady).
	readyAckPending atomic.Bool
	// Protected by ingressSession.mu; only complete WS writes start this clock.
	upSentOffset   uint64
	upPendingSince time.Time
}

func (g *generation) close() {
	g.cancel()
	g.mu.Lock()
	ws, sub := g.ws, g.sub
	g.mu.Unlock()
	if ws != nil {
		ws.Close()
	}
	if sub != nil {
		sub.Close()
	}
}

type wireMessage struct {
	release func()
	kind    string
	data    []byte
	offset  uint64
	ctrl    protocol.WSTextControlMessage
	ready   *protocol.ReadyEventData
}

func (s *ingressSession) attach(parent context.Context, c *transport.TripleTransportClient, ep *link.Endpoint, epoch uint64, mode, resumeID string) (result *generation, err error) {
	start := time.Now()
	defer func() { s.m.observePhase(s.p.ID, c.URL(), "attach", start, err == nil) }()
	ctx, cancel := context.WithCancel(s.ctx)
	g := &generation{ctx: ctx, cancel: cancel, c: c, ep: epoch, mode: mode, resumeID: resumeID, sent: map[*retainedChunk]bool{}, endpoint: ep, lastActivity: time.Now()}
	s.mu.Lock()
	s.all = append(s.all, g)
	prev := s.g
	s.g = g
	s.mu.Unlock()
	deadline := time.Now().Add(s.m.cfg.Transport.ControlRequestTimeout)
	if d, ok := parent.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	timer := time.AfterFunc(time.Until(deadline), func() { g.close() })
	defer timer.Stop()
	stopParent := context.AfterFunc(parent, g.close)
	defer stopParent()
	success := false
	defer func() {
		if !success {
			for _, p := range g.pending {
				if p.release != nil {
					p.release()
				}
			}
			g.pending = nil
			g.close()
			g.c.UnbindStream(s.sid, g.ep)
			s.mu.Lock()
			for i, candidate := range s.all {
				if candidate == g {
					s.all = append(s.all[:i], s.all[i+1:]...)
					break
				}
			}
			// A failed attach must not stay the current generation.
			if s.g == g {
				s.g = prev
			}
			s.mu.Unlock()
		}
	}()
	var ready *protocol.ReadyEventData
	var e error
	if mode == "ws" {
		var ws *transport.WSSessionConn
		ws, ready, e = c.DialWSSession(ctx, s.sid, s.token, epoch, s.owner)
		g.mu.Lock()
		g.ws = ws
		g.mu.Unlock()
	} else {
		var sub *transport.SSESubscriber
		sub, e = c.SubscribeEvents(ctx, s.sid, s.token, s.owner, epoch, s.limits.MaxFrameSize)
		g.mu.Lock()
		g.sub = sub
		g.mu.Unlock()
		if e == nil {
			var msg wireMessage
			msg, e = s.readWire(g, deadline)
			ready = msg.ready
		}
	}
	if e == nil && ctx.Err() != nil {
		e = ctx.Err()
	}
	if e != nil {
		g.close()
		return nil, e
	}
	if ready == nil || ready.Type != "ready" || ready.ProtocolVersion != "1.4" || ready.SessionID != s.sid || ready.InstanceID != s.owner || ready.Epoch != strconv.FormatUint(epoch, 10) || ready.Transport != mode || ready.StreamID == "" || resumeID == "" && ready.ResumeRequestID != nil || resumeID != "" && (ready.ResumeRequestID == nil || *ready.ResumeRequestID != resumeID) {
		g.close()
		return nil, transport.ErrProtocol
	}
	g.stream = ready.StreamID
	c.BindStream(s.sid, epoch, g.stream, mode)
	nonce := ""
	implicit := false
	if resumeID == "" {
		g.mu.Lock()
		implicit = g.ws != nil && g.ws.ImplicitReady() || g.sub != nil && g.sub.ImplicitReady()
		g.mu.Unlock()
	}
	switch {
	case implicit:
		// The egress activates the initial attach on our first upstream
		// frame or on the ready ACK, whichever arrives first, so data may
		// flow now: the control writer sends the ready ACK at once.
		g.readyAckPending.Store(true)
	case resumeID == "":
		// An initial attach is active once the ready ACK is accepted. The
		// first ping's nonce only completes a recovery, so the control
		// writer echoes it later instead of the attach waiting for it.
		if e = s.ack(g, true, "", deadline); e != nil {
			g.close()
			return nil, e
		}
	default:
		if e = s.ack(g, true, "", deadline); e != nil {
			g.close()
			return nil, e
		}
	}
	for i := 0; resumeID != "" && i < 16 && nonce == ""; i++ {
		msg, e := s.readWire(g, deadline)
		if e != nil {
			g.close()
			return nil, e
		}
		if msg.kind == "ping" {
			nonce = msg.ctrl.Nonce
		} else {
			g.pending = append(g.pending, msg)
		}
	}
	if resumeID != "" {
		// A resumed epoch completes recovery with the first ping's nonce.
		if nonce == "" {
			g.close()
			return nil, transport.ErrProtocol
		}
		if e = s.ack(g, false, nonce, deadline); e != nil {
			g.close()
			return nil, e
		}
	}
	s.mu.Lock()
	if s.closed.Load() || ctx.Err() != nil || !time.Now().Before(deadline) || !s.deadline.IsZero() && !time.Now().Before(s.deadline) {
		s.mu.Unlock()
		g.close()
		return nil, context.DeadlineExceeded
	}
	g.active = true
	s.creationDone = true
	s.epoch = epoch
	s.mode = mode
	s.deadline = time.Time{}
	if s.deadlineTimer != nil {
		s.deadlineTimer.Stop()
		s.deadlineTimer = nil
	}
	s.heartbeatNonce = nonce
	s.lastControl = time.Now()
	s.rxBlockedAt = time.Time{}
	s.rxBlockReason = ""
	s.remoteTxBlocked = false
	s.remoteRxBlocked = false
	s.remoteBlockedAt = time.Time{}
	s.recoveryLease.End()
	s.wake()
	s.mu.Unlock()
	success = true
	return g, nil
}
func (s *ingressSession) readWire(g *generation, deadline time.Time) (message wireMessage, err error) {
	defer func() {
		if err == nil {
			g.mu.Lock()
			g.lastActivity = time.Now()
			g.mu.Unlock()
		}
	}()
	var kind, data, id string
	var payload []byte
	blocked := func(waiting bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.g != g || s.closed.Load() {
			return
		}
		if waiting {
			if s.rxBlockedAt.IsZero() {
				s.rxBlockedAt = time.Now()
			}
			s.rxBlockReason = "global_buffer"
		} else {
			s.rxBlockedAt = time.Time{}
			s.rxBlockReason = ""
		}
	}
	if g.mode == "ws" {
		ctx := g.ctx
		cancel := func() {}
		if !deadline.IsZero() {
			ctx, cancel = context.WithDeadline(ctx, deadline)
		}
		defer cancel()
		t, b, release, _, e := transport.ReadWSMessageWithPayload(ctx, g.ws.Conn(), s.m.cfg.Resources.Codec, nil, s.limits.MaxFrameSize, s.m.cfg.Transport.BackpressureTimeout, func() { g.mu.Lock(); g.lastActivity = time.Now(); g.mu.Unlock() }, blocked)
		if e != nil {
			return wireMessage{}, e
		}
		if t == websocket.BinaryMessage {
			ep, off, p, e := protocol.DecodeWSFrame(b)
			if e != nil || ep != g.ep || len(p) > s.limits.MaxFrameSize {
				release()
				return wireMessage{}, transport.ErrProtocol
			}
			return wireMessage{kind: "data", offset: off, data: p, release: release}, nil
		}
		if t != websocket.TextMessage {
			return wireMessage{}, transport.ErrProtocol
		}
		payload = b
		defer release()
	} else {
		ev, e := g.sub.NextEventWithProgress(g.ctx, s.m.cfg.Transport.BackpressureTimeout, func() { g.mu.Lock(); g.lastActivity = time.Now(); g.mu.Unlock() }, blocked)
		if e != nil {
			return wireMessage{}, e
		}
		kind, data, id = ev.Event, ev.Data, ev.ID
		if kind != "data" {
			defer ev.Release()
		}
		if kind == "data" {
			off, p, e := protocol.ParseSSEData(data)
			if e != nil || len(p) > s.limits.MaxFrameSize || id != fmt.Sprintf("%d:%d", g.ep, off+uint64(len(p))) {
				ev.Release()
				return wireMessage{}, transport.ErrProtocol
			}
			// Only the decoded payload stays resident from here on: drop
			// the text and lower the codec charge to it. The
			// release closure keeps ev alive, so Data must be cleared.
			ev.Data = ""
			ev.Shrink(int64(cap(p)))
			return wireMessage{kind: "data", offset: off, data: p, release: ev.Release}, nil
		}
		payload = []byte(data)
	}
	var hdr struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &hdr); err != nil {
		return wireMessage{}, transport.ErrProtocol
	}
	if kind == "ready" || g.mode == "ws" && hdr.Type == "ready" {
		var r protocol.ReadyEventData
		if protocol.DecodeJSON(bytes.NewReader(payload), 16<<10, &r) != nil {
			return wireMessage{}, transport.ErrProtocol
		}
		return wireMessage{kind: "ready", ready: &r}, nil
	}
	var ctrl protocol.WSTextControlMessage
	if protocol.DecodeJSON(bytes.NewReader(payload), 16<<10, &ctrl) != nil {
		return wireMessage{}, transport.ErrProtocol
	}
	if kind != "" && kind != ctrl.Type || ctrl.SessionID != s.sid || ctrl.InstanceID != s.owner || ctrl.Epoch != strconv.FormatUint(g.ep, 10) || ctrl.StreamID != g.stream {
		return wireMessage{}, transport.ErrProtocol
	}
	switch ctrl.Type {
	case "ping", "ack_up", "fin", "error":
	default:
		return wireMessage{}, transport.ErrProtocol
	}
	return wireMessage{kind: ctrl.Type, ctrl: ctrl}, nil
}
func (s *ingressSession) startGeneration(g *generation) {
	for _, fn := range []func(*generation){s.receive, s.send, s.control, s.watchActivity} {
		g.wg.Add(1)
		go func(f func(*generation)) { defer g.wg.Done(); f(g) }(fn)
	}
}
func (s *ingressSession) watchActivity(g *generation) {
	detect := s.m.cfg.Transport.FailureDetectTimeout
	ticker := time.NewTicker(max(time.Nanosecond, min(detect/4, 250*time.Millisecond)))
	defer ticker.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.mu.Lock()
			last := g.lastActivity
			g.mu.Unlock()
			s.mu.Lock()
			blocked := !s.rxBlockedAt.IsZero() || s.remoteTxBlocked
			blockedAt := s.rxBlockedAt
			if blockedAt.IsZero() || !s.remoteBlockedAt.IsZero() && s.remoteBlockedAt.Before(blockedAt) {
				blockedAt = s.remoteBlockedAt
			}
			controlRecent := !s.lastControl.IsZero() && time.Since(s.lastControl) < detect
			upPending := s.g == g && g.active && g.upSentOffset > s.upAck && !g.upPendingSince.IsZero()
			upSince, remoteRxBlocked := g.upPendingSince, s.remoteRxBlocked
			s.mu.Unlock()
			if upPending {
				if remoteRxBlocked && controlRecent {
					stall := s.m.cfg.Transport.BackpressureTimeout
					if stall <= 0 {
						stall = 120 * time.Second
					}
					if time.Since(upSince) >= stall {
						s.fault(g, observability.FaultUpstream, errors.New("upstream backpressure timeout"), true)
						return
					}
				} else if time.Since(upSince) >= detect {
					s.fault(g, observability.FaultUpstream, fmt.Errorf("upstream confirmation timeout: %w", context.DeadlineExceeded), false)
					return
				}
			}
			if blocked && controlRecent {
				stall := s.m.cfg.Transport.BackpressureTimeout
				if stall <= 0 {
					stall = 120 * time.Second
				}
				if last.After(blockedAt) {
					blockedAt = last
				}
				if !blockedAt.IsZero() && time.Since(blockedAt) >= stall {
					s.fault(g, observability.FaultData, errors.New("backpressure timeout"), true)
					return
				}
				continue
			}
			if time.Since(last) >= detect {
				s.fault(g, observability.FaultLiveness, fmt.Errorf("no session traffic: %w", context.DeadlineExceeded), false)
				return
			}
		}
	}
}
