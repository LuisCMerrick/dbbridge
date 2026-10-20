package ingress

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/link"
	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"dbbridge/internal/transport"
)

type sessionFault struct {
	g     *generation
	err   error
	fatal bool
}
type ingressSession struct {
	m                                          *IngressManager
	p                                          config.PeerConfig
	l                                          config.ListenerConfig
	conn                                       net.Conn
	ctx                                        context.Context
	cancel                                     context.CancelFunc
	mu                                         sync.Mutex
	changed                                    chan struct{}
	faults                                     chan sessionFault
	closed                                     atomic.Bool
	closeOnce                                  sync.Once
	gate                                       chan struct{}
	g                                          *generation
	all                                        []*generation
	frames                                     []*retainedChunk
	used                                       int64
	upNext, upAck, downActual, downStable      uint64
	upEOF, upFINApplied, downFIN, downFINAcked bool
	heartbeatNonce                             string
	sid, token, owner                          string
	epoch                                      uint64
	mode                                       string
	limits                                     protocol.SessionLimits
	recovery                                   bool
	grace                                      time.Duration
	deadline                                   time.Time
	deadlineTimer                              *time.Timer
	readerWG                                   sync.WaitGroup
	creationDone                               bool
	rxBlockedAt                                time.Time
	rxBlockReason                              string
	lastControl                                time.Time
	remoteTxBlocked                            bool
	remoteRxBlocked                            bool
	remoteBlockedAt                            time.Time
	recoveryLease                              *resource.RetentionLease
	retainedBytes                              atomic.Int64
	ackNotify                                  chan struct{}
	ackProgress                                uint64
	faultTime                                  time.Time
	faultSource, faultReason                   string // first fault of the current fault cycle
	recoveryAttempts                           int
	outcomeUnknown                             bool
	// ownerClosedNormal is set when the owner answered a resume or attach
	// with a trusted 410 SESSION_CLOSED for a session that had finished:
	// the remote session is already gone, so no cleanup DELETE is sent.
	ownerClosedNormal bool
	// Frames delivered/acknowledged since the last control ACK.
	downFramesUnacked, upFramesUnconfirmed int
	// createClient is the endpoint client the session was created through,
	// used for remote cleanup before any generation exists.
	createClient *transport.TripleTransportClient
	// localCloseOnce/localClosedAt record the one full close of the client
	// socket; localClosing makes later faults (caused by that close) inert
	// while a recovery-disabled session settles its terminal reason.
	localCloseOnce sync.Once
	localClosedAt  time.Time
	localClosing   atomic.Bool
	// attachBy is when the owner reaps the session if it is still not
	// attached (creation plus its unattached_timeout), zero when unknown.
	attachBy time.Time
}

func (s *ingressSession) wake() { close(s.changed); s.changed = make(chan struct{}) }

// Only threshold/urgent events wake the control writer. The fixed timer provides
// the maximum delay for the first unconfirmed byte when the threshold is not met.
func (s *ingressSession) notifyAckLocked(urgent bool) {
	if !urgent || s.ackNotify == nil {
		return
	}
	select {
	case s.ackNotify <- struct{}{}:
	default:
	}
}
func (s *ingressSession) stop(reason error) { s.stopRecovery(nil, reason) }

// closeLocal fully closes the client socket once and returns when that
// close completed (T_local_closed).
func (s *ingressSession) closeLocal() time.Time {
	s.localCloseOnce.Do(func() {
		s.localClosing.Store(true)
		if s.conn != nil {
			_ = s.conn.Close()
		}
		s.localClosedAt = time.Now()
	})
	return s.localClosedAt
}
func (s *ingressSession) stopRecovery(lease *resource.RetentionLease, reason error) {
	s.mu.Lock()
	if s.closed.Load() || lease != nil && (s.recoveryLease != lease || !lease.Active()) {
		s.mu.Unlock()
		return
	}
	s.closed.Store(true)
	s.recoveryLease.End()
	s.mu.Unlock()
	s.closeOnce.Do(func() {
		terminal := time.Now()
		s.mu.Lock()
		faultTime := s.faultTime
		if faultTime.IsZero() && reasonCategory(reason) != "normal" {
			faultTime = terminal
		}
		recovering := !s.deadline.IsZero()
		if !s.deadline.IsZero() && terminal.After(s.deadline) {
			terminal = s.deadline
		}
		// Recovery disabled: T_terminal is the fault decision itself,
		// so a terminal query that ran after the early local close is not
		// hidden from local_close_ms.
		if !s.recovery && !recovering && !s.faultTime.IsZero() && s.faultTime.Before(terminal) {
			terminal = s.faultTime
		}
		s.mu.Unlock()
		s.closed.Store(true)
		localClosed := s.closeLocal()
		atomic.AddInt64(&observability.DefaultMetrics.ClientCloseCount, 1)
		countTermination(reason, s.recovery, recovering)
		atomic.AddInt64(&observability.DefaultMetrics.ClientCloseDurationSumMs, localClosed.Sub(terminal).Milliseconds())
		s.cancel()
		if s.m.cfg.Resources != nil {
			s.m.cfg.Resources.Suspended.Enforce()
		}
		s.mu.Lock()
		sid, owner := s.sid, s.owner
		epoch, mode, attempts, deadline, unknown := s.epoch, s.mode, s.recoveryAttempts, s.deadline, s.outcomeUnknown
		faultSource, faultReason := s.faultSource, s.faultReason
		endpoint := ""
		if s.g != nil && s.g.endpoint != nil {
			endpoint = s.g.endpoint.ID
		}
		if s.deadlineTimer != nil {
			s.deadlineTimer.Stop()
		}
		gens := append([]*generation(nil), s.all...)
		s.wake()
		s.mu.Unlock()
		for _, g := range gens {
			g.close()
		}
		observability.RecordEvent("ingress", "terminate", reasonCategory(reason), mode, "none")
		observability.ObserveDuration("ingress", "local_close", localClosed.Sub(terminal))
		slog.Info("session terminated", "session", shortID(sid), "role", "ingress", "peer", s.p.ID, "endpoint", endpoint, "owner", shortID(owner), "epoch", epoch, "transport", mode, "recovery_enabled", s.recovery, "attempts", attempts, "deadline", logTime(deadline), "outcome_unknown", unknown, "reason", reasonCategory(reason), "fault_source", faultSource, "fault_reason", faultReason, "local_close_ms", localClosed.Sub(terminal).Milliseconds(), "fault_time", faultTime, "terminal_time", terminal, "local_closed_time", localClosed)
		s.startRemoteCleanup(reason, mode)
	})
}

// finalAck handles the normal-close race: egress closes the
// carrier as soon as both FINs are done, which can reach the receive loop
// before the ACK response that reports down_fin_acked. When only that bit is
// missing, one more ACK on the same generation (the control channel does not
// depend on the closed carrier) settles it; egress answers 200 with
// down_fin_acked for a normally closed session. Without this the close was
// treated as a carrier failure and cooled the carrier down for new sessions.
func (s *ingressSession) finalAck(g *generation) (bool, error) {
	if !s.finAdjacent() {
		return false, nil
	}
	wait := min(s.m.cfg.Transport.ControlRequestTimeout, finalAckTimeout)
	if wait <= 0 {
		wait = finalAckTimeout
	}
	if e := s.ack(g, false, "", time.Now().Add(wait)); e != nil {
		return false, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upEOF && s.upFINApplied && s.downFIN && s.downFINAcked, nil
}

// finAdjacent reports the FIN-adjacent state: both FINs applied, only the
// confirmation of the downstream FIN is missing.
func (s *ingressSession) finAdjacent() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upEOF && s.upFINApplied && s.downFIN && !s.downFINAcked
}

// finalAckTimeout bounds the extra ACK of finalAck.
const finalAckTimeout = 2 * time.Second

// carrierFailure reports whether e, the failure of a real attach or of an
// established carrier, is a failure of the (endpoint, carrier) pair. Fatal
// protocol errors, local client socket errors, local resource limits, the
// session's own stop and the expiry or cancellation of parent (the local
// create or recovery deadline) say nothing about the carrier.
func (s *ingressSession) carrierFailure(parent context.Context, e error) bool {
	if e == nil || fatalError(e) || localResourceError(e) || errors.Is(e, errLocalSocket) {
		return false
	}
	if s.closed.Load() || s.ctx.Err() != nil {
		return false
	}
	if parent.Err() != nil {
		return false
	}
	if d, ok := parent.Deadline(); ok && !time.Now().Before(d) {
		return false
	}
	return true
}

// coolCarrier cools the (endpoint, carrier) pair down and drops a cached
// successful WS precheck of that address, so new selections avoid it.
func (s *ingressSession) coolCarrier(ep *link.Endpoint, c *transport.TripleTransportClient, mode string) {
	s.m.linkManagers[s.p.ID].RecordTransportFailure(ep.ID, mode)
	if mode == "ws" {
		s.m.invalidatePrecheck(s.p.ID, c.URL(), s.owner)
	}
}

// countTermination updates the termination counters. Normal closes
// and node shutdown are not failures. A failed session with recovery
// disabled is a direct termination; with recovery enabled it is a final
// failure termination, and a failed resume as well when it ended inside a
// recovery window.
func countTermination(reason error, recoveryEnabled, recovering bool) {
	switch reasonCategory(reason) {
	case "normal", "shutdown":
		return
	}
	if !recoveryEnabled {
		atomic.AddInt64(&observability.DefaultMetrics.TerminatedDirectTotal, 1)
		return
	}
	atomic.AddInt64(&observability.DefaultMetrics.TerminatedAfterResumeFailTotal, 1)
	if recovering {
		atomic.AddInt64(&observability.DefaultMetrics.ResumeFailed, 1)
	}
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// fault reports a fault detected at source on generation g (nil: the
// session itself). The first fault of a fault cycle is kept for the
// termination log; fatal faults end the session at once.
func (s *ingressSession) fault(g *generation, source string, e error, fatal bool) {
	if e == nil || s.closed.Load() || s.localClosing.Load() {
		return
	}
	s.mu.Lock()
	if g != nil && s.g != g {
		s.mu.Unlock()
		return
	}
	reason := reasonCategory(e)
	if s.faultTime.IsZero() {
		s.faultTime = time.Now()
	}
	if s.faultSource == "" {
		s.faultSource, s.faultReason = source, reason
	}
	mode := s.mode
	s.mu.Unlock()
	observability.RecordEvent("ingress", "fault", reason, mode, "none")
	observability.RecordFault(source, reason)
	if fatal || fatalError(e) {
		s.stop(e)
		return
	}
	send := func() {
		var done <-chan struct{}
		if g != nil {
			done = g.ctx.Done()
		}
		select {
		case s.faults <- sessionFault{g, e, false}:
		case <-done:
		case <-s.ctx.Done():
		}
	}
	if g != nil {
		g.faultOnce.Do(send)
	} else {
		send()
	}
}
func (m *IngressManager) handleConn(ctx context.Context, cancel context.CancelFunc, conn net.Conn, l config.ListenerConfig, id string, acceptedAt time.Time) {
	s := &ingressSession{m: m, l: l, conn: conn, ctx: ctx, cancel: cancel, changed: make(chan struct{}), faults: make(chan sessionFault, 16), gate: make(chan struct{}, 1), ackNotify: make(chan struct{}, 1)}
	s.gate <- struct{}{}
	for _, p := range m.cfg.Peers {
		if p.ID == l.Peer {
			s.p = p
		}
	}
	s.recovery = s.p.EffectiveRecoveryEnabled(m.cfg.Transport.Recovery.Enabled)
	s.grace = m.cfg.Transport.Recovery.Grace
	m.mu.Lock()
	if !m.closed {
		m.activeConns.Store(id, &activeConnEntry{cancel: cancel, closeFn: func() { s.stop(errNodeStopped) }, session: s})
	}
	m.mu.Unlock()
	defer m.activeConns.Delete(id)
	defer func() {
		s.stop(errors.New("session finished"))
		s.readerWG.Wait()
		for _, g := range s.all {
			g.close()
			g.wg.Wait()
			g.c.UnbindStream(s.sid, g.ep)
		}
		s.mu.Lock()
		for _, f := range s.frames {
			if !f.acked {
				f.acked = true
				if f.refs == 0 {
					s.releaseFrameLocked(f)
				}
			}
		}
		s.frames = nil
		s.mu.Unlock()
	}()
	createDeadline := acceptedAt.Add(m.cfg.Transport.CreateRetryTimeout)
	createCtx, createCancel := context.WithDeadline(ctx, createDeadline)
	defer createCancel()
	createTimer := time.AfterFunc(time.Until(createDeadline), func() {
		s.mu.Lock()
		finished := s.creationDone
		s.mu.Unlock()
		if !finished {
			s.stop(context.DeadlineExceeded)
		}
	})
	defer createTimer.Stop()
	strategy := m.cfg.Transport.Protocol
	if s.p.Transport != nil && s.p.Transport.Protocol != "" {
		strategy = s.p.Transport.Protocol
	}
	c, ep, mode, ok := s.create(createCtx, strategy)
	if !ok {
		return
	}
	// The first attach is bounded by the create deadline and by the owner's
	// unattached timeout, whichever is earlier.
	attachCtx, attachCancel := createCtx, context.CancelFunc(func() {})
	s.mu.Lock()
	attachBy := s.attachBy
	s.mu.Unlock()
	reapBound := !attachBy.IsZero() && attachBy.Before(createDeadline)
	if reapBound {
		attachCtx, attachCancel = context.WithDeadline(createCtx, attachBy)
	}
	g, e := s.attach(attachCtx, c, ep, 1, mode, "")
	// The first carrier never came up: unless the cause was local, cool that
	// carrier for every new selection, whatever the recovery mode.
	// attach already closed and unbound the generation.
	carrierFailed := e != nil && s.carrierFailure(attachCtx, e)
	attachCancel()
	if e != nil {
		if carrierFailed {
			s.coolCarrier(ep, c, mode)
		}
		if !s.recovery || fatalError(e) {
			s.stop(e)
			return
		}
		if reapBound && !time.Now().Before(attachBy) {
			// The owner has reaped the unattached session; a resume cannot
			// succeed.
			s.stop(fmt.Errorf("session not attached within the peer's unattached timeout: %w", context.DeadlineExceeded))
			return
		}
		// The session exists remotely; from here the recovery grace deadline,
		// not the create retry timeout, bounds it.
		createTimer.Stop()
		if !s.recover(e, strategy) {
			return
		}
	} else {
		s.startGeneration(g)
	}
	createTimer.Stop()
	createCancel()
	s.readerWG.Add(1)
	go s.readLocal()
	for {
		select {
		case <-ctx.Done():
			return
		case f := <-s.faults:
			s.mu.Lock()
			current := s.g
			normal := s.upEOF && s.upFINApplied && s.downFIN && s.downFINAcked
			s.mu.Unlock()
			if f.g != nil && f.g != current {
				continue
			}
			if !normal && !f.fatal && current != nil {
				if !s.recovery && s.finAdjacent() {
					// Recovery disabled: this transport failure is terminal
					// now. Close the client first; the final-ACK
					// query only settles normal-vs-failure afterwards and
					// must not use the 1s local-close budget.
					s.closeLocal()
				}
				var e error
				if normal, e = s.finalAck(current); fatalError(e) {
					s.stop(e)
					return
				}
			}
			if normal {
				s.stop(errClosedNormal)
				return
			}
			if f.fatal || !s.recovery {
				if !f.fatal && current != nil && s.carrierFailure(context.Background(), f.err) {
					// Recovery disabled: the session ends, but the carrier
					// that failed still cools down for new selections.
					s.m.linkManagers[s.p.ID].RecordTransportDrop(current.endpoint.ID, current.mode)
					if current.mode == "ws" {
						s.m.invalidatePrecheck(s.p.ID, current.c.URL(), s.owner)
					}
				}
				s.stop(f.err)
				return
			}
			if !s.recover(f.err, strategy) {
				return
			}
		}
	}
}
