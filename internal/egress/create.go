package egress

import (
	"context"
	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"sync/atomic"
	"time"
)

func (s *Server) handleSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		methodNotAllowed(w, r, "POST")
		return
	}
	peer, ok := s.peer(w, r)
	if !ok || !s.instance(w, r) {
		return
	}
	// Creation slots are taken only by authenticated callers, and no single
	// peer may hold the whole budget while other peers are configured.
	release, fair := s.auth.ReserveFair(peer, "create", s.cfg.Resources.Creates.Limit())
	if !fair {
		reject(w, r, 429, protocol.ErrQuotaExceeded, "peer creation concurrency", false, true)
		return
	}
	defer release()
	if !s.cfg.Resources.Creates.Try(1) {
		reject(w, r, 429, protocol.ErrQuotaExceeded, "creation concurrency", false, true)
		return
	}
	defer s.cfg.Resources.Creates.Put(1)
	cred := credential(r)
	var q protocol.CreateSessionRequest
	if !decode(w, r, &q) {
		return
	}
	if !validCreate(w, &q, s) {
		return
	}
	keyBytes, _ := json.Marshal([]string{peer, q.IngressInstanceID, q.CreateRequestID})
	key := string(keyBytes)
	metadataCost := s.sessionMetadataCost()
	lease := &createLease{s: s, peer: peer, target: q.TargetID, key: key, metadataCost: metadataCost}
	defer lease.rollback()
	// A retry is answered from the idempotency table before anything is
	// reserved: a pending create answers 202, a failed one its cached
	// refusal, a completed one its response, and differing parameters a
	// conflict. Only a key the table does not hold takes resources.
	if status, cached, code, msg, found := s.registry.LookupCreate(key, q.TargetID, q.Limits, q.Recovery, q.Transport.Mode); found {
		s.answerCreate(w, status, cached, code, msg)
		return
	}
	t, known := s.cfg.Targets[q.TargetID]
	// Deterministic refusals are answered before anything is reserved and
	// leave no idempotency record: an authenticated peer cannot fill the
	// shared table and metadata budget with refused creates.
	if !known || s.auth.AuthorizeTarget(peer, q.TargetID) != nil {
		writeJSONError(w, 403, protocol.ErrTargetForbidden, "creation refused", false, false)
		return
	}
	lim, limitsOK := s.negotiateLimits(q.Limits)
	if !limitsOK {
		// Checked before dialing so a bad combination never opens and drops
		// a backend connection.
		writeJSONError(w, 400, protocol.ErrBadFrame, "creation refused", false, false)
		return
	}
	// Transient refusals (quotas, budgets) are taken before the table entry
	// exists, so they are never cached and a retry with the same
	// create_request_id succeeds once capacity frees.
	if st, code, msg := lease.acquire(); st != 0 {
		writeJSONError(w, st, code, msg, false, st == 429 || st == 503)
		return
	}
	status, cached, code, msg := s.registry.ReserveCreate(key, q.TargetID, q.Limits, q.Recovery, q.Transport.Mode)
	if status != session.CreateReserved {
		// A concurrent request with the same key reserved it first, or the
		// node refused a new record (draining, table or budget full).
		s.answerCreate(w, status, cached, code, msg)
		return
	}
	lease.reserve()
	network := t.Network
	if network == "" {
		network = "tcp"
	}
	timeout := s.cfg.Transport.DialTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	// One deadline, cancelled by Close, covers DNS resolution and dialing
	//.
	dialCtx, cancelDial := context.WithTimeout(s.ctx, timeout)
	defer cancelDial()
	addrs, e := resolveTarget(dialCtx, network, t.Address, t.AllowedCIDRs)
	if e != nil {
		switch {
		case s.ctx.Err() != nil:
			lease.fail(w, 503, protocol.ErrDraining)
		case errors.Is(e, errTargetNotAllowed):
			lease.fail(w, 403, protocol.ErrTargetForbidden)
		default:
			// A resolver failure or timeout is a link problem, not a
			// verdict on the target's authorization; it counts as a
			// failed dial attempt.
			atomic.AddInt64(&observability.DefaultMetrics.DialTotal, 1)
			atomic.AddInt64(&observability.DefaultMetrics.DialFailedTotal, 1)
			lease.fail(w, 502, protocol.ErrDialFailed)
		}
		return
	}
	atomic.AddInt64(&observability.DefaultMetrics.DialTotal, 1)
	conn, e := dialTarget(dialCtx, network, addrs, timeout)
	if e != nil {
		if s.ctx.Err() != nil {
			lease.fail(w, 503, protocol.ErrDraining)
			return
		}
		atomic.AddInt64(&observability.DefaultMetrics.DialFailedTotal, 1)
		lease.fail(w, 502, protocol.ErrDialFailed)
		return
	}
	grace := time.Duration(0)
	if q.Recovery.Enabled {
		grace = s.cfg.Transport.Recovery.Grace
		if requested := time.Duration(q.Recovery.GraceMs) * time.Millisecond; requested < grace {
			grace = requested
		}
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		conn.Close()
		// Resolve the idempotency reservation so a retry is not told "pending".
		lease.fail(w, 503, protocol.ErrDraining)
		return
	}
	if err := s.auth.AuthorizeTarget(peer, q.TargetID); err != nil || !s.auth.PeerTokenValid(peer, cred) {
		s.mu.Unlock()
		_ = conn.Close()
		lease.fail(w, 403, protocol.ErrTargetForbidden)
		return
	}
	sess := session.NewSession(peer, q.TargetID, s.cfg.Node.InstanceID, conn, lim.DownBufferBytes, lim.UpWindowBytes, lim.UpMaxInflightFrames, s.cfg.Transport.ReceiptLimitPerSession, q.Recovery.Enabled)
	sess.SetTransportMode(q.Transport.Mode)
	sess.SetNegotiatedLimits(lim)
	lim.UpReceiptLimit, lim.DownMaxFrames = protocol.ClampAdvertisedFrames(sess.UpReceiptCapacity()), protocol.ClampAdvertisedFrames(session.MaxDownFrames)
	sess.SetChunkSize(s.chunkSize())
	notify := make(chan struct{}, 1)
	sess.Configure(grace, s.cfg.Transport.SocketWriteTimeout, q.IngressInstanceID, notify)
	sess.SetOnFinalize(func(*session.Session) {
		s.auth.DecrSession(peer)
		s.cfg.Resources.Sessions.Put(1)
		s.cfg.Resources.Metadata.Put(metadataCost)
		s.downNotifier.Delete(sess.ID)
		s.sessions.Delete(sess.ID)
		// The create record and tombstone outlive the session by the
		// tombstone TTL; keep them charged to the peer until then.
		s.idem.closed(peer, key, time.Now().Add(s.idemTTL()+time.Second))
	})
	s.sessions.Store(sess.ID, &sessState{upLimit: int64(lim.UpMaxInflightFrames)})
	s.registry.Register(sess)
	s.downNotifier.Store(sess.ID, notify)
	lease.commit()
	unattached := s.cfg.Transport.UnattachedTimeout
	if unattached <= 0 {
		unattached = 30 * time.Second
	}
	// The timer closes a still unattached session itself, under the lock that
	// also decides ReserveStream/ready activation.
	sess.StartUnattachedTimer(unattached, nil)
	s.wg.Add(1) // s.mu is held and s.stopped is false, so this precedes Close's Wait
	go func() { defer s.wg.Done(); s.runBackendReader(sess, notify) }()
	resp := &protocol.CreateSessionResponse{ProtocolVersion: "1.4", NodeID: s.cfg.Node.ID, InstanceID: s.cfg.Node.InstanceID, SessionID: sess.ID, SessionToken: sess.SessionToken, Epoch: "1", Transport: q.Transport, Recovery: protocol.RecoveryNegotiation{Enabled: q.Recovery.Enabled, GraceMs: grace.Milliseconds()}, Limits: lim, UnattachedTimeoutMs: unattached.Milliseconds()}
	s.registry.CompleteCreate(key, resp)
	s.mu.Unlock()
	writeJSON(w, 201, resp)
}

// validCreate applies the request-only checks; each refusal is deterministic
// and leaves no record.
func validCreate(w http.ResponseWriter, q *protocol.CreateSessionRequest, s *Server) bool {
	if q.ProtocolVersion != "1.4" {
		writeJSONError(w, 409, protocol.ErrProtocolMismatch, "protocol", false, false)
		return false
	}
	if q.TargetID == "" || q.CreateRequestID == "" || q.IngressInstanceID == "" || len(q.CreateRequestID) > 128 || len(q.IngressInstanceID) > 128 {
		writeJSONError(w, 400, protocol.ErrBadFrame, "missing/invalid identity", false, false)
		return false
	}
	if !s.allows(q.Transport.Mode) {
		writeJSONError(w, 409, protocol.ErrTransportUnsupported, "transport", false, false)
		return false
	}
	if q.Recovery.Enabled && !s.cfg.Transport.Recovery.Enabled {
		writeJSONError(w, 409, protocol.ErrRecoveryUnsupported, "recovery unsupported", false, false)
		return false
	}
	if !q.Recovery.Enabled && q.Recovery.GraceMs != 0 || q.Recovery.Enabled && q.Recovery.GraceMs <= 0 {
		writeJSONError(w, 400, protocol.ErrBadFrame, "recovery grace", false, false)
		return false
	}
	if q.Recovery.GraceMs > math.MaxInt64/int64(time.Millisecond) || q.Limits.MaxFrameSize <= 0 || q.Limits.MaxFrameSize > 1<<20 || q.Limits.UpMaxInflightFrames <= 0 || q.Limits.UpWindowBytes <= 0 || q.Limits.DownBufferBytes <= 0 || int64(q.Limits.MaxFrameSize) > q.Limits.UpWindowBytes || q.Limits.UpWindowBytes > q.Limits.DownBufferBytes {
		writeJSONError(w, 400, protocol.ErrBadFrame, "invalid session limits or recovery duration", false, false)
		return false
	}
	return true
}

func (s *Server) chunkSize() int {
	if s.cfg.Transport.ChunkSize > 0 {
		return s.cfg.Transport.ChunkSize
	}
	return 32 << 10
}

// sessionMetadataCost is the data-plane metadata one live session holds.
func (s *Server) sessionMetadataCost() int64 {
	bufferSize := s.cfg.Transport.BufferPerDirection
	if bufferSize <= 0 {
		bufferSize = 4 << 20
	}
	return int64(64<<10) + int64(s.cfg.Transport.ReceiptLimitPerSession)*160 + int64(s.cfg.Transport.UpMaxInflightFrames)*192 + (bufferSize/int64(s.chunkSize())+513)*32
}

// negotiateLimits lowers the configured limits to the request's and reports
// whether the result is usable.
func (s *Server) negotiateLimits(req protocol.SessionLimits) (protocol.SessionLimits, bool) {
	lim := protocol.SessionLimits{MaxFrameSize: s.cfg.Transport.MaxFrameSize, UpMaxInflightFrames: s.cfg.Transport.UpMaxInflightFrames, UpWindowBytes: s.cfg.Transport.UpWindowBytes, DownBufferBytes: s.cfg.Transport.BufferPerDirection}
	if lim.MaxFrameSize <= 0 {
		lim.MaxFrameSize = 256 << 10
	}
	if lim.UpMaxInflightFrames <= 0 {
		lim.UpMaxInflightFrames = 8
	}
	if lim.UpWindowBytes <= 0 {
		lim.UpWindowBytes = 2 << 20
	}
	if lim.DownBufferBytes <= 0 {
		lim.DownBufferBytes = 4 << 20
	}
	if req.MaxFrameSize > 0 && req.MaxFrameSize < lim.MaxFrameSize {
		lim.MaxFrameSize = req.MaxFrameSize
	}
	if req.UpMaxInflightFrames > 0 && req.UpMaxInflightFrames < lim.UpMaxInflightFrames {
		lim.UpMaxInflightFrames = req.UpMaxInflightFrames
	}
	if req.UpWindowBytes > 0 && req.UpWindowBytes < lim.UpWindowBytes {
		lim.UpWindowBytes = req.UpWindowBytes
	}
	if req.DownBufferBytes > 0 && req.DownBufferBytes < lim.DownBufferBytes {
		lim.DownBufferBytes = req.DownBufferBytes
	}
	ok := int64(lim.MaxFrameSize) <= lim.UpWindowBytes && int64(lim.MaxFrameSize) <= lim.DownBufferBytes
	return lim, ok
}

// answerCreate answers a create from an existing idempotency record or from
// a refused reservation.
func (s *Server) answerCreate(w http.ResponseWriter, status session.CreateReserveStatus, cached *protocol.CreateSessionResponse, code, msg string) {
	switch status {
	case session.CreateDone:
		if cached == nil || s.registry.Get(cached.SessionID) == nil {
			writeJSONError(w, 410, protocol.ErrSessionClosed, "creation is terminal", true, false)
		} else {
			writeJSON(w, 200, cached)
		}
	case session.CreatePending:
		w.Header().Set("Retry-After", "1")
		writeJSONError(w, 202, protocol.ErrCreatePending, "pending", false, true)
	case session.CreateConflict:
		writeJSONError(w, 409, protocol.ErrIdempotencyConflict, "parameters differ", false, false)
	default:
		st := 502
		switch code {
		case protocol.ErrTargetForbidden:
			st = 403
		case protocol.ErrQuotaExceeded:
			st = 429
		case protocol.ErrDraining:
			st = 503
		}
		writeJSONError(w, st, code, msg, false, st == 429 || st == 503)
	}
}
