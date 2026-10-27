package egress

import (
	"dbbridge/internal/auth"
	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
)

func (s *Server) handleSessionRoutes(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/sessions/"), "/")
	if len(parts) < 1 || len(parts) > 2 {
		reject(w, r, 404, protocol.ErrSessionNotFound, "not found", false, false)
		return
	}
	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}
	method := "POST"
	if action == "" {
		method = "DELETE"
	} else if action == "events" {
		method = "GET"
	}
	if r.Method != method {
		methodNotAllowed(w, r, method)
		return
	}
	peer, ok := s.peer(w, r)
	if !ok || !s.instance(w, r) {
		return
	}
	sess := s.registry.GetAny(parts[0])
	if sess == nil || sess.OwnerPeer != peer {
		reject(w, r, 404, protocol.ErrSessionNotFound, "not found", true, false)
		return
	}
	if !auth.ValidateSessionToken(sess.SessionToken, r.Header.Get(protocol.HeaderSessionToken)) {
		reject(w, r, 401, protocol.ErrUnauthorized, "invalid session credentials", false, false)
		return
	}
	if s.auth.AuthorizeTarget(peer, sess.TargetID) != nil {
		sess.CloseWithReason(protocol.ErrTargetForbidden)
		reject(w, r, 403, protocol.ErrTargetForbidden, "authorization revoked", true, false)
		return
	}
	if !s.bindCred(peer, credential(r)) {
		unauthorized(w, r)
		return
	}
	if action == "resume" {
		s.handleResume(w, r, sess)
		return
	}
	ep, e := protocol.Offset(r.URL.Query().Get("epoch"))
	if e != nil || ep != sess.GetEpoch() {
		reject(w, r, 409, protocol.ErrStaleEpoch, "epoch", false, false)
		return
	}
	switch action {
	case "events":
		if sess.GetTransportMode() != "http-sse" {
			reject(w, r, 409, protocol.ErrTransportMismatch, "mode", false, false)
			return
		}
		s.handleSSEEvents(w, r, sess)
	case "up":
		if sess.GetTransportMode() != "http-sse" {
			reject(w, r, 409, protocol.ErrTransportMismatch, "mode", false, false)
			return
		}
		s.handleUp(w, r, sess)
	case "ack":
		s.handleAck(w, r, sess)
	case "fin":
		s.handleFin(w, r, sess)
	case "":
		if e := sess.CloseWithEpoch(ep); e != nil {
			s.writeSessionError(w, sess, e)
			return
		}
		writeJSON(w, 200, protocol.DeleteSessionResponse{SessionID: sess.ID, InstanceID: sess.InstanceID, Epoch: strconv.FormatUint(sess.GetEpoch(), 10), Closed: true})
	default:
		reject(w, r, 404, protocol.ErrBadFrame, "action", false, false)
	}
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	atomic.AddInt64(&observability.DefaultMetrics.ExplicitAckTotal, 1)
	var q protocol.AckRequest
	if !decode(w, r, &q) {
		return
	}
	stream := r.Header.Get(protocol.HeaderStreamID)
	if q.StreamID != "" && q.StreamID != stream {
		writeJSONError(w, 400, protocol.ErrBadFrame, "stream mismatch", false, false)
		return
	}
	ep, _ := protocol.Offset(r.URL.Query().Get("epoch"))
	resp, e := sess.Ack(ep, stream, q)
	if e != nil {
		s.writeStreamError(w, sess, ep, stream, e)
		return
	}
	writeJSON(w, 200, resp)
}

func (s *Server) handleFin(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	ep, _ := protocol.Offset(r.URL.Query().Get("epoch"))
	_ = sess.ActivateImplicit(ep, r.Header.Get(protocol.HeaderStreamID)) // protocol.HeaderImplicitReady
	if e := sess.CheckStream(ep, r.Header.Get(protocol.HeaderStreamID), true); e != nil {
		closeIfUnread(w, r)
		s.writeSessionError(w, sess, e)
		return
	}
	var q protocol.FinRequest
	if !decode(w, r, &q) {
		return
	}
	off, e := protocol.Offset(q.FinalOffset)
	if e != nil || q.Direction != "up" {
		writeJSONError(w, 400, protocol.ErrBadFrame, "FIN", false, false)
		return
	}
	_, e = sess.IngestUpFIN(ep, off)
	if e != nil {
		s.writeStreamError(w, sess, ep, r.Header.Get(protocol.HeaderStreamID), e)
		return
	}
	a := sess.Snapshot()
	writeJSON(w, 200, protocol.FinResponse{SessionID: a.SessionID, InstanceID: a.InstanceID, Epoch: a.Epoch, UpFinApplied: a.UpFinApplied, StreamID: a.StreamID, Transport: a.Transport})
}

func (s *Server) handleResume(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	var q protocol.ResumeRequest
	if !decode(w, r, &q) {
		return
	}
	if !s.allows(q.Transport.Mode) {
		writeJSONError(w, 409, protocol.ErrTransportUnsupported, "transport", false, false)
		return
	}
	release, fair := s.auth.ReserveFair(sess.OwnerPeer, "resume", s.cfg.Resources.Recovery.Limit())
	if !fair {
		writeJSONError(w, 429, protocol.ErrQuotaExceeded, "peer recovery budget", false, true)
		return
	}
	defer release()
	if !s.cfg.Resources.Recovery.Try(1) {
		writeJSONError(w, 429, protocol.ErrQuotaExceeded, "recovery budget", false, true)
		return
	}
	defer s.cfg.Resources.Recovery.Put(1)
	resp, e := sess.Resume(&q, s.cfg.Transport.Recovery.BarrierTimeout)
	if e != nil {
		s.writeSessionError(w, sess, e)
		return
	}
	writeJSON(w, 200, resp)
}
