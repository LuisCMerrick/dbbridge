package egress

import (
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"encoding/json"
	"errors"
	"net/http"
)

func writeJSONError(w http.ResponseWriter, status int, code, msg string, closed, retry bool) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: code, Message: msg, Retryable: retry, SessionClosed: closed})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if e := protocol.DecodeJSON(r.Body, 16<<10, v); e != nil {
		writeJSONError(w, 400, protocol.ErrBadFrame, "invalid control body", false, false)
		return false
	}
	return true
}

func (s *Server) writeSessionErr(w http.ResponseWriter, e error) {
	code, st, closed, retry := protocol.ErrBadFrame, 400, false, false
	switch {
	case errors.Is(e, session.ErrSessionClosed):
		code, st, closed = protocol.ErrSessionClosed, 410, true
	case errors.Is(e, session.ErrStaleEpoch):
		code, st = protocol.ErrStaleEpoch, 409
	case errors.Is(e, session.ErrResumeExpired):
		code, st, closed = protocol.ErrResumeExpired, 410, true
	case errors.Is(e, session.ErrResumePending):
		code, st, retry = protocol.ErrResumePending, 202, true
	case errors.Is(e, session.ErrResumeInProgress):
		code, st, retry = protocol.ErrResumeInProgress, 409, true
	case errors.Is(e, session.ErrIdempotencyConflict):
		code, st = protocol.ErrIdempotencyConflict, 409
	case errors.Is(e, session.ErrResumeDisabled):
		code, st = protocol.ErrRecoveryDisabled, 409
	case errors.Is(e, session.ErrBadAck):
		code = protocol.ErrBadAck
	case errors.Is(e, session.ErrFrameConflict):
		code, st, closed = protocol.ErrFrameConflict, 409, true
	case errors.Is(e, session.ErrFrameTooLarge):
		code, st = protocol.ErrFrameTooLarge, 413
	case errors.Is(e, session.ErrReplayTooOld):
		code, st = protocol.ErrReplayTooOld, 409
	case errors.Is(e, session.ErrWindowExceeded), errors.Is(e, session.ErrNotActive):
		code, st, retry = protocol.ErrWindowExceeded, 409, true
	case errors.Is(e, session.ErrBufferExhausted):
		code, st, retry = protocol.ErrBufferExhausted, 503, true
	case errors.Is(e, session.ErrStreamExists):
		code, st = protocol.ErrStreamExists, 409
		// ErrTransportMismatch and ErrInvalidToken deliberately stay 400
		// BAD_FRAME: the ingress treats that as fatal, which is the intended
		// outcome; a new non-fatal code would make it retry.
	}
	writeJSONError(w, st, code, "session operation rejected", closed, retry)
}

func (s *Server) writeSessionError(w http.ResponseWriter, sess *session.Session, err error) {
	if errors.Is(err, session.ErrSessionClosed) {
		code := sess.TerminalCode()
		switch code {
		case protocol.ErrBackendLost, protocol.ErrResumeExpired, protocol.ErrBufferExhausted, protocol.ErrTargetForbidden, protocol.ErrFrameConflict:
			writeJSONError(w, 410, code, "session terminated", true, false)
			return
		}
		// The message carries the terminal category, so the ingress can tell
		// a normal close from an abnormal one.
		writeJSONError(w, 410, protocol.ErrSessionClosed, protocol.SessionClosedMessagePrefix+closeCategory(sess), true, false)
		return
	}
	s.writeSessionErr(w, err)
}

// writeStreamError answers a data-plane error on stream of epoch ep. A frame
// conflict terminates the session with DBB_FRAME_CONFLICT before the answer
// says session_closed:true; when the stream no longer owns the session (a
// resume superseded it) nothing is closed and the answer says so.
func (s *Server) writeStreamError(w http.ResponseWriter, sess *session.Session, ep uint64, stream string, err error) {
	if errors.Is(err, session.ErrFrameConflict) && !sess.CloseStreamWithReason(ep, stream, protocol.ErrFrameConflict) && sess.GetState() != session.StateClosed {
		writeJSONError(w, 409, protocol.ErrFrameConflict, "session operation rejected", false, false)
		return
	}
	s.writeSessionError(w, sess, err)
}

// SessionClosedNormalMessage is the error message of a 410
// DBB_SESSION_CLOSED for a session that ended after both FINs completed.
const SessionClosedNormalMessage = protocol.SessionClosedNormalMessage

// closeCategory is the session's terminal category: "normal" for a
// closed_normal session, otherwise the category of its close reason
// ("timeout", "failed", ...). A session whose upstream FIN was applied and
// whose downstream FIN was acknowledged is reported as normal whatever ended
// it afterwards, since every byte was delivered.
func closeCategory(sess *session.Session) string {
	cat := sess.TerminalCategory()
	if cat == "normal" {
		return cat
	}
	if up, down := sess.FINs(); up.Applied && down.Acked {
		return "normal"
	}
	if cat == "" {
		return "failed"
	}
	return cat
}
