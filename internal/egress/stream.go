package egress

import (
	"context"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"errors"
	"strconv"
	"time"
)

func (s *Server) ready(sess *session.Session, ep uint64, stream, mode string) protocol.ReadyEventData {
	return protocol.ReadyEventData{Type: "ready", ProtocolVersion: protocol.ProtocolVersionString, SessionID: sess.ID, InstanceID: sess.InstanceID, Epoch: strconv.FormatUint(ep, 10), StreamID: stream, Transport: mode, ResumeRequestID: sess.LastResumeRequestID()}
}

func (s *Server) control(sess *session.Session, ep uint64, stream, kind string) protocol.WSTextControlMessage {
	return protocol.WSTextControlMessage{Type: kind, SessionID: sess.ID, InstanceID: sess.InstanceID, Epoch: strconv.FormatUint(ep, 10), StreamID: stream}
}

// downFrame is one downstream DATA frame handed to a transport writer: the
// payload starting at offset, as read-only views of the session window
// (Session.ReadDownSegments). The writer encodes it with a single copy.
type downFrame struct {
	offset uint64
	segs   [][]byte
}

func (s *Server) streamWriter(ctx context.Context, sess *session.Session, ep uint64, stream string, write func(string, []byte, any, uint64) error) error {
	hb := s.cfg.Transport.Heartbeat
	if hb <= 0 {
		hb = 15 * time.Second
	}
	detect := s.cfg.Transport.FailureDetectTimeout
	if detect <= 0 {
		detect = 45 * time.Second
	}
	tick := s.cfg.Transport.UpAckMaxDelay
	if tick <= 0 {
		tick = 10 * time.Millisecond
	}
	timer := time.NewTicker(tick)
	defer timer.Stop()
	n, _ := s.downNotifier.Load(sess.ID)
	notify, _ := n.(chan struct{})
	offset := sess.DownAckOffset()
	lastACK := ""
	lastFin := false
	_, downFinState := sess.FINs()
	finSent := downFinState.Acked
	lastPing := time.Time{}
	lastWanted := false
	codec := s.codecFor(sess.OwnerPeer)
	frameCost := sseDownCost
	if sess.GetTransportMode() == "ws" {
		frameCost = wsDownCost
	}
schedule:
	for {
		if sess.GetEpoch() != ep {
			return session.ErrStaleEpoch
		}
		if sess.GetState() == session.StateClosed {
			return session.ErrSessionClosed
		}
		if sess.ControlExpired(detect) {
			return errors.New("reverse heartbeat timeout")
		}
		if time.Since(lastPing) >= hb {
			v := s.control(sess, ep, stream, "ping")
			var err error
			v.Nonce, err = sess.NewHeartbeat(ep, stream)
			if err != nil {
				return err
			}
			if e := write("ping", nil, v, 0); e != nil {
				return e
			}
			lastPing = time.Now()
		}
		a := sess.Snapshot()
		// A newly wanted control ACK is announced even when the ack
		// offset has not moved; re-sending an unchanged ack_up_offset is
		// a no-op for the peer.
		wanted := sess.AckWanted()
		if a.AckUpOffset != lastACK || a.UpFinApplied != lastFin || wanted && !lastWanted {
			v := s.control(sess, ep, stream, "ack_up")
			v.AckUpOffset = a.AckUpOffset
			v.UpFinApplied = a.UpFinApplied
			v.AckRequested = wanted
			if e := write("ack_up", nil, v, 0); e != nil {
				return e
			}
			lastACK = a.AckUpOffset
			lastFin = a.UpFinApplied
		}
		lastWanted = wanted
		if sess.IsDataPlaneActive() {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				// Continuous downstream production must yield at frame boundaries
				// when an upstream confirmation or heartbeat becomes due.
				current := sess.Snapshot()
				if current.AckUpOffset != lastACK || current.UpFinApplied != lastFin || time.Since(lastPing) >= hb {
					continue schedule
				}
				size := sess.NextDownFrameBytes(offset)
				cost := frameCost(size)
				if size > 0 && !codec.Try(cost) {
					break
				}
				segs, n, end, eof := sess.ReadDownSegments(offset, size)
				if n > 0 {
					e := sess.BeginEmit(ep, offset, end)
					if e == nil {
						sess.SetTransportWriting(ep, stream, true)
						e = write("data", nil, downFrame{offset: offset, segs: segs}, end)
						sess.SetTransportWriting(ep, stream, false)
					}
					sess.FinishEmit(ep, end, e)
					codec.Put(cost)
					if e != nil {
						return e
					}
					offset = end
				}
				if size > 0 && n == 0 {
					codec.Put(cost)
				}
				if eof && !finSent {
					if e := sess.BeginFinEmit(ep); e != nil {
						return e
					}
					v := s.control(sess, ep, stream, "fin")
					v.FinalOffset = strconv.FormatUint(end, 10)
					e := write("fin", nil, v, end)
					sess.FinishFinEmit(ep, e)
					if e != nil {
						return e
					}
					finSent = true
				}
				if n == 0 {
					break
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sess.Done():
			return session.ErrSessionClosed
		case <-timer.C:
		case <-notify:
		}
	}
}

func (s *Server) controlTimeout() time.Duration {
	if s.cfg.Transport.FailureDetectTimeout > 0 {
		return s.cfg.Transport.FailureDetectTimeout
	}
	return 45 * time.Second
}

func (s *Server) watchControl(ctx context.Context, sess *session.Session, ep uint64, cancel context.CancelFunc) {
	detect := s.controlTimeout()
	ticker := time.NewTicker(max(time.Nanosecond, min(detect/4, 250*time.Millisecond)))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sess.Done():
			// The stream writer announces the terminal state on its own
			// carrier first; tear the carrier down if it cannot finish.
			wait := time.NewTimer(terminalNoticeGrace)
			select {
			case <-ctx.Done():
			case <-wait.C:
			}
			wait.Stop()
			cancel()
			return
		case <-ticker.C:
			if sess.ControlExpired(detect) {
				sess.DropTransport(ep)
				cancel()
				return
			}
		}
	}
}

const (
	// terminalNoticeTimeout bounds writing the terminal error notice.
	terminalNoticeTimeout = time.Second
	// terminalNoticeGrace bounds how long a carrier of a terminated session
	// stays open for its writer to deliver the notice.
	terminalNoticeGrace = 2 * time.Second
)

// terminalError is the bound terminal `error` control message. retryable and
// session_closed are always present on the wire.
type terminalError struct {
	Type          string `json:"type"`
	SessionID     string `json:"session_id"`
	InstanceID    string `json:"instance_id"`
	Epoch         string `json:"epoch"`
	StreamID      string `json:"stream_id"`
	Code          string `json:"code"`
	SessionClosed bool   `json:"session_closed"`
	Retryable     bool   `json:"retryable"`
}

// terminalNotice returns the error message announcing that this node
// terminated the session, when stream of epoch ep is still the session's
// stream and the session ended other than by a normal close (both FINs).
// Normal closes, superseded streams and open sessions get no notice.
func (s *Server) terminalNotice(sess *session.Session, ep uint64, stream string) (terminalError, bool) {
	if sess.GetState() != session.StateClosed || sess.GetEpoch() != ep || sess.CurrentStreamID() != stream || closeCategory(sess) == "normal" {
		return terminalError{}, false
	}
	return terminalError{Type: "error", SessionID: sess.ID, InstanceID: sess.InstanceID, Epoch: strconv.FormatUint(ep, 10), StreamID: stream, Code: sess.TerminalCode(), SessionClosed: true}, true
}

// streamCancel wraps a carrier's cancel for Session.SetStreamCancel. The
// session calls it when a resume supersedes the stream, which cancels at once,
// and when the session terminates; then the carrier stays up until done is
// closed (the writer delivered the terminal notice) or terminalNoticeGrace
// passes.
func streamCancel(sess *session.Session, cancel context.CancelFunc, done <-chan struct{}) context.CancelFunc {
	return func() {
		if sess.GetState() != session.StateClosed {
			cancel()
			return
		}
		go func() {
			wait := time.NewTimer(terminalNoticeGrace)
			defer wait.Stop()
			select {
			case <-done:
			case <-wait.C:
			}
			cancel()
		}()
	}
}
