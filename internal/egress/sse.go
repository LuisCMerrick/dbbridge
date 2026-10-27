package egress

import (
	"context"
	"crypto/sha256"
	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func (s *Server) handleSSEEvents(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	ep, _ := protocol.Offset(r.URL.Query().Get("epoch"))
	stream, e := sess.ReserveStream(ep, "http-sse")
	if e != nil {
		s.writeSessionError(w, sess, e)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	finished := make(chan struct{})
	defer close(finished)
	if e = sess.SetStreamCancel(ep, stream, streamCancel(sess, cancel, finished)); e != nil {
		s.writeSessionError(w, sess, e)
		return
	}
	defer sess.DropTransport(ep)
	releaseAdmission(r)
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	rc := http.NewResponseController(w)
	// The stream outlives the request read deadline; liveness is enforced by
	// heartbeats (watchControl) and write-idle deadlines instead.
	_ = rc.SetReadDeadline(time.Time{})
	var deadlineMu sync.Mutex
	writeDone := make(chan struct{})
	stopWrite := context.AfterFunc(ctx, func() {
		deadlineMu.Lock()
		_ = rc.SetWriteDeadline(time.Now())
		deadlineMu.Unlock()
		close(writeDone)
	})
	defer func() {
		if !stopWrite() {
			<-writeDone
		}
	}()
	s.wg.Add(1) // this handler is itself counted, so the counter is non-zero
	go func() { defer s.wg.Done(); s.watchControl(ctx, sess, ep, cancel) }()
	timeout := s.cfg.Transport.SSEWriteIdleTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	write := func(kind string, v any, id string) error {
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		event := ""
		if id != "" {
			event = "id: " + id + "\n"
		}
		event += "event: " + kind + "\ndata: " + string(b) + "\n\n"
		return writeSSEProgress(ctx, w, rc, &deadlineMu, []byte(event), timeout)
	}
	if write("ready", s.ready(sess, ep, stream, "http-sse"), "") != nil {
		return
	}
	_ = s.streamWriter(ctx, sess, ep, stream, func(kind string, data []byte, v any, end uint64) error {
		if kind == "data" {
			f := v.(downFrame)
			// The event is assembled in one buffer: payload segments are
			// hashed and Base64-encoded straight into it.
			n := 0
			for _, seg := range f.segs {
				n += len(seg)
			}
			event := make([]byte, 0, 96+base64.StdEncoding.EncodedLen(n))
			event = append(event, "id: "...)
			event = strconv.AppendUint(event, ep, 10)
			event = append(event, ':')
			event = strconv.AppendUint(event, end, 10)
			event = append(event, "\nevent: data\ndata: "...)
			event, err := protocol.AppendSSEDataSegments(event, f.offset, f.segs)
			if err != nil {
				return err
			}
			event = append(event, "\n\n"...)
			return writeSSEProgress(ctx, w, rc, &deadlineMu, event, timeout)
		}
		return write(kind, v, "")
	})
	// A session this node terminated ends its event stream with a bound
	// terminal error event, so the ingress closes the local session instead
	// of reconnecting.
	if notice, ok := s.terminalNotice(sess, ep, stream); ok {
		timeout = terminalNoticeTimeout
		_ = write("error", notice, "")
	}
}

func (s *Server) handleUp(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	atomic.AddInt64(&observability.DefaultMetrics.PostCountTotal, 1)
	ep, _ := protocol.Offset(r.URL.Query().Get("epoch"))
	stream := r.Header.Get(protocol.HeaderStreamID)
	if e := sess.CheckStream(ep, stream, true); e != nil {
		closeIfUnread(w, r)
		s.writeSessionError(w, sess, e)
		return
	}
	off, e := protocol.Offset(r.Header.Get(protocol.HeaderOffset))
	length, le := strconv.Atoi(r.Header.Get(protocol.HeaderLength))
	if e != nil || le != nil || length <= 0 || r.Header.Get("Content-Encoding") != "" || r.Header.Get("Content-Type") != "application/octet-stream" {
		reject(w, r, 400, protocol.ErrBadFrame, "frame headers", false, false)
		return
	}
	// A frame or body above the negotiated max_frame_size is refused
	// unread; the connection is closed instead of drained.
	if maxFrame := int64(sess.NegotiatedMaxFrameSize()); int64(length) > maxFrame || r.ContentLength > maxFrame {
		reject(w, r, 413, protocol.ErrFrameTooLarge, "frame too large", false, false)
		return
	}
	if r.ContentLength != int64(length) {
		reject(w, r, 400, protocol.ErrBadFrame, "frame headers", false, false)
		return
	}
	// The session's negotiated in-flight frame limit bounds its concurrent
	// /up bodies before any codec memory is taken; 409
	// WINDOW_EXCEEDED is retryable for the ingress.
	if st, ok := s.sessions.Load(sess.ID); ok {
		st := st.(*sessState)
		if st.upActive.Add(1) > st.upLimit {
			st.upActive.Add(-1)
			reject(w, r, 409, protocol.ErrWindowExceeded, "too many frames in flight", false, true)
			return
		}
		defer st.upActive.Add(-1)
	}
	// The body is charged to the owner's share of the egress Codec.
	codec := s.codecFor(sess.OwnerPeer)
	if !codec.Try(int64(length)) {
		reject(w, r, 503, protocol.ErrBufferExhausted, "codec budget", false, true)
		return
	}
	defer codec.Put(int64(length))
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(s.upBodyTimeout(length)))
	// Read into an exact-size buffer the session can adopt (see
	// IngestUpFrameOwned) instead of an io.ReadAll buffer it would copy.
	body, e := readExactBody(r.Body, length)
	if e != nil {
		writeJSONError(w, 400, protocol.ErrBadFrame, "body", false, false)
		return
	}
	digest := sha256.Sum256(body)
	sha := hex.EncodeToString(digest[:])
	expected := r.Header.Get(protocol.HeaderSHA256)
	if sha != expected {
		writeJSONError(w, 400, protocol.ErrBadDigest, "digest", false, false)
		return
	}
	down, up := uint64(0), uint64(0)
	if v := r.Header.Get(protocol.HeaderAckDownOffset); v != "" {
		down, e = protocol.Offset(v)
		if e != nil {
			s.writeSessionErr(w, session.ErrBadAck)
			return
		}
	}
	if v := r.Header.Get(protocol.HeaderConfirmUpOffset); v != "" {
		up, e = protocol.Offset(v)
		if e != nil {
			s.writeSessionErr(w, session.ErrBadAck)
			return
		}
	}
	if e = sess.ApplyPiggyback(ep, stream, down, up); e != nil {
		s.writeSessionError(w, sess, e)
		return
	}
	atomic.AddInt64(&observability.DefaultMetrics.PiggybackAckTotal, 1)
	written, accepted, _, _, e := sess.IngestUpFrameOwned(ep, off, sha, body, false)
	if e != nil {
		s.writeStreamError(w, sess, ep, stream, e)
		return
	}
	a := sess.Snapshot()
	ack, _ := protocol.Offset(a.AckUpOffset)
	status := 202
	if written {
		status = 200
	}
	writeJSON(w, status, protocol.UpResponse{SessionID: a.SessionID, InstanceID: a.InstanceID, Epoch: a.Epoch, FrameAccepted: accepted, AckUpOffset: a.AckUpOffset, DownAckOffset: a.DownAckOffset, UpReceiveLimit: strconv.FormatUint(sess.UpReceiveLimit(ack), 10), UpFinApplied: a.UpFinApplied, StreamID: a.StreamID, Transport: a.Transport})
}

// Flushing bounded pieces exposes stream progress even when a whole SSE event
// takes longer than the idle timeout. The deadline is scoped to this HTTP/2
// stream; other multiplexed streams and independent controls are unaffected.
func writeSSEProgress(ctx context.Context, w http.ResponseWriter, rc *http.ResponseController, deadlineMu *sync.Mutex, event []byte, idle time.Duration) error {
	piece := ssePieceMin
	for len(event) > 0 {
		deadlineMu.Lock()
		err := ctx.Err()
		if err == nil {
			err = rc.SetWriteDeadline(time.Now().Add(idle))
		}
		deadlineMu.Unlock()
		if err != nil {
			return err
		}
		part := event[:min(len(event), piece)]
		start := time.Now()
		n, err := w.Write(part)
		if err != nil {
			return err
		}
		if n != len(part) {
			return io.ErrShortWrite
		}
		if err = rc.Flush(); err != nil {
			return err
		}
		event = event[n:]
		// Grow the piece while the link keeps well ahead of the idle
		// deadline, shrink it when it does not.
		if el := time.Since(start); el < idle/8 {
			piece = min(piece*2, ssePieceMax)
		} else if el > idle/4 {
			piece = max(piece/2, ssePieceMin)
		}
	}
	return nil
}

// An SSE event is written and flushed in pieces that start at ssePieceMin
// and adapt to the link up to ssePieceMax: progress stays visible within the
// idle timeout on a slow link, and a fast link does not pay ~1400 flushes
// and deadline updates per 1 MiB frame.
const (
	ssePieceMin = 1 << 10
	ssePieceMax = 64 << 10
)

// upBodyTimeout bounds reading one /up body: a fixed 5 s plus 16 KiB/s of the
// declared length, capped by up_request_timeout, so a trickled body cannot
// hold its codec reservation for the whole request timeout.
func (s *Server) upBodyTimeout(length int) time.Duration {
	limit := s.cfg.Transport.UpRequestTimeout
	if limit <= 0 {
		limit = 30 * time.Second
	}
	return min(limit, 5*time.Second+time.Duration(length)*time.Second/(16<<10))
}

// readExactBody reads exactly length bytes and fails if the body is shorter
// or longer.
func readExactBody(r io.Reader, length int) ([]byte, error) {
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	// Content-Length already equals length, so the body ends here; one
	// probe read confirms it like the WS reader does.
	var extra [1]byte
	if n, err := r.Read(extra[:]); n != 0 {
		return nil, io.ErrUnexpectedEOF
	} else if err != nil && err != io.EOF {
		return nil, err
	}
	return body, nil
}
