package egress

import (
	"context"
	"crypto/sha256"
	"dbbridge/internal/auth"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"dbbridge/internal/transport"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

var wsUpgrader = websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096, Subprotocols: []string{protocol.SecWebSocketProtocol}, HandshakeTimeout: 5 * time.Second, CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == "" }}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		methodNotAllowed(w, r, "GET")
		return
	}
	peer, ok := s.peer(w, r)
	if !ok || !s.instance(w, r) {
		return
	}
	if r.URL.RawQuery != "" {
		writeJSONError(w, 400, protocol.ErrBadFrame, "WS query is forbidden", false, false)
		return
	}
	found := false
	for _, v := range websocket.Subprotocols(r) {
		if v == protocol.SecWebSocketProtocol {
			found = true
		}
	}
	if !found || !s.allows("ws") {
		writeJSONError(w, 409, protocol.ErrTransportUnsupported, "subprotocol or transport", false, false)
		return
	}
	purpose := r.Header.Get(protocol.HeaderWSPurpose)
	if purpose == "check" {
		// Prechecks share the global probe budget with HTTP probes, so they
		// take the same per-peer share of it.
		fairRelease, fair := s.auth.ReserveFair(peer, "probe", s.cfg.Resources.Probes.Limit())
		if !fair {
			w.Header().Set("Retry-After", "1")
			writeJSONError(w, 429, protocol.ErrQuotaExceeded, "peer probe budget", false, true)
			return
		}
		defer fairRelease()
		if !s.cfg.Resources.Probes.Try(1) {
			w.Header().Set("Retry-After", "1")
			writeJSONError(w, 429, protocol.ErrQuotaExceeded, "probe budget", false, true)
			return
		}
		defer s.cfg.Resources.Probes.Put(1)
		release, admitted := s.auth.ReservePrecheck(peer)
		if !admitted {
			w.Header().Set("Retry-After", "1")
			writeJSONError(w, 429, protocol.ErrQuotaExceeded, "peer precheck budget", false, true)
			return
		}
		defer release()
		nonce := r.Header.Get(protocol.HeaderWSNonce)
		if nonce == "" || len(nonce) > 128 {
			writeJSONError(w, 400, protocol.ErrBadFrame, "nonce", false, false)
			return
		}
		c, e := wsUpgrader.Upgrade(transport.WebSocketResponseWriter{ResponseWriter: w}, r, http.Header{"Cache-Control": []string{"no-store"}, protocol.HeaderInstanceID: []string{s.cfg.Node.InstanceID}})
		if e != nil {
			return
		}
		s.connections.Store(c, true)
		defer s.connections.Delete(c)
		defer c.Close()
		c.SetReadLimit(16 << 10)
		c.SetWriteDeadline(time.Now().Add(time.Second))
		_ = c.WriteJSON(protocol.WSHelloMessage{Type: "hello", ProtocolVersion: protocol.ProtocolVersionString, NodeID: s.cfg.Node.ID, InstanceID: s.cfg.Node.InstanceID, Nonce: nonce, Transport: "ws"})
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		_, _, _ = c.ReadMessage()
		return
	}
	if purpose != "session" {
		writeJSONError(w, 400, protocol.ErrBadFrame, "purpose", false, false)
		return
	}
	// GetAny, like the HTTP session routes: a finished session is answered
	// with its terminal code instead of looking unknown.
	sess := s.registry.GetAny(r.Header.Get(protocol.HeaderWSSessionID))
	if sess == nil || sess.OwnerPeer != peer {
		writeJSONError(w, 404, protocol.ErrSessionNotFound, "not found", true, false)
		return
	}
	if !auth.ValidateSessionToken(sess.SessionToken, r.Header.Get(protocol.HeaderSessionToken)) {
		writeJSONError(w, 401, protocol.ErrUnauthorized, "session", false, false)
		return
	}
	if s.auth.AuthorizeTarget(peer, sess.TargetID) != nil {
		sess.CloseWithReason(protocol.ErrTargetForbidden)
		writeJSONError(w, 403, protocol.ErrTargetForbidden, "target", true, false)
		return
	}
	if !s.bindCred(peer, credential(r)) {
		unauthorized(w, r)
		return
	}
	if sess.GetState() == session.StateClosed {
		s.writeSessionError(w, sess, session.ErrSessionClosed)
		return
	}
	// A request that cannot be upgraded is refused before the stream is
	// reserved, so it never suspends or closes the session.
	if !validWSHandshake(r) {
		writeJSONError(w, 400, protocol.ErrBadFrame, "not a WebSocket upgrade", false, false)
		return
	}
	ep, e := protocol.Offset(r.Header.Get(protocol.HeaderWSEpoch))
	if e != nil || sess.GetTransportMode() != "ws" {
		writeJSONError(w, 409, protocol.ErrTransportMismatch, "mode", false, false)
		return
	}
	stream, e := sess.ReserveStream(ep, "ws")
	if e != nil {
		s.writeSessionError(w, sess, e)
		return
	}
	c, e := wsUpgrader.Upgrade(transport.WebSocketResponseWriter{ResponseWriter: w}, r, http.Header{"Cache-Control": []string{"no-store"}, protocol.HeaderInstanceID: []string{s.cfg.Node.InstanceID}})
	if e != nil {
		sess.DropTransport(ep)
		return
	}
	s.connections.Store(c, true)
	defer s.connections.Delete(c)
	releaseAdmission(r)
	// The hijacked connection keeps the request read deadline; a session
	// stream is bounded by heartbeats and backpressure instead.
	_ = c.SetReadDeadline(time.Time{})
	s.runWSSession(c, sess, ep, stream)
}

// validWSHandshake applies every request check the WebSocket upgrader makes
// (RFC 6455 4.2.1): Connection/Upgrade tokens, version 13 and a 16-byte
// base64 Sec-WebSocket-Key. Origin and the subprotocol are checked earlier.
func validWSHandshake(r *http.Request) bool {
	if !websocket.IsWebSocketUpgrade(r) || r.Header.Get("Sec-Websocket-Version") != "13" {
		return false
	}
	key, err := base64.StdEncoding.DecodeString(r.Header.Get("Sec-Websocket-Key"))
	return err == nil && len(key) == 16
}

func (s *Server) runWSSession(c *websocket.Conn, sess *session.Session, ep uint64, stream string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer c.Close()
	writerDone := make(chan struct{})
	if err := sess.SetStreamCancel(ep, stream, streamCancel(sess, cancel, writerDone)); err != nil {
		if !errors.Is(err, session.ErrSessionClosed) || sess.GetEpoch() != ep || sess.CurrentStreamID() != stream {
			return
		}
	}
	defer sess.DropTransport(ep)
	s.wg.Add(2) // called from a counted handler
	go func() { defer s.wg.Done(); <-ctx.Done(); c.Close() }()
	go func() { defer s.wg.Done(); s.watchControl(ctx, sess, ep, cancel) }()
	maxFrame := sess.NegotiatedMaxFrameSize()
	c.SetReadLimit(int64(max(16<<10, maxFrame+protocol.WSBinaryHeaderLen)))
	write := func(typ int, b []byte) error {
		d := s.cfg.Transport.WS.WriteIdleTimeout
		if d <= 0 {
			d = 60 * time.Second
		}
		return transport.WriteWSMessage(c, typ, b, d)
	}
	b, _ := json.Marshal(s.ready(sess, ep, stream, "ws"))
	if write(websocket.TextMessage, b) != nil {
		close(writerDone)
		return
	}
	go func() {
		defer close(writerDone)
		_ = s.streamWriter(ctx, sess, ep, stream, func(kind string, data []byte, v any, end uint64) error {
			if kind == "data" {
				frame, err := protocol.EncodeWSFrameChecked(ep, end-uint64(len(data)), data)
				if err != nil {
					return err
				}
				return write(websocket.BinaryMessage, frame)
			}
			b, e := json.Marshal(v)
			if e != nil {
				return e
			}
			return write(websocket.TextMessage, b)
		})
		// A session this node terminated is announced with a bound error
		// message and a Close frame before the carrier goes away, so
		// the ingress ends the local session instead of treating the
		// carrier loss as a reconnectable transport fault.
		if notice, ok := s.terminalNotice(sess, ep, stream); ok {
			if b, e := json.Marshal(notice); e == nil && transport.WriteWSMessage(c, websocket.TextMessage, b, terminalNoticeTimeout) == nil {
				_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, notice.Code), time.Now().Add(terminalNoticeTimeout))
			}
		}
		cancel()
	}()
	defer func() {
		// Give the writer the chance to deliver the terminal notice of a
		// session that ended while this loop ran.
		if sess.GetState() == session.StateClosed {
			wait := time.NewTimer(terminalNoticeGrace)
			select {
			case <-writerDone:
			case <-wait.C:
			}
			wait.Stop()
		}
		cancel()
		c.Close()
		<-writerDone
	}()
	codec := s.codecFor(sess.OwnerPeer)
	// One message body must arrive within a deadline derived from the
	// largest frame, counted from its first byte and paused while the reader
	// waits for budget: a trickled body cannot hold its codec and payload
	// reservations indefinitely. Expiry drops the carrier.
	bodyLimit := s.upBodyTimeout(maxFrame + protocol.WSBinaryHeaderLen)
	body := time.AfterFunc(time.Hour, cancel)
	body.Stop()
	defer body.Stop()
	armed := false
	progress := func() {
		if !armed {
			armed = true
			body.Reset(bodyLimit)
		}
	}
	var retry *time.Timer
	defer func() {
		if retry != nil {
			retry.Stop()
		}
	}()
	for {
		stall := s.cfg.Transport.BackpressureTimeout
		if stall <= 0 {
			stall = 120 * time.Second
		}
		armed = false
		typ, b, release, releasePayload, e := transport.ReadWSMessageWithPayload(ctx, c, codec, s.cfg.Resources.Payload, maxFrame, stall, progress, func(blocked bool) {
			if blocked {
				body.Stop()
			} else if armed {
				body.Reset(bodyLimit)
			}
			sess.SetTransportRxBlocked(ep, stream, blocked)
		})
		body.Stop()
		if e != nil {
			if errors.Is(e, transport.ErrBackpressure) {
				sess.CloseStreamWithReason(ep, stream, protocol.ErrBufferExhausted)
			} else if errors.Is(e, transport.ErrProtocol) || errors.Is(e, websocket.ErrReadLimit) {
				sess.CloseStream(ep, stream)
			}
			return
		}
		releaseAll := func() {
			release()
			if releasePayload != nil {
				releasePayload()
			}
		}
		if typ != websocket.BinaryMessage {
			releaseAll()
			sess.CloseStream(ep, stream)
			return
		}
		fep, off, data, e := protocol.DecodeWSFrame(b)
		if e != nil || fep != ep || sess.CheckStream(ep, stream, true) != nil {
			releaseAll()
			sess.CloseStream(ep, stream)
			return
		}
		sum := sha256.Sum256(data)
		until := time.Now().Add(stall)
		for {
			var consumed bool
			_, _, _, consumed, e = sess.IngestUpFrameReserved(ep, off, len(data), hex.EncodeToString(sum[:]), data)
			if consumed {
				releasePayload = nil
			}
			if !errors.Is(e, session.ErrWindowExceeded) {
				break
			}
			if time.Now().After(until) {
				break
			}
			// One reusable timer instead of a time.After allocation per
			// 5 ms retry.
			if retry == nil {
				retry = time.NewTimer(5 * time.Millisecond)
			} else {
				retry.Reset(5 * time.Millisecond)
			}
			select {
			case <-ctx.Done():
				releaseAll()
				return
			case <-retry.C:
			}
		}
		releaseAll()
		if e != nil {
			if errors.Is(e, session.ErrWindowExceeded) {
				sess.CloseStreamWithReason(ep, stream, protocol.ErrBufferExhausted)
			} else {
				sess.CloseStream(ep, stream)
			}
			return
		}
	}
}
