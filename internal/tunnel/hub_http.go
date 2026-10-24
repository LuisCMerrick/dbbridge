package tunnel

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"dbbridge/internal/protocol"

	"github.com/xtaci/smux"
)

// Sizes of the HTTP carrier. pipeCapacity bounds the bytes a side keeps
// unacknowledged (outbound) and unread (inbound); both are charged to the
// codec budget with the smux receive buffer. One POST carries at most
// maxUpBody bytes, one poll response at most maxPollBody and one SSE event
// at most sseChunk bytes before base64.
const (
	pipeCapacity = 512 << 10
	maxUpBody    = 128 << 10
	maxPollBody  = 256 << 10
	sseChunk     = 32 << 10
	// sseMaxLife ends an SSE response after a while, so intermediaries never
	// see an endless response; the database side reconnects at once and
	// continues from what it received.
	sseMaxLife = 5 * time.Minute
	// defaultPollWait is how long a poll waits for data before it answers
	// 204 and the database side polls again.
	defaultPollWait = 25 * time.Second
	// upReceiveWait bounds how long a POST waits for room in the receive
	// buffer before it answers 503 and the database side sends it again.
	upReceiveWait = 10 * time.Second
)

// httpTunnelBudget is what an HTTP-carried tunnel charges to the codec
// budget on each side: the smux receive buffer plus both pipe buffers.
func httpTunnelBudget(rb int64) int64 { return rb + 2*pipeCapacity }

// serveHTTP handles /v1/reverse/{tunnel}[/action] for tunnels carried over
// plain HTTP requests.
func (h *Hub) serveHTTP(w http.ResponseWriter, r *http.Request, authenticated func()) {
	rest := strings.TrimPrefix(r.URL.Path, protocol.ReverseTunnelPath+"/")
	id, action, _ := strings.Cut(rest, "/")
	if action != "" {
		action = "/" + action
	}
	if !tunnelIDPattern.MatchString(id) {
		h.reject(w, r, nil, refuse(404, protocol.ErrBadFrame, "not found", false))
		return
	}
	if action == protocol.ReverseOpenSuffix {
		h.openHTTP(w, r, id, authenticated)
		return
	}
	t, herr := h.lookup(r, id, authenticated)
	if herr != nil {
		h.reject(w, r, nil, herr)
		return
	}
	want := map[string]string{"": http.MethodDelete, protocol.ReverseUpSuffix: http.MethodPost, protocol.ReverseEventsSuffix: http.MethodGet, protocol.ReversePollSuffix: http.MethodGet}
	method, known := want[action]
	if !known {
		h.reject(w, r, nil, refuse(404, protocol.ErrBadFrame, "not found", false))
		return
	}
	if r.Method != method {
		w.Header().Set("Allow", method)
		h.reject(w, r, nil, refuse(405, protocol.ErrBadFrame, "method", false))
		return
	}
	if action == "" {
		slog.Info("reverse tunnel closed by its database side", "peer", t.Peer, "tunnel", t.ID)
		t.close()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if v := r.Header.Get(protocol.HeaderAckDownOffset); v != "" {
		off, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			err = t.pipe.ack(off)
		}
		if err != nil {
			slog.Warn("reverse tunnel closed: invalid acknowledgement", "peer", t.Peer, "tunnel", t.ID, "err", err)
			t.close()
			h.reject(w, r, nil, refuse(409, protocol.ErrBadAck, "acknowledgement", false))
			return
		}
	}
	switch action {
	case protocol.ReverseUpSuffix:
		h.upHTTP(w, r, t)
	case protocol.ReverseEventsSuffix:
		h.eventsHTTP(w, r, t)
	case protocol.ReversePollSuffix:
		h.pollHTTP(w, r, t)
	}
}

// lookup authenticates a request for an open HTTP-carried tunnel: the token
// must be the one the tunnel was opened with and the instance the one that
// opened it.
func (h *Hub) lookup(r *http.Request, id string, authenticated func()) (*Tunnel, *handshakeError) {
	peer, digest, herr := h.authenticate(r)
	if herr != nil {
		return nil, herr
	}
	if authenticated != nil {
		authenticated()
	}
	h.mu.Lock()
	t := peer.tunnels[id]
	h.mu.Unlock()
	if t == nil || t.pipe == nil || t.sess.IsClosed() {
		return nil, refuse(404, protocol.ErrTunnelNotFound, "tunnel not found", false)
	}
	if subtle.ConstantTimeCompare(t.digest[:], digest[:]) != 1 {
		return nil, refuse(401, protocol.ErrUnauthorized, "unauthorized", false)
	}
	if r.Header.Get(protocol.HeaderInstanceID) != t.Instance {
		return nil, refuse(404, protocol.ErrTunnelNotFound, "tunnel not found", false)
	}
	return t, nil
}

// openHTTP handles POST /v1/reverse/{tunnel}/open.
func (h *Hub) openHTTP(w http.ResponseWriter, r *http.Request, id string, authenticated func()) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		h.reject(w, r, nil, refuse(405, protocol.ErrBadFrame, "method", false))
		return
	}
	q, herr := h.verifyIdentity(r)
	if q != nil && authenticated != nil {
		authenticated()
	}
	if herr == nil {
		switch v := r.Header.Get(protocol.HeaderReverseProtocol); {
		case v == protocol.ReverseSubprotocol:
		case strings.HasPrefix(v, protocol.ReverseSubprotocolPrefix):
			herr = refuse(409, protocol.ErrProtocolMismatch, "protocol", false)
		default:
			herr = refuse(409, protocol.ErrTransportUnsupported, "reverse protocol", false)
		}
	}
	if herr == nil && q.id != id {
		herr = refuse(400, protocol.ErrBadFrame, "tunnel identity", false)
	}
	transportName := r.Header.Get(protocol.HeaderTunnelTransport)
	if herr == nil && transportName != "http-sse" && transportName != "long-poll" {
		herr = refuse(400, protocol.ErrBadFrame, "tunnel transport", false)
	}
	if herr == nil {
		h.mu.Lock()
		herr = h.checkConflict(q)
		h.mu.Unlock()
	}
	var release func()
	if herr == nil {
		need := httpTunnelBudget(ReceiveBuffer(h.tcfg))
		if h.codec != nil {
			if !h.codec.Try(need) {
				herr = refuse(503, protocol.ErrBufferExhausted, "codec budget", true)
			} else {
				release = func() { h.codec.Put(need) }
			}
		}
	}
	if herr != nil {
		h.reject(w, r, q, herr)
		return
	}
	p := newPipe(pipeCapacity, Addr("hub"), Addr("tunnel/"+q.id))
	sess, err := smux.Client(p, smuxConfig(h.tcfg))
	if err != nil {
		_ = p.Close()
		if release != nil {
			release()
		}
		h.reject(w, r, q, refuse(500, protocol.ErrInternal, "multiplexer", true))
		return
	}
	t := &Tunnel{ID: q.id, Peer: q.peer.id, Instance: q.instance, Priority: q.priority, Remote: q.remote, Transport: transportName, digest: q.digest, sess: sess, conn: p, pipe: p, release: release}
	if herr = h.register(q, t); herr != nil {
		h.reject(w, r, q, herr)
		return
	}
	w.Header().Set(protocol.HeaderNodeID, h.nodeID)
	w.Header().Set(protocol.HeaderInstanceID, h.instanceID)
	w.Header().Set(protocol.HeaderReverseProtocol, protocol.ReverseSubprotocol)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
}

// upHTTP handles POST /v1/reverse/{tunnel}/up: bytes from the database side
// at X-DBB-Offset, verified by X-DBB-SHA256. Retransmissions are accepted
// and skipped; the answer carries the offset of the next byte expected.
func (h *Hub) upHTTP(w http.ResponseWriter, r *http.Request, t *Tunnel) {
	off, err := strconv.ParseInt(r.Header.Get(protocol.HeaderOffset), 10, 64)
	if err != nil || off < 0 {
		h.reject(w, r, nil, refuse(400, protocol.ErrBadFrame, "offset", false))
		return
	}
	want, err := hex.DecodeString(r.Header.Get(protocol.HeaderSHA256))
	if err != nil || len(want) != sha256.Size {
		h.reject(w, r, nil, refuse(400, protocol.ErrBadFrame, "digest", false))
		return
	}
	if r.ContentLength > maxUpBody {
		h.reject(w, r, nil, refuse(413, protocol.ErrFrameTooLarge, "body", false))
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxUpBody+1))
	if err != nil {
		return
	}
	if len(body) > maxUpBody {
		h.reject(w, r, nil, refuse(413, protocol.ErrFrameTooLarge, "body", false))
		return
	}
	if got := sha256.Sum256(body); subtle.ConstantTimeCompare(got[:], want) != 1 {
		h.reject(w, r, nil, refuse(400, protocol.ErrBadDigest, "digest", true))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), upReceiveWait)
	defer cancel()
	next, err := t.pipe.receive(ctx, off, body)
	w.Header().Set(protocol.HeaderConfirmUpOffset, strconv.FormatInt(next, 10))
	switch {
	case err == nil:
		w.WriteHeader(http.StatusOK)
	case errors.Is(err, errPipeGap):
		slog.Warn("reverse tunnel closed: offset gap", "peer", t.Peer, "tunnel", t.ID, "offset", off, "expected", next)
		t.close()
		h.reject(w, r, nil, refuse(409, protocol.ErrOffsetGap, "offset gap", false))
	case ctx.Err() != nil && !t.pipe.isClosed():
		h.reject(w, r, nil, refuse(503, protocol.ErrBufferExhausted, "receive buffer full", true))
	default:
		h.reject(w, r, nil, refuse(404, protocol.ErrTunnelNotFound, "tunnel closed", false))
	}
}

// downStart reads the offset the database side has received, which is where
// its next bytes start.
func downStart(r *http.Request) (int64, bool) {
	off, err := strconv.ParseInt(r.Header.Get(protocol.HeaderAckDownOffset), 10, 64)
	return off, err == nil && off >= 0
}

func (h *Hub) writeIdle() time.Duration {
	if d := h.tcfg.SSEWriteIdleTimeout; d > 0 {
		return d
	}
	return 30 * time.Second
}

// eventsHTTP handles GET /v1/reverse/{tunnel}/events: an SSE stream of the
// public side's bytes. It opens with a "ready" event, so the database side
// can tell a buffering intermediary from a quiet tunnel; then each event's id
// is the offset of the bytes its data (base64) carries.
func (h *Hub) eventsHTTP(w http.ResponseWriter, r *http.Request, t *Tunnel) {
	from, ok := downStart(r)
	if !ok {
		h.reject(w, r, nil, refuse(400, protocol.ErrBadFrame, "offset", false))
		return
	}
	ctx, cancel := t.takeReader(r.Context(), sseMaxLife)
	defer cancel()
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})
	idle := h.writeIdle()
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	write := func(s string) bool {
		_ = rc.SetWriteDeadline(time.Now().Add(idle))
		if _, err := io.WriteString(w, s); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	if !write("event: ready\ndata: " + strconv.FormatInt(from, 10) + "\n\n") {
		return
	}
	for {
		data, off := t.pipe.pending(ctx, from, sseChunk)
		if len(data) == 0 {
			return
		}
		if !write("id: " + strconv.FormatInt(off, 10) + "\ndata: " + base64.StdEncoding.EncodeToString(data) + "\n\n") {
			return
		}
		from = off + int64(len(data))
	}
}

// pollHTTP handles GET /v1/reverse/{tunnel}/poll: the public side's bytes
// from the offset the database side received, as soon as there are any, or
// 204 after the poll wait.
func (h *Hub) pollHTTP(w http.ResponseWriter, r *http.Request, t *Tunnel) {
	from, ok := downStart(r)
	if !ok {
		h.reject(w, r, nil, refuse(400, protocol.ErrBadFrame, "offset", false))
		return
	}
	wait := h.pollWait
	if wait <= 0 {
		wait = defaultPollWait
	}
	ctx, cancel := t.takeReader(r.Context(), wait)
	defer cancel()
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})
	data, off := t.pipe.pending(ctx, from, maxPollBody)
	if len(data) == 0 && t.pipe.isClosed() {
		h.reject(w, r, nil, refuse(404, protocol.ErrTunnelNotFound, "tunnel closed", false))
		return
	}
	_ = rc.SetWriteDeadline(time.Now().Add(h.writeIdle()))
	w.Header().Set(protocol.HeaderOffset, strconv.FormatInt(off, 10))
	if len(data) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
