package tunnel

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
)

// Transport names of reverse tunnels, in auto's fallback order.
const (
	modeWS   = "ws"
	modeSSE  = "http-sse"
	modePoll = "long-poll"
)

// refusedError is a tunnel request the public side, or something in front
// of it, answered with an HTTP status. code is the DBBridge error code of
// the body, empty when the answer did not come from DBBridge.
type refusedError struct {
	status int
	code   string
}

func (e *refusedError) Error() string {
	if e.code == "" {
		return fmt.Sprintf("tunnel handshake refused: HTTP %d", e.status)
	}
	return fmt.Sprintf("tunnel handshake refused: HTTP %d %s", e.status, e.code)
}

// errSSEBuffered: the SSE stream's first event did not arrive in time, so
// something between the nodes buffers the response.
var errSSEBuffered = errors.New("SSE stream buffered: the ready event did not arrive in time")

// errCarrierFatal wraps an HTTP carrier failure that ends the tunnel at once
// instead of being retried.
type errCarrierFatal struct{ err error }

func (e errCarrierFatal) Error() string { return e.err.Error() }
func (e errCarrierFatal) Unwrap() error { return e.err }

// canceledCtx makes pipe.pending return without waiting.
var canceledCtx = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

func refusal(resp *http.Response) *refusedError {
	e := &refusedError{status: resp.StatusCode}
	var body protocol.ErrorResponse
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") && protocol.DecodeJSON(resp.Body, 32<<10, &body) == nil {
		e.code = body.Code
	}
	return e
}

// fatalRefusal reports whether an answer ends the tunnel rather than being
// retried: it is gone, the token is no longer accepted or the request was
// refused as a violation.
func fatalRefusal(e *refusedError) bool {
	switch e.code {
	case protocol.ErrTunnelNotFound, protocol.ErrUnauthorized, protocol.ErrNodeMismatch, protocol.ErrBadAck, protocol.ErrOffsetGap, protocol.ErrBadFrame, protocol.ErrFrameTooLarge:
		return true
	}
	return false
}

// httpClient returns the HTTP client the HTTP carriers share.
func (c *Client) httpClient() *http.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hc != nil {
		return c.hc
	}
	nd := &net.Dialer{Timeout: c.cfg.Transport.PeerDial(), KeepAlive: 30 * time.Second}
	tr := &http.Transport{
		TLSClientConfig:     c.tlsConfig.Clone(),
		MaxIdleConnsPerHost: 4 * max(c.cfg.Reverse.TunnelsPerEndpoint, 1),
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: c.cfg.Transport.PeerDial(),
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := nd.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return transport.NewIdleWriteConn(conn), nil
		},
	}
	if c.cfg.Transport.UseEnvironmentProxy {
		tr.Proxy = http.ProxyFromEnvironment
	}
	c.hc = &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return c.hc
}

// httpCarrier is the database side of a tunnel carried over HTTP requests:
// an upload loop that POSTs this side's bytes and a download loop that
// reads the public side's bytes from an SSE stream or by polling.
type httpCarrier struct {
	c      *Client
	hc     *http.Client
	id     string
	mode   string
	base   string
	token  string
	p      *pipe
	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once
	// readySent is set once the first SSE stream delivered its ready event
	// (download loop only).
	readySent bool
}

func (h *httpCarrier) request(ctx context.Context, method, suffix string, body []byte) (*http.Request, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, h.base+suffix, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set(protocol.HeaderPeerAuth, "Bearer "+h.token)
	req.Header.Set(protocol.HeaderInstanceID, h.c.cfg.Node.InstanceID)
	req.Header.Set("Cache-Control", "no-store")
	return req, nil
}

// dialHTTP opens tunnel id to ep over HTTP requests (mode http-sse or
// long-poll) and returns its byte stream.
func (c *Client) dialHTTP(ep config.EndpointConfig, id, mode string) (net.Conn, error) {
	token, terr := c.cfg.ReverseToken()
	if terr != nil {
		slog.Warn("reverse token source unreadable; using the token loaded at startup", "err", terr)
	}
	hs := c.cfg.Transport.WS.HandshakeTimeout
	if hs <= 0 {
		hs = 10 * time.Second
	}
	h := &httpCarrier{c: c, hc: c.httpClient(), id: id, mode: mode, token: token, base: strings.TrimSuffix(ep.URL, "/") + protocol.ReverseTunnelPath + "/" + id}
	ctx, cancel := context.WithTimeout(c.ctx, c.cfg.Transport.PeerDial()+hs)
	defer cancel()
	req, err := h.request(ctx, http.MethodPost, protocol.ReverseOpenSuffix, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(protocol.HeaderNodeID, c.cfg.Node.ID)
	req.Header.Set(protocol.HeaderExpectedNodeID, c.cfg.Reverse.PeerNodeID)
	req.Header.Set(protocol.HeaderTunnelID, id)
	req.Header.Set(protocol.HeaderTunnelPriority, strconv.Itoa(ep.Priority))
	req.Header.Set(protocol.HeaderReverseProtocol, protocol.ReverseSubprotocol)
	req.Header.Set(protocol.HeaderTunnelTransport, mode)
	resp, err := h.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, refusal(resp)
	}
	if got := resp.Header.Get(protocol.HeaderNodeID); got != c.cfg.Reverse.PeerNodeID {
		if got == "" {
			// Not an answer from DBBridge (an intermediary answered).
			return nil, &refusedError{status: resp.StatusCode}
		}
		return nil, fmt.Errorf("tunnel peer node mismatch: expected %q, got %q", c.cfg.Reverse.PeerNodeID, got)
	}
	if resp.Header.Get(protocol.HeaderReverseProtocol) != protocol.ReverseSubprotocol || resp.Header.Get(protocol.HeaderInstanceID) == "" {
		return nil, fmt.Errorf("tunnel handshake: protocol mismatch (%q)", resp.Header.Get(protocol.HeaderReverseProtocol))
	}
	h.p = newPipe(pipeCapacity, Addr("tunnel"), Addr("tunnel/"+id))
	h.ctx, h.cancel = context.WithCancel(c.ctx)
	h.p.onClose = h.shutdown
	c.wg.Add(2)
	go func() { defer c.wg.Done(); h.upLoop() }()
	if mode == modePoll {
		go func() { defer c.wg.Done(); h.pollLoop() }()
		return h.p, nil
	}
	ready := make(chan error, 1)
	go func() { defer c.wg.Done(); h.sseLoop(ready) }()
	t := time.NewTimer(hs)
	defer t.Stop()
	select {
	case err = <-ready:
	case <-t.C:
		err = errSSEBuffered
	case <-c.ctx.Done():
		err = c.ctx.Err()
	}
	if err != nil {
		h.p.closeWith(err)
		return nil, err
	}
	return h.p, nil
}

// shutdown runs once when the pipe closes: it stops both loops and tells
// the public side, unless the public side already dropped the tunnel.
func (h *httpCarrier) shutdown() {
	h.once.Do(func() {
		h.cancel()
		var re *refusedError
		if errors.As(h.p.err(), &re) && re.code == protocol.ErrTunnelNotFound {
			return
		}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			req, err := h.request(ctx, http.MethodDelete, "", nil)
			if err != nil {
				return
			}
			if resp, err := h.hc.Do(req); err == nil {
				_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
				resp.Body.Close()
			}
		}()
	})
}

// fail ends the tunnel when err is fatal and otherwise waits before the
// next attempt. It reports whether the loop should go on.
func (h *httpCarrier) retry(err error, failures int) bool {
	if h.ctx.Err() != nil {
		return false
	}
	var re *refusedError
	var fe errCarrierFatal
	if errors.As(err, &fe) || errors.As(err, &re) && fatalRefusal(re) {
		slog.Warn("reverse tunnel carrier failed", "tunnel", h.id, "transport", h.mode, "err", err)
		h.p.closeWith(err)
		return false
	}
	d := 50 * time.Millisecond << min(failures, 5)
	t := time.NewTimer(min(d, time.Second))
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-h.ctx.Done():
		return false
	}
}

// upLoop sends this side's bytes, and acknowledgements of the bytes
// received, one POST at a time. A POST that fails is sent again from the
// acknowledged offset; the public side skips what it already has.
func (h *httpCarrier) upLoop() {
	acked := int64(0) // receive offset last acknowledged to the public side
	failures := 0
	for h.ctx.Err() == nil {
		changed := h.p.wait()
		data, off := h.p.pending(canceledCtx, 0, maxUpBody)
		recv := h.p.received()
		if len(data) == 0 {
			if recv == acked || h.mode == modePoll {
				// Nothing to send (polls carry the acknowledgement).
				acked = recv
				select {
				case <-changed:
				case <-h.ctx.Done():
				}
				continue
			}
			// Acknowledgement only: let more bytes arrive first unless
			// a large share of the buffer waits for it.
			if recv-acked < pipeCapacity/4 {
				t := time.NewTimer(20 * time.Millisecond)
				select {
				case <-t.C:
				case <-h.ctx.Done():
				}
				t.Stop()
				data, off = h.p.pending(canceledCtx, 0, maxUpBody)
				recv = h.p.received()
			}
		}
		if err := h.up(data, off, recv); err != nil {
			failures++
			if !h.retry(err, failures) {
				return
			}
			continue
		}
		failures = 0
		acked = max(acked, recv)
	}
}

func (h *httpCarrier) up(data []byte, off, recv int64) error {
	ctx, cancel := context.WithTimeout(h.ctx, upReceiveWait+h.c.cfg.Transport.PeerDial()+10*time.Second)
	defer cancel()
	if data == nil {
		data = []byte{}
	}
	req, err := h.request(ctx, http.MethodPost, protocol.ReverseUpSuffix, data)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(protocol.HeaderOffset, strconv.FormatInt(off, 10))
	req.Header.Set(protocol.HeaderSHA256, hex.EncodeToString(sum[:]))
	req.Header.Set(protocol.HeaderAckDownOffset, strconv.FormatInt(recv, 10))
	resp, err := h.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return refusal(resp)
	}
	confirm, err := strconv.ParseInt(resp.Header.Get(protocol.HeaderConfirmUpOffset), 10, 64)
	if err != nil {
		return errCarrierFatal{errors.New("tunnel upload answer without a confirmed offset")}
	}
	if err := h.p.ack(confirm); err != nil {
		return errCarrierFatal{err}
	}
	return nil
}

// pollLoop reads the public side's bytes by long polling.
func (h *httpCarrier) pollLoop() {
	failures := 0
	for h.ctx.Err() == nil {
		if err := h.poll(); err != nil {
			failures++
			if !h.retry(err, failures) {
				return
			}
			continue
		}
		failures = 0
	}
}

func (h *httpCarrier) poll() error {
	ctx, cancel := context.WithTimeout(h.ctx, defaultPollWait+h.c.cfg.Transport.PeerDial()+30*time.Second)
	defer cancel()
	req, err := h.request(ctx, http.MethodGet, protocol.ReversePollSuffix, nil)
	if err != nil {
		return err
	}
	recv := h.p.received()
	req.Header.Set(protocol.HeaderAckDownOffset, strconv.FormatInt(recv, 10))
	resp, err := h.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusOK:
	default:
		return refusal(resp)
	}
	off, err := strconv.ParseInt(resp.Header.Get(protocol.HeaderOffset), 10, 64)
	if err != nil || off < 0 {
		return errCarrierFatal{errors.New("tunnel poll answer without an offset")}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPollBody+1))
	if err != nil {
		return err
	}
	if len(body) > maxPollBody {
		return errCarrierFatal{errors.New("tunnel poll answer too large")}
	}
	if _, err := h.p.receive(h.ctx, off, body); err != nil {
		if errors.Is(err, errPipeGap) {
			return errCarrierFatal{err}
		}
		return err
	}
	return nil
}

// sseLoop reads the public side's bytes from SSE streams, reconnecting from
// the received offset whenever one ends. ready receives the outcome of the
// first stream's ready event.
func (h *httpCarrier) sseLoop(ready chan<- error) {
	failures := 0
	for h.ctx.Err() == nil {
		err := h.sse(ready)
		if ready != nil {
			if !h.readySent {
				ready <- err
				return
			}
			ready = nil
		}
		if err == nil {
			failures = 0
			continue
		}
		failures++
		if !h.retry(err, failures) {
			return
		}
	}
}

// sseLineLimit bounds one SSE line: a data line of sseChunk bytes in base64.
const sseLineLimit = sseChunk*4/3 + 64

func (h *httpCarrier) sse(ready chan<- error) error {
	req, err := h.request(h.ctx, http.MethodGet, protocol.ReverseEventsSuffix, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set(protocol.HeaderAckDownOffset, strconv.FormatInt(h.p.received(), 10))
	resp, err := h.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return refusal(resp)
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		return &refusedError{status: resp.StatusCode}
	}
	br := bufio.NewReaderSize(resp.Body, 16<<10)
	var event, id, data string
	readyRead := false
	for {
		line, err := readLine(br, sseLineLimit)
		if err != nil {
			if readyRead && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
				// The public side ended this stream; continue on a new one.
				return nil
			}
			return err
		}
		if line != "" {
			name, value, _ := strings.Cut(line, ":")
			value = strings.TrimPrefix(value, " ")
			switch name {
			case "event":
				event = value
			case "id":
				id = value
			case "data":
				data = value
			}
			continue
		}
		switch {
		case event == "ready":
			if !readyRead {
				readyRead = true
				if ready != nil && !h.readySent {
					h.readySent = true
					ready <- nil
				}
			}
		case event == "" && data != "":
			if !readyRead {
				return errCarrierFatal{errors.New("SSE data before the ready event")}
			}
			off, err := strconv.ParseInt(id, 10, 64)
			if err != nil || off < 0 {
				return errCarrierFatal{errors.New("SSE event without a valid offset")}
			}
			b, err := base64.StdEncoding.DecodeString(data)
			if err != nil {
				return errCarrierFatal{errors.New("SSE event with invalid base64")}
			}
			if _, err := h.p.receive(h.ctx, off, b); err != nil {
				if errors.Is(err, errPipeGap) {
					return errCarrierFatal{err}
				}
				return err
			}
		}
		event, id, data = "", "", ""
	}
}

// readLine reads one SSE line (without its line ending) of at most limit
// bytes.
func readLine(br *bufio.Reader, limit int) (string, error) {
	var buf []byte
	for {
		chunk, isPrefix, err := br.ReadLine()
		if err != nil {
			return "", err
		}
		if len(buf)+len(chunk) > limit {
			return "", errCarrierFatal{errors.New("SSE line too long")}
		}
		buf = append(buf, chunk...)
		if !isPrefix {
			return string(buf), nil
		}
	}
}
