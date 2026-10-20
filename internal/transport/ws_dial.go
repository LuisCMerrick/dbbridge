package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"dbbridge/internal/protocol"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// wsUnavailable classifies a precheck failure that carries no HTTP response.
// Only a genuine refusal of the WebSocket upgrade path becomes
// ErrWSUpgradeRejected: gorilla's bad-handshake error, an established
// connection closed/reset by the far side instead of answering the Upgrade
// (upgradeAborted), or, when upgraded is true, a WebSocket-layer failure
// (close frame, abnormal close, framing error) after the 101. TLS
// verification failures become ErrProtocol, and so do, after the 101, an
// oversized message or a WebSocket framing violation (illegal protocol data,
// DBB_WS_PROTOCOL_ERROR). Context cancellation or deadline
// (checked against ctx, the precheck context) is returned wrapping ctx.Err()
// so errors.Is(err, context.Canceled/DeadlineExceeded) holds; a wait for a
// local connection slot keeps ErrLocalConnBudget. Anything else
// (dial refused/unreachable, DNS, i/o timeout) is returned wrapped but
// unclassified.
func wsUnavailable(ctx context.Context, err error, upgraded bool) error {
	if err == nil {
		return nil
	}
	if e := wsTLSFailure(err); e != nil {
		return e
	}
	if errors.Is(err, ErrLocalConnBudget) {
		return fmt.Errorf("ws precheck: %w", err)
	}
	if ce := ctx.Err(); ce != nil {
		if errors.Is(err, ce) {
			return fmt.Errorf("ws precheck: %w", err)
		}
		return fmt.Errorf("ws precheck: %w (%v)", ce, err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("ws precheck: %w", err)
	}
	// A socket deadline derived from ctx can fire a moment before ctx.Err()
	// is set; report it as the context deadline it is.
	var ne net.Error
	if dl, ok := ctx.Deadline(); ok && errors.As(err, &ne) && ne.Timeout() && !time.Now().Before(dl.Add(-10*time.Millisecond)) {
		return fmt.Errorf("ws precheck: %w (%v)", context.DeadlineExceeded, err)
	}
	if errors.Is(err, websocket.ErrBadHandshake) || upgradeAborted(err) {
		return fmt.Errorf("%w: %v", ErrWSUpgradeRejected, err)
	}
	if upgraded {
		var closed *websocket.CloseError
		if errors.As(err, &closed) {
			// The far side closed (or the link dropped, 1006) before a hello:
			// like a reset, fallback is allowed after a fresh probe.
			return fmt.Errorf("%w: after upgrade: %v", ErrWSUpgradeRejected, err)
		}
		if errors.Is(err, websocket.ErrReadLimit) || strings.HasPrefix(err.Error(), "websocket: ") {
			return fmt.Errorf("%w: ws precheck illegal message after upgrade: %v", ErrProtocol, err)
		}
	}
	return fmt.Errorf("ws precheck: %w", err)
}

// upgradeAborted reports a connection that was established and then closed or
// reset by the far side instead of answering (a gateway killing Upgrade
// requests, since the HTTP probe on the same endpoint just succeeded). Dial
// failures (refused, unreachable, DNS) and timeouts are not included.
func upgradeAborted(err error) bool {
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return false
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET)
}

// buildWSURL converts an HTTP/HTTPS endpoint URL to WS/WSS URL with /v1/transport/ws path.
func buildWSURL(endpointURL string) string {
	base := strings.TrimRight(endpointURL, "/")
	if strings.HasPrefix(base, "https://") {
		return "wss://" + strings.TrimPrefix(base, "https://") + "/v1/transport/ws"
	}
	if strings.HasPrefix(base, "http://") {
		return "ws://" + strings.TrimPrefix(base, "http://") + "/v1/transport/ws"
	}
	return "ws://" + base + "/v1/transport/ws"
}

// PrecheckWS performs an authenticated, session-less WS precheck.
// It tests whether the endpoint and intermediate proxies correctly support WebSocket Upgrade.
func (c *TripleTransportClient) PrecheckWS(ctx context.Context, expectedNodeID, expectedInstanceID string) error {
	ctx, cancel := context.WithTimeout(ctx, c.wsHandshakeTimeout())
	defer cancel()
	wsURL := buildWSURL(c.peerURL)

	nonceBytes := make([]byte, 16)
	_, _ = rand.Read(nonceBytes)
	nonce := hex.EncodeToString(nonceBytes)

	header := http.Header{}
	if tok := c.GetToken(); tok != "" {
		header.Set(protocol.HeaderPeerAuth, "Bearer "+tok)
	}
	if expectedInstanceID != "" {
		header.Set(protocol.HeaderInstanceID, expectedInstanceID)
	}
	header.Set(protocol.HeaderWSPurpose, "check")
	header.Set(protocol.HeaderWSNonce, nonce)

	conn, resp, err := c.dialWSAbortable(ctx, wsURL, header)
	if err != nil {
		if resp != nil {
			defer func() { drainBody(resp.Body); resp.Body.Close() }()
			remote := ReadError(resp, expectedInstanceID)
			var re *RemoteError
			if errors.As(remote, &re) && re.Trusted && re.Code == protocol.ErrTransportUnsupported {
				return fmt.Errorf("%w: %v", ErrWSUpgradeRejected, remote)
			}
			if errors.As(remote, &re) && (re.Trusted || resp.StatusCode == 401) {
				return remote
			}
			// Any other non-101 answer without a trusted DBBridge body is an
			// ordinary response from the path (gateway, proxy, WAF, redirect):
			// WS is unavailable there and auto may fall back to SSE.
			return &UpgradeRefusedError{Status: resp.StatusCode}
		}
		return wsUnavailable(ctx, err, false)
	}
	defer closeWSHard(conn)
	stopCancel := context.AfterFunc(ctx, func() { _ = closeWSHard(conn) })
	defer stopCancel()
	conn.SetReadLimit(16 << 10)

	// After a 101 every subprotocol,
	// version, structure or content error of the hello is a protocol error
	// that excludes this candidate. It is never ErrWSUpgradeRejected, so auto
	// cannot fall back to SSE on the same endpoint to tolerate it.
	if conn.Subprotocol() != protocol.SecWebSocketProtocol {
		return fmt.Errorf("%w: ws precheck subprotocol mismatch: expected %q, got %q", ErrProtocol, protocol.SecWebSocketProtocol, conn.Subprotocol())
	}

	// Read hello message within timeout
	deadline, _ := ctx.Deadline()
	_ = conn.SetReadDeadline(deadline)
	msgType, data, err := conn.ReadMessage()
	if err != nil {
		return wsUnavailable(ctx, err, true)
	}
	if msgType != websocket.TextMessage {
		return fmt.Errorf("%w: ws precheck hello expected text message, got %d", ErrProtocol, msgType)
	}

	var hello protocol.WSHelloMessage
	if err := protocol.DecodeJSON(bytes.NewReader(data), 16<<10, &hello); err != nil {
		return fmt.Errorf("%w: invalid hello message payload: %v", ErrProtocol, err)
	}

	if hello.Type != "hello" || hello.ProtocolVersion != protocol.ProtocolVersionString || hello.Transport != "ws" {
		return fmt.Errorf("%w: unexpected hello message content: type=%q protocol_version=%q transport=%q", ErrProtocol, hello.Type, hello.ProtocolVersion, hello.Transport)
	}
	if hello.Nonce != nonce {
		return fmt.Errorf("%w: ws precheck hello nonce mismatch", ErrProtocol)
	}

	if expectedNodeID != "" && hello.NodeID != expectedNodeID {
		return fmt.Errorf("%w: node ID mismatch in hello: expected %q, got %q", ErrProtocol, expectedNodeID, hello.NodeID)
	}
	if expectedInstanceID != "" && hello.InstanceID != expectedInstanceID {
		return fmt.Errorf("%w: instance ID mismatch in hello: expected %q, got %q", ErrProtocol, expectedInstanceID, hello.InstanceID)
	}
	if err := ctx.Err(); err != nil {
		return wsUnavailable(ctx, err, true)
	}

	// Clean shutdown of precheck connection (within 1s)
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		minTime(time.Now().Add(time.Second), deadline),
	)

	return nil
}

// DialWSSession establishes a genuine WebSocket data plane session.
//
// ws.handshake_timeout bounds only the Upgrade (TCP, TLS, response headers).
// The first ready message is read under ctx, the caller's attach budget
// (control_request_timeout covers handshake, ready and the reverse
// confirmation); without a ctx deadline control_request_timeout applies.
// When that budget runs out the error wraps context.DeadlineExceeded.
func (c *TripleTransportClient) DialWSSession(
	ctx context.Context,
	sessionID string,
	sessionToken string,
	epoch uint64,
	expectedInstanceID string,
) (*WSSessionConn, *protocol.ReadyEventData, error) {
	if _, ok := ctx.Deadline(); !ok {
		attach := c.tCfg.ControlRequestTimeout
		if attach <= 0 {
			attach = 10 * time.Second
		}
		var cancelAttach context.CancelFunc
		ctx, cancelAttach = context.WithTimeout(ctx, attach)
		defer cancelAttach()
	}
	handshakeCtx, cancel := context.WithTimeout(ctx, c.wsHandshakeTimeout())
	defer cancel()
	wsURL := buildWSURL(c.peerURL)

	header := http.Header{}
	if tok := c.GetToken(); tok != "" {
		header.Set(protocol.HeaderPeerAuth, "Bearer "+tok)
	}
	header.Set(protocol.HeaderSessionToken, sessionToken)
	if expectedInstanceID != "" {
		header.Set(protocol.HeaderInstanceID, expectedInstanceID)
	}
	header.Set(protocol.HeaderWSPurpose, "session")
	header.Set(protocol.HeaderWSSessionID, sessionID)
	header.Set(protocol.HeaderWSEpoch, strconv.FormatUint(epoch, 10))

	writeIdle := 60 * time.Second
	if c.tCfg.WS.WriteIdleTimeout > 0 {
		writeIdle = c.tCfg.WS.WriteIdleTimeout
	}

	conn, resp, err := c.dialWSAbortable(handshakeCtx, wsURL, header)
	cancel()
	if err != nil {
		if resp != nil {
			defer func() { drainBody(resp.Body); resp.Body.Close() }()
			return nil, nil, ReadError(resp, expectedInstanceID)
		}
		if e := wsTLSFailure(err); e != nil {
			return nil, nil, e
		}
		return nil, nil, fmt.Errorf("ws session dial failed: %w", err)
	}
	stopCancel := context.AfterFunc(ctx, func() { _ = closeWSHard(conn) })
	defer stopCancel()

	if conn.Subprotocol() != protocol.SecWebSocketProtocol {
		_ = closeWSHard(conn)
		return nil, nil, fmt.Errorf("%w: ws session subprotocol mismatch", ErrProtocol)
	}

	conn.SetReadLimit(16 << 10)
	// Read the initial ready text message.
	deadline, _ := ctx.Deadline()
	_ = conn.SetReadDeadline(deadline)
	msgType, data, err := conn.ReadMessage()
	if err != nil {
		_ = closeWSHard(conn)
		if attachExpired(ctx, err) {
			return nil, nil, fmt.Errorf("failed to read initial ready message: %w (%v)", context.DeadlineExceeded, err)
		}
		if ce := ctx.Err(); ce != nil {
			return nil, nil, fmt.Errorf("failed to read initial ready message: %w (%v)", ce, err)
		}
		return nil, nil, fmt.Errorf("failed to read initial ready message: %w", classifyWSReadError(err))
	}
	if msgType != websocket.TextMessage {
		_ = closeWSHard(conn)
		return nil, nil, fmt.Errorf("%w: expected text ready message, got %d", ErrProtocol, msgType)
	}

	var ready protocol.ReadyEventData
	if err := protocol.DecodeJSON(bytes.NewReader(data), 16<<10, &ready); err != nil {
		_ = closeWSHard(conn)
		return nil, nil, fmt.Errorf("%w: invalid ready message: %v", ErrProtocol, err)
	}

	expectedEpochStr := strconv.FormatUint(epoch, 10)
	if ready.Type != "ready" || ready.ProtocolVersion != protocol.ProtocolVersionString || ready.SessionID != sessionID || ready.InstanceID != expectedInstanceID || ready.StreamID == "" || ready.Transport != "ws" || ready.Epoch != expectedEpochStr {
		_ = closeWSHard(conn)
		return nil, nil, fmt.Errorf("%w: unexpected WS ready binding", ErrProtocol)
	}
	if err := ctx.Err(); err != nil {
		_ = closeWSHard(conn)
		return nil, nil, err
	}

	// The attach AfterFunc may already be closing conn if ctx ended after
	// the check above; never hand out a connection that is going away.
	if !stopCancel() {
		_ = closeWSHard(conn)
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		return nil, nil, context.DeadlineExceeded
	}

	conn.SetReadLimit(int64(max(16<<10, c.tCfg.MaxFrameSize+protocol.WSBinaryHeaderLen)))
	c.BindStream(sessionID, epoch, ready.StreamID, "ws")
	// Reset read deadline for normal session operation
	_ = conn.SetReadDeadline(time.Time{})

	sessConn := &WSSessionConn{
		conn:      conn,
		streamID:  ready.StreamID,
		writeIdle: writeIdle,
	}

	return sessConn, &ready, nil
}

// attachExpired reports a read failure caused by ctx's deadline: either ctx
// already says so or the socket read deadline derived from it fired a moment
// before ctx.Err() is set.
func attachExpired(ctx context.Context, err error) bool {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	dl, ok := ctx.Deadline()
	return ok && errors.As(err, &ne) && ne.Timeout() && !time.Now().Before(dl.Add(-10*time.Millisecond))
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// wsHandshakeTimeout is ws.handshake_timeout, 5s when unset.
func (c *TripleTransportClient) wsHandshakeTimeout() time.Duration {
	if c.tCfg.WS.HandshakeTimeout > 0 {
		return c.tCfg.WS.HandshakeTimeout
	}
	return 5 * time.Second
}

// newWSDialer builds the dialer shared by the precheck and session dials:
// budgeted TCP dial wrapped for write-idle detection, http/1.1-only TLS and
// the optional environment proxy.
func (c *TripleTransportClient) newWSDialer() *websocket.Dialer {
	dialer := &websocket.Dialer{
		Subprotocols:     []string{protocol.SecWebSocketProtocol},
		HandshakeTimeout: c.wsHandshakeTimeout(),
	}
	dialer.NetDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := c.dial(ctx, network, address, false, nil)
		if err != nil {
			return nil, err
		}
		return NewIdleWriteConn(conn), nil
	}
	if c.tCfg.UseEnvironmentProxy {
		dialer.Proxy = http.ProxyFromEnvironment
	}
	dialer.TLSClientConfig = c.wsTLS()
	return dialer
}
