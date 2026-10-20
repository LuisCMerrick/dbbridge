package ingress

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"dbbridge/internal/transport"
)

func fatalError(e error) bool {
	if e == nil {
		return false
	}
	if errors.Is(e, transport.ErrProtocol) || errors.Is(e, transport.ErrBackpressure) || errors.Is(e, io.ErrNoProgress) {
		return true
	}
	var r *transport.RemoteError
	if errors.As(e, &r) {
		return r.Trusted && (r.SessionClosed || r.Status == 401 || r.Status == 403 || r.Code == protocol.ErrBadAck || r.Code == protocol.ErrBadFrame || r.Code == protocol.ErrFrameConflict || r.Code == protocol.ErrIdempotencyConflict || r.Code == protocol.ErrRecoveryDisabled || r.Code == protocol.ErrRecoveryUnsupported || r.Code == protocol.ErrReplayTooOld)
	}
	return false
}
func safeRetry(e error) bool {
	var r *transport.RemoteError
	return errors.As(e, &r) && r.Trusted && r.Retryable && !r.SessionClosed && (r.Status == 429 || r.Status == 503 || r.Code == protocol.ErrWindowExceeded || r.Code == protocol.ErrCreatePending || r.Code == protocol.ErrResumePending || r.Code == protocol.ErrResumeInProgress)
}
func pause(ctx context.Context, attempt int, e error) error {
	d := 100 * time.Millisecond * time.Duration(1<<min(attempt, 4))
	if d > 2*time.Second {
		d = 2 * time.Second
	}
	var b [1]byte
	rand.Read(b[:])
	d = d/2 + time.Duration(b[0])*d/512
	var r *transport.RemoteError
	if errors.As(e, &r) && r.RetryAfter > d {
		d = r.RetryAfter
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// errClosedNormal terminates a session whose both directions finished.
var errClosedNormal = errors.New("closed_normal")

// errNodeStopped terminates sessions because the node shuts down. It is
// reported with reason "shutdown" and is not counted as a failed session.
var errNodeStopped = errors.New("node stopped")

// errLocalSocket wraps read/write errors of the local client socket so that
// they are reported as "local_socket", apart from carrier failures.
var errLocalSocket = errors.New("local client socket error")

// localSocketError wraps a client socket error, keeping the original error
// in the chain.
func localSocketError(e error) error {
	return fmt.Errorf("%w: %w", errLocalSocket, e)
}

// localResourceError reports failures that are caused by this process's own
// limits rather than by the remote path: local probe slots, local resource
// budgets, or a connection budget wait that ended without the request ever
// leaving this node.
func localResourceError(e error) bool {
	if e == nil {
		return false
	}
	if errors.Is(e, errLocalBusy) || errors.Is(e, resource.ErrLimit) || errors.Is(e, transport.ErrLocalConnBudget) {
		return true
	}
	var local interface{ LocalResource() bool }
	return errors.As(e, &local) && local.LocalResource()
}

// terminalRemote reports a trusted, deliberate, non-retryable refusal from
// the owner: retrying the same request elsewhere cannot change the answer.
// Server errors (5xx) are excluded; they are not a verdict on the request.
func terminalRemote(e error) bool {
	var r *transport.RemoteError
	return errors.As(e, &r) && r.Trusted && !r.Retryable && r.Status >= 400 && r.Status < 500 && !safeRetry(e)
}

func reasonCategory(e error) string {
	if errors.Is(e, errClosedNormal) || e != nil && e.Error() == "closed_normal" {
		return "normal"
	}
	if errors.Is(e, errNodeStopped) {
		return "shutdown"
	}
	if errors.Is(e, errLocalSocket) {
		return "local_socket"
	}
	if errors.Is(e, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(e, transport.ErrProtocol) {
		return "protocol"
	}
	var r *transport.RemoteError
	if errors.As(e, &r) {
		switch r.Code {
		case protocol.ErrSessionClosed:
			return "failed"
		case protocol.ErrBackendLost:
			return "backend_lost"
		case protocol.ErrUnauthorized:
			return "unauthorized"
		case protocol.ErrTargetForbidden:
			return "forbidden"
		case protocol.ErrBadAck:
			return "offset"
		case protocol.ErrFrameConflict:
			return "protocol"
		case protocol.ErrQuotaExceeded:
			return "quota"
		case protocol.ErrDialFailed:
			return "dial_failed"
		case protocol.ErrDraining:
			return "draining"
		}
	}
	if e != nil {
		text := e.Error()
		switch {
		case strings.Contains(text, "barrier"):
			return "barrier"
		case strings.Contains(text, "quota"):
			return "quota"
		case strings.Contains(text, "buffer"):
			return "buffer"
		case strings.Contains(text, "backpressure"):
			return "timeout"
		}
	}
	return "network"
}

// logTime renders t for structured logs, leaving the zero time empty.
func logTime(t time.Time) any {
	if t.IsZero() {
		return ""
	}
	return t
}
