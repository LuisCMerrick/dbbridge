package transport

import (
	"dbbridge/internal/protocol"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ErrProtocol is a fatal peer protocol violation. It is the same value as
// protocol.ErrPeerProtocol so that protocol validators (e.g. ValidateBinding)
// classify identically without wrapping at every call site.
var ErrProtocol = protocol.ErrPeerProtocol

type RemoteError struct {
	Status int
	Code   string
	// Message is the peer's error message (bounded by the 32 KiB decode
	// limit). It carries the terminal category of DBB_SESSION_CLOSED.
	Message                           string
	Retryable, SessionClosed, Trusted bool
	RetryAfter                        time.Duration
}

func (e *RemoteError) Error() string {
	if e.Status == 0 {
		// A terminal error message received on the carrier.
		return "peer error: " + e.Code
	}
	return fmt.Sprintf("peer HTTP %d: %s", e.Status, e.Code)
}
func ReadError(resp *http.Response, inst string) error {
	var e protocol.ErrorResponse
	valid := protocol.DecodeJSON(resp.Body, 32<<10, &e) == nil && strings.HasPrefix(e.Code, "DBB_")
	drainBody(resp.Body)
	return &RemoteError{Status: resp.StatusCode, Code: e.Code, Message: e.Message, Retryable: e.Retryable, SessionClosed: e.SessionClosed, Trusted: valid && inst != "" && resp.Header.Get(protocol.HeaderInstanceID) == inst, RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())}
}

// ErrWSUpgradeRejected reports that the WebSocket path of an endpoint is
// unavailable while the endpoint itself answered: the Upgrade was
// refused by an intermediary or by the peer (DBB_TRANSPORT_UNSUPPORTED), or
// the connection was dropped instead of being upgraded. In auto mode the
// caller may fall back to SSE on the same endpoint after a fresh probe; it is
// never an identity or protocol failure of the endpoint.
var ErrWSUpgradeRejected = errors.New("websocket upgrade rejected by gateway or peer")

// UpgradeRefusedError is an ordinary HTTP answer (no trusted DBBridge error
// body, not a raw 401) to the WS precheck Upgrade: a redirect, a proxy 407,
// a WAF 413/414/429/431, a gateway 404/502 and the like. It matches
// ErrWSUpgradeRejected with errors.Is.
type UpgradeRefusedError struct {
	// Status is the HTTP status the path answered with.
	Status int
}

func (e *UpgradeRefusedError) Error() string {
	return fmt.Sprintf("%v: status %d", ErrWSUpgradeRejected, e.Status)
}

func (e *UpgradeRefusedError) Unwrap() error { return ErrWSUpgradeRejected }

// Transient reports a refusal that says nothing lasting about the WS path
// (408, 425, 429 and 5xx): the outcome should not be cached.
func (e *UpgradeRefusedError) Transient() bool {
	switch e.Status {
	case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests:
		return true
	}
	return e.Status >= 500
}

// TransientUpgradeRefusal reports whether err is an UpgradeRefusedError whose
// Transient method is true.
func TransientUpgradeRefusal(err error) bool {
	var u *UpgradeRefusedError
	return errors.As(err, &u) && u.Transient()
}

var ErrBackpressure = errors.New("data channel backpressure timeout")

// maxRetryAfter caps how long a peer or gateway may ask us to back off.
const maxRetryAfter = 2 * time.Second

// parseRetryAfter accepts delay-seconds or an HTTP-date (RFC 9110 10.2.3) and
// clamps the result to maxRetryAfter; a larger hint is truncated, not ignored.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	var d time.Duration
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		if n <= 0 {
			return 0
		}
		if n >= int64(maxRetryAfter/time.Second) {
			return maxRetryAfter
		}
		d = time.Duration(n) * time.Second
	} else if t, err := http.ParseTime(v); err == nil {
		d = t.Sub(now)
	}
	return min(max(d, 0), maxRetryAfter)
}

// maxDrain bounds how much of an unwanted response body is read so that the
// HTTP/1.1 connection can go back to the idle pool; a larger body is
// abandoned and the connection closed by Body.Close.
const maxDrain = 64 << 10

func drainBody(body io.Reader) {
	if body != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(body, maxDrain))
	}
}
