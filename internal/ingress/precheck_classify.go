package ingress

import (
	"context"
	"errors"

	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
)

// precheckVerdict is what a failed WS precheck means for candidate selection.
type precheckVerdict int

const (
	// precheckWSUnavailable: WS does not work on this endpoint right now.
	// Only the (endpoint, ws) pair cools down; auto may use SSE on the same
	// endpoint after a fresh probe.
	precheckWSUnavailable precheckVerdict = iota
	// precheckUnsafe: TLS, identity, subprotocol, version or illegal hello
	// data. The endpoint is isolated and SSE is not used to tolerate it.
	precheckUnsafe
	// precheckAuthRejected: the authenticated Upgrade was refused with 401
	// (trusted or raw) or a trusted 403. No SSE fallback.
	precheckAuthRejected
	// precheckTemporary: local contention, our own deadline, a retryable
	// quota answer or a server error of the peer itself. Nothing is cooled;
	// the selection is retried later.
	precheckTemporary
)

// classifyPrecheck decides what a failed precheck means. ctx is the
// selection's context; checkDone reports that the per-check timeout ended.
//
// Only an authenticated identity or protocol failure isolates the endpoint.
// Every other answer of the path (an Upgrade refusal, a redirect, a proxy
// 407, a WAF 413/414/429/431, an untrusted 5xx, a reset) means only that WS
// is unavailable there.
func classifyPrecheck(ctx context.Context, checkDone bool, e error) precheckVerdict {
	if localOrCanceled(ctx, e) || localResourceError(e) {
		return precheckTemporary
	}
	if errors.Is(e, transport.ErrProtocol) {
		return precheckUnsafe
	}
	if checkDone {
		return precheckTemporary
	}
	var remote *transport.RemoteError
	if errors.As(e, &remote) {
		if remote.Trusted {
			switch remote.Code {
			case protocol.ErrInstanceMismatch, protocol.ErrNodeMismatch, protocol.ErrProtocolMismatch:
				return precheckUnsafe
			case protocol.ErrTransportUnsupported:
				return precheckWSUnavailable
			}
			if remote.Status == 401 || remote.Status == 403 {
				return precheckAuthRejected
			}
			if safeRetry(e) || remote.Status >= 500 {
				return precheckTemporary
			}
			return precheckWSUnavailable
		}
		if remote.Status == 401 {
			return precheckAuthRejected
		}
		return precheckWSUnavailable
	}
	return precheckWSUnavailable
}
