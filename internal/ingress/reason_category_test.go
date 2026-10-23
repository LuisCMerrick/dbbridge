package ingress

import (
	"testing"
	"time"

	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
)

func TestReasonCategoryNamesEgressDialAndDrainRefusals(t *testing.T) {
	for code, want := range map[string]string{
		protocol.ErrDialFailed:           "dial_failed",
		protocol.ErrDraining:             "draining",
		protocol.ErrBadAck:               "offset",
		protocol.ErrReplayTooOld:         "offset",
		protocol.ErrBadFrame:             "protocol",
		protocol.ErrBadDigest:            "protocol",
		protocol.ErrFrameTooLarge:        "protocol",
		protocol.ErrFrameConflict:        "protocol",
		protocol.ErrIdempotencyConflict:  "protocol",
		protocol.ErrProtocolMismatch:     "protocol",
		protocol.ErrNodeMismatch:         "protocol",
		protocol.ErrTransportMismatch:    "protocol",
		protocol.ErrTransportUnsupported: "protocol",
		protocol.ErrRecoveryDisabled:     "protocol",
		protocol.ErrRecoveryUnsupported:  "protocol",
		protocol.ErrInstanceMismatch:     "instance",
	} {
		if got := reasonCategory(&transport.RemoteError{Code: code}); got != want {
			t.Fatalf("%s: got %q, want %q", code, got, want)
		}
	}
	for _, code := range []string{
		protocol.ErrFrameTooLarge,
		protocol.ErrTransportMismatch,
		protocol.ErrTransportUnsupported,
		protocol.ErrProtocolMismatch,
		protocol.ErrNodeMismatch,
	} {
		if !fatalError(&transport.RemoteError{Code: code, Trusted: true}) {
			t.Fatalf("expected trusted %s to be fatal", code)
		}
	}
}

func TestLogTimeLeavesZeroEmpty(t *testing.T) {
	if got := logTime(time.Time{}); got != "" {
		t.Fatalf("zero time logged as %v", got)
	}
	now := time.Now()
	if got := logTime(now); got != now {
		t.Fatalf("non-zero time changed: %v", got)
	}
}
