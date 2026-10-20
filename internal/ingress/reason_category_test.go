package ingress

import (
	"testing"
	"time"

	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
)

func TestReasonCategoryNamesEgressDialAndDrainRefusals(t *testing.T) {
	for code, want := range map[string]string{
		protocol.ErrDialFailed: "dial_failed",
		protocol.ErrDraining:   "draining",
	} {
		if got := reasonCategory(&transport.RemoteError{Code: code}); got != want {
			t.Fatalf("%s: got %q, want %q", code, got, want)
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
