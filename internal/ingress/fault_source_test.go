package ingress

import (
	"context"
	"errors"
	"testing"

	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
)

// The first fault of a fault cycle keeps its source and reason; later
// faults do not overwrite them.
func TestFirstFaultSourceIsKept(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &ingressSession{ctx: ctx, cancel: cancel, faults: make(chan sessionFault, 4)}
	s.fault(nil, observability.FaultControl, errors.New("connection reset"), false)
	s.fault(nil, observability.FaultLiveness, context.DeadlineExceeded, false)
	if s.faultSource != observability.FaultControl || s.faultReason != "network" {
		t.Fatalf("first fault %q/%q, want control/network", s.faultSource, s.faultReason)
	}
}

// A terminal error message on the carrier keeps its code for the reason.
func TestCarrierErrorMessageReason(t *testing.T) {
	for code, want := range map[string]string{protocol.ErrBackendLost: "backend_lost", protocol.ErrBufferExhausted: "buffer", protocol.ErrTargetForbidden: "forbidden"} {
		e := &transport.RemoteError{Code: code, Trusted: true, SessionClosed: true}
		if got := reasonCategory(e); got != want {
			t.Errorf("%s: reason %q, want %q", code, got, want)
		}
		if !fatalError(e) {
			t.Errorf("%s: not fatal", code)
		}
	}
}
