package transport

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/gorilla/websocket"
)

// A close frame answered after this side already sent its own is a closing
// socket, not a protocol violation that would end the session.
func TestClassifyWSReadErrorCloseSent(t *testing.T) {
	for _, e := range []error{websocket.ErrCloseSent, fmt.Errorf("read: %w", websocket.ErrCloseSent)} {
		if got := classifyWSReadError(e); errors.Is(got, ErrProtocol) || !errors.Is(got, websocket.ErrCloseSent) {
			t.Fatalf("classify(%v) = %v", e, got)
		}
	}
	if !errors.Is(classifyWSReadError(errors.New("websocket: bad opcode")), ErrProtocol) {
		t.Fatal("framing error no longer a protocol error")
	}
}

// A TLS record cut short by the end of the stream (a reverse tunnel stream
// ending mid-record) is a lost carrier, not a framing violation.
func TestWSReadTruncatedTLSRecordIsCarrierLoss(t *testing.T) {
	if got := classifyWSReadError(io.ErrUnexpectedEOF); errors.Is(got, ErrProtocol) {
		t.Fatalf("got %v", got)
	}
}
