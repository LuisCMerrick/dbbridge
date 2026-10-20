package session

import (
	"errors"
	"testing"

	"dbbridge/internal/protocol"
)

// An up FIN whose final offset is below the bytes already written is a frame
// conflict, the same as a FIN below a pending or in-flight frame.
func TestUpFINBelowWrittenOffsetIsFrameConflict(t *testing.T) {
	s, _ := finTestSession(t, false)
	d := []byte("hello")
	if _, _, _, err := s.IngestUpFrame(1, 0, 5, afSHA(d), d); err != nil {
		t.Fatal(err)
	}
	afWait(t, "backend write", func() bool { return s.UpAckOffset() == 5 })
	_, err := s.IngestUpFIN(1, 3)
	if !errors.Is(err, ErrFrameConflict) {
		t.Fatalf("FIN(3) after 5 written: err=%v, want ErrFrameConflict", err)
	}
}

// An upstream frame above the negotiated max_frame_size reports
// ErrFrameConflict, which is not a frame conflict.
func TestOversizeUpFrameReportsFrameTooLarge(t *testing.T) {
	s, _ := finTestSession(t, false)
	s.SetNegotiatedLimits(protocol.SessionLimits{MaxFrameSize: 4})
	d := []byte("hello")
	_, _, _, err := s.IngestUpFrame(1, 0, 5, afSHA(d), d)
	if !errors.Is(err, ErrFrameTooLarge) || errors.Is(err, ErrFrameConflict) {
		t.Fatalf("oversize frame: err=%v, want ErrFrameConflict only", err)
	}
}

// data_rx_blocked caused by a full in-flight queue behind a backend write is
// reported with the local_socket_write reason, never "none".
func TestAckNamesBackendWriteWhenInflightQueueIsFull(t *testing.T) {
	s, _ := finTestSession(t, false)
	s.mu.Lock()
	s.inflightWriting = true
	s.maxInflight = 1
	resp := s.ackLocked("")
	s.inflightWriting = false
	s.mu.Unlock()
	if !resp.DataRxBlocked || resp.BackpressureReason != "local_socket_write" {
		t.Fatalf("rx_blocked=%v reason=%q, want true/local_socket_write", resp.DataRxBlocked, resp.BackpressureReason)
	}
	s.mu.Lock()
	s.transportRxBlocked = true
	s.transportWriting = true
	resp = s.ackLocked("")
	s.transportRxBlocked, s.transportWriting = false, false
	s.mu.Unlock()
	if resp.BackpressureReason != "global_buffer" {
		t.Fatalf("reason=%q, want global_buffer", resp.BackpressureReason)
	}
	s.mu.Lock()
	s.transportWriting = true
	resp = s.ackLocked("")
	s.transportWriting = false
	s.mu.Unlock()
	if resp.DataRxBlocked || resp.BackpressureReason != "data_channel_write" {
		t.Fatalf("rx_blocked=%v reason=%q, want false/data_channel_write", resp.DataRxBlocked, resp.BackpressureReason)
	}
	if resp = s.Snapshot(); resp.BackpressureReason != "none" {
		t.Fatalf("idle reason=%q, want none", resp.BackpressureReason)
	}
}

// Closing with a specific terminal code records it and its category.
func TestCloseWithReasonRecordsTerminalCode(t *testing.T) {
	for code, cat := range map[string]string{protocol.ErrFrameConflict: "protocol", protocol.ErrFrameTooLarge: "protocol", protocol.ErrTargetForbidden: "forbidden"} {
		s, _ := finTestSession(t, false)
		s.CloseWithReason(code)
		if s.TerminalCode() != code || s.TerminalCategory() != cat {
			t.Errorf("CloseWithReason(%s): code=%s category=%s, want %s", code, s.TerminalCode(), s.TerminalCategory(), cat)
		}
	}
}
