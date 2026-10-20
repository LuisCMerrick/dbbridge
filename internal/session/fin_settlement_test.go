package session

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"dbbridge/internal/protocol"
)

// finTestSession returns an attached WS session in epoch 1 whose backend sees
// no traffic, and its stream ID.
func finTestSession(t *testing.T, recovery bool) (*Session, string) {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { _ = b.Close() })
	go func() { _, _ = io.Copy(io.Discard, b) }()
	s := NewSession("p", "t", "inst", a, 4<<20, 2<<20, 8, 64, recovery)
	t.Cleanup(s.Close)
	st, err := s.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ack(1, st, protocol.AckRequest{StreamID: st, ReadyReceived: true, DownDeliveredOffset: "0", UpConfirmedOffset: "0"}); err != nil {
		t.Fatal(err)
	}
	return s, st
}

// finTestDownData produces "world" from the backend followed by EOF, and emits
// and acknowledges it on epoch ep / stream st.
func finTestDownData(t *testing.T, s *Session, ep uint64, st string) {
	t.Helper()
	if !s.ReserveDownRead(32 << 10) {
		t.Fatal("reserve down read")
	}
	buf := make([]byte, 32<<10)
	copy(buf, "world")
	if _, err := s.FinishDownRead(buf[:5]); err != nil {
		t.Fatal(err)
	}
	s.MarkDownFinal(5)
	if err := s.BeginEmit(ep, 0, 5); err != nil {
		t.Fatal(err)
	}
	s.FinishEmit(ep, 5, nil)
	if _, err := s.Ack(ep, st, protocol.AckRequest{StreamID: st, DownDeliveredOffset: "5", UpConfirmedOffset: "0"}); err != nil {
		t.Fatal(err)
	}
}

func finTestUpFIN(t *testing.T, s *Session) {
	t.Helper()
	if _, err := s.IngestUpFIN(1, 0); err != nil {
		t.Fatal(err)
	}
	afWait(t, "up fin applied", func() bool { u, _ := s.FINs(); return u.Applied })
}

func finAck(s *Session, ep uint64, st, probe string) (*protocol.AckResponse, error) {
	return s.Ack(ep, st, protocol.AckRequest{StreamID: st, DownDeliveredOffset: "5", DownFinApplied: true, UpConfirmedOffset: "0", ProbeID: probe})
}

func finResume(s *Session, id, epoch string, applied bool) (*protocol.ResumeResponse, error) {
	five := "5"
	return s.Resume(&protocol.ResumeRequest{ResumeRequestID: id, ExpectedEpoch: epoch, Transport: protocol.TransportMode{Mode: "ws"},
		KnownUpAckOffset: "0", UpNextOffset: "0", DownDeliveredOffset: "5",
		DownFIN: protocol.DirectionFIN{Seen: true, FinalOffset: &five, Applied: applied}}, time.Second)
}

// A down-FIN ACK parked while the FIN write settles proves delivery even when
// the write then reports an error, so the session completes normally and the
// peer's repeated FIN ACK stays valid.
func TestFinAckParkedDuringFailedFinEmitCountsAsDelivered(t *testing.T) {
	s, st := finTestSession(t, false)
	finTestUpFIN(t, s)
	finTestDownData(t, s, 1, st)
	if err := s.BeginFinEmit(1); err != nil {
		t.Fatal(err)
	}
	resp, err := finAck(s, 1, st, "p1")
	if err != nil {
		t.Fatalf("parked FIN ack: %v", err)
	}
	if resp.DownFinAcked {
		t.Fatal("FIN ACK applied before the FIN emit settled")
	}
	s.FinishFinEmit(1, errors.New("flush failed after the bytes left"))
	if _, down := s.FINs(); !down.Acked || !down.Applied {
		t.Fatalf("parked FIN ACK was dropped: DownFIN=%+v", down)
	}
	if s.GetState() != StateClosed || s.TerminalCategory() != "normal" {
		t.Fatalf("state=%s category=%q, want CLOSED/normal", s.GetState(), s.TerminalCategory())
	}
	resp, err = finAck(s, 1, st, "p2")
	if err != nil || !resp.DownFinAcked {
		t.Fatalf("repeated FIN ack: resp=%+v err=%v", resp, err)
	}
}

// A failed FIN write without a parked ACK leaves the FIN unemitted, so a FIN
// ACK is still refused until the FIN is written again.
func TestFailedFinEmitWithoutAckKeepsFinUnemitted(t *testing.T) {
	s, st := finTestSession(t, false)
	finTestDownData(t, s, 1, st)
	if err := s.BeginFinEmit(1); err != nil {
		t.Fatal(err)
	}
	s.FinishFinEmit(1, errors.New("write failed"))
	if _, err := finAck(s, 1, st, "p1"); !errors.Is(err, ErrBadAck) {
		t.Fatalf("FIN ack after failed FIN emit: err=%v, want ErrBadAck", err)
	}
}

// /resume refuses down_fin.applied for a FIN that never entered emit
// settlement, consistent with /ack.
func TestResumeRejectsDownFINAppliedBeforeFinEmit(t *testing.T) {
	s, st := finTestSession(t, true)
	finTestUpFIN(t, s)
	finTestDownData(t, s, 1, st)
	if _, err := finAck(s, 1, st, ""); !errors.Is(err, ErrBadAck) {
		t.Fatalf("/ack with down_fin_applied before FIN emit: err=%v, want ErrBadAck", err)
	}
	zero, five := "0", "5"
	_, err := s.Resume(&protocol.ResumeRequest{ResumeRequestID: "r1", ExpectedEpoch: "1", Transport: protocol.TransportMode{Mode: "ws"},
		KnownUpAckOffset: "0", UpNextOffset: "0", DownDeliveredOffset: "5",
		UpFIN:   protocol.DirectionFIN{Seen: true, FinalOffset: &zero, Applied: true},
		DownFIN: protocol.DirectionFIN{Seen: true, FinalOffset: &five, Applied: true}}, time.Second)
	if !errors.Is(err, ErrFrameConflict) {
		t.Fatalf("resume err=%v, want ErrFrameConflict", err)
	}
	if s.TerminalCategory() == "normal" {
		t.Fatal("unemitted down FIN was accepted as a normal close")
	}
}

// A FIN that was mid-emit when the resume started may have reached the peer,
// so its down_fin.applied is accepted. In the new epoch the peer's repeated
// FIN ACK is valid before any FIN is written on the new stream.
func TestResumeAcceptsDownFINAppliedWhileFinEmitInProgress(t *testing.T) {
	s, st := finTestSession(t, true)
	finTestDownData(t, s, 1, st)
	if err := s.BeginFinEmit(1); err != nil {
		t.Fatal(err)
	}
	resp, err := finResume(s, "r1", "1", true)
	if err != nil {
		t.Fatalf("resume with down_fin.applied during FIN emit: %v", err)
	}
	s.FinishFinEmit(1, errors.New("stream cancelled"))
	if !resp.DownFIN.Acked {
		t.Fatalf("resume response DownFIN=%+v, want acked", resp.DownFIN)
	}
	st2, err := s.ReserveStream(2, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ack(2, st2, protocol.AckRequest{StreamID: st2, ReadyReceived: true, DownDeliveredOffset: "5", DownFinApplied: true, UpConfirmedOffset: "0"}); err != nil {
		t.Fatalf("ready ACK repeating the acked FIN: %v", err)
	}
}

// The FIN must be emitted again in a new epoch before a FIN ACK of that epoch
// is accepted, like the data bytes.
func TestFinAckInNewEpochRequiresFinReemit(t *testing.T) {
	s, st := finTestSession(t, true)
	finTestDownData(t, s, 1, st)
	if err := s.BeginFinEmit(1); err != nil {
		t.Fatal(err)
	}
	s.FinishFinEmit(1, nil)
	if _, err := finResume(s, "r1", "1", false); err != nil {
		t.Fatalf("resume: %v", err)
	}
	st2, err := s.ReserveStream(2, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ack(2, st2, protocol.AckRequest{StreamID: st2, ReadyReceived: true, DownDeliveredOffset: "5", UpConfirmedOffset: "0"}); err != nil {
		t.Fatal(err)
	}
	if _, err := finAck(s, 2, st2, "p1"); !errors.Is(err, ErrBadAck) {
		t.Fatalf("FIN ack before FIN re-emit: err=%v, want ErrBadAck", err)
	}
	if err := s.BeginFinEmit(2); err != nil {
		t.Fatal(err)
	}
	s.FinishFinEmit(2, nil)
	resp, err := finAck(s, 2, st2, "p2")
	if err != nil || !resp.DownFinAcked {
		t.Fatalf("FIN ack after re-emit: resp=%+v err=%v", resp, err)
	}
}

// A FIN that was emitted in an earlier epoch can still be reported as applied
// by a later resume, even though the new epoch has not re-emitted it.
func TestResumeAcceptsDownFINAppliedFromEarlierEpoch(t *testing.T) {
	s, st := finTestSession(t, true)
	finTestDownData(t, s, 1, st)
	if err := s.BeginFinEmit(1); err != nil {
		t.Fatal(err)
	}
	s.FinishFinEmit(1, nil)
	if _, err := finResume(s, "r1", "1", false); err != nil {
		t.Fatalf("first resume: %v", err)
	}
	resp, err := finResume(s, "r2", "2", true)
	if err != nil || !resp.DownFIN.Acked {
		t.Fatalf("second resume: resp=%+v err=%v", resp, err)
	}
}
