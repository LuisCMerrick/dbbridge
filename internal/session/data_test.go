package session

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
)

func afActive(t *testing.T, buffer int64) (*Session, string) {
	t.Helper()
	s := NewSession("p", "db", "i", afSink{}, buffer, 1<<20, 8, 64, true)
	t.Cleanup(s.Close)
	s.Configure(10*time.Second, time.Minute, "ing", nil)
	id, err := s.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ActivateDataPlane(id); err != nil {
		t.Fatal(err)
	}
	return s, id
}

func afFillDown(t *testing.T, s *Session, n int64) {
	t.Helper()
	if !s.ReserveDownRead(n) {
		t.Fatal("reserve")
	}
	if _, err := s.FinishDownRead(make([]byte, n)); err != nil {
		t.Fatal(err)
	}
}

// A suspended session waiting for downstream capacity is bounded by the
// recovery grace, not by the backpressure timeout.
func TestSuspendedBackpressureWaitsForGrace(t *testing.T) {
	s, _ := afActive(t, 64)
	s.SetBackpressureTimeout(30 * time.Millisecond)
	afFillDown(t, s, 64)
	s.DropTransport(1)
	result := make(chan bool, 1)
	go func() { result <- s.WaitDownCapacity(32) }()
	select {
	case ok := <-result:
		t.Fatalf("WaitDownCapacity returned %v during recovery (terminal=%s)", ok, s.TerminalCode())
	case <-time.After(200 * time.Millisecond):
	}
	if s.GetState() == StateClosed || !s.Recovering() {
		t.Fatalf("suspended session closed or not recovering: %s", s.TerminalCode())
	}
	s.Close()
	if <-result {
		t.Fatal("closed session reported capacity")
	}
}

// An attached session still ends with BUFFER_EXHAUSTED once the
// backpressure timeout elapses, and the close is decided by the waiter.
func TestAttachedBackpressureStillCloses(t *testing.T) {
	s, _ := afActive(t, 64)
	s.SetBackpressureTimeout(30 * time.Millisecond)
	afFillDown(t, s, 64)
	started := time.Now()
	if s.WaitDownCapacity(32) {
		t.Fatal("full window reported capacity")
	}
	if s.TerminalCode() != protocol.ErrBufferExhausted || s.TerminalCategory() != "buffer" || time.Since(started) > time.Second {
		t.Fatalf("terminal=%s after %v", s.TerminalCode(), time.Since(started))
	}
}

// Short backend reads are retained compactly, so a 4 MiB direction
// window holds far more than 128 one-byte frames.
func TestShortReadsDoNotPinReadAllocation(t *testing.T) {
	payload := resource.New(64 << 20)
	r := NewRegistry(1<<20, 64<<20)
	r.SetResources(payload, nil, 0)
	s, _ := afActive(t, 4<<20)
	r.Register(s)
	defer r.CloseAll()
	for i := 0; i < 400; i++ {
		if !s.WaitDownCapacity(32 << 10) {
			t.Fatalf("frame %d: session closed", i)
		}
		if !s.ReserveDownRead(32 << 10) {
			t.Fatalf("frame %d: window full after %d one-byte frames", i, i)
		}
		buf := make([]byte, 32<<10)
		if _, err := s.FinishDownRead(buf[:1]); err != nil {
			t.Fatal(err)
		}
	}
	if s.BufferSize() != 400 || payload.Used() != 400 {
		t.Fatalf("retained=%d payload=%d, want 400", s.BufferSize(), payload.Used())
	}
	if !s.AckWanted() {
		t.Fatal("400 retained frames should ask the peer for an ACK")
	}
	// A read that fills most of its allocation keeps it without a copy.
	if !s.ReserveDownRead(1024) {
		t.Fatal("reserve")
	}
	if _, err := s.FinishDownRead(make([]byte, 1000, 1024)); err != nil {
		t.Fatal(err)
	}
	if s.BufferSize() != 400+1024 || payload.Used() != 400+1024 {
		t.Fatalf("retained=%d payload=%d", s.BufferSize(), payload.Used())
	}
}

// A frame straddling upAckOffset is a non-retryable conflict.
func TestStraddlingFrameIsConflict(t *testing.T) {
	s, _ := afActive(t, 1<<20)
	p := make([]byte, 10)
	if _, _, _, err := s.IngestUpFrame(1, 0, 10, afSHA(p), p); err != nil {
		t.Fatal(err)
	}
	afWait(t, "write", func() bool { return s.UpAckOffset() == 10 })
	s.ConfirmUpAdvancesReceiptFloor(10)
	q := []byte("0123456789")
	_, _, _, err := s.IngestUpFrame(1, 5, 10, afSHA(q), q)
	if !errors.Is(err, ErrFrameConflict) {
		t.Fatalf("straddling frame: %v", err)
	}
	// Frames at the ack boundary are still accepted.
	if _, ok, _, err := s.IngestUpFrame(1, 10, 10, afSHA(q), q); err != nil || !ok {
		t.Fatalf("next frame: %v %v", ok, err)
	}
}

// Heartbeat-matching ACKs on a healthy session do not re-run recovery
// completion (and the global retention sweep it spawns).
func TestHealthyAckSkipsRecoveryCompletion(t *testing.T) {
	r := NewRegistry(64<<20, 256<<20)
	ret := resource.NewRetention(64 << 20)
	r.SetRetention(ret)
	var sizeCalls atomic.Int64
	ret.Register(resource.NewRetentionLease(time.Now(), func() int64 { sizeCalls.Add(1); return 0 }, func() {}))
	s, id := afActive(t, 1<<20)
	r.Register(s)
	nonce, err := s.NewHeartbeat(1, id)
	if err != nil {
		t.Fatal(err)
	}
	sizeCalls.Store(0)
	for i := 0; i < 200; i++ {
		if _, err := s.Ack(1, id, protocol.AckRequest{StreamID: id, DownDeliveredOffset: "0", UpConfirmedOffset: "0", HeartbeatNonce: nonce, ProbeID: "p"}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if n := sizeCalls.Load(); n > 10 {
		t.Fatalf("200 healthy ACKs caused %d retention passes", n)
	}
}

// During a resume barrier, a replay of the previous resume ID is
// told to retry and the old epoch cannot reserve a stream.
func TestBarrierGuards(t *testing.T) {
	s, _ := afActive(t, 1<<20)
	s.DropTransport(1)
	first, err := s.Resume(testReq("r1"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if first.Epoch != "2" {
		t.Fatalf("epoch %s", first.Epoch)
	}
	s.mu.Lock()
	s.resumeBusy, s.barrierPending, s.State = true, true, StateResuming
	s.mu.Unlock()
	if _, err := s.Resume(testReq("r1"), time.Second); err != ErrResumeInProgress {
		t.Fatalf("replay during barrier: %v", err)
	}
	if _, err := s.ReserveStream(2, "ws"); err != ErrNotActive {
		t.Fatalf("reserve during barrier: %v", err)
	}
}

// An ordinary backend write is not reported as receive backpressure.
func TestWriteInProgressIsNotRxBlocked(t *testing.T) {
	s, _ := testBlockedWriter(t)
	if a := s.Snapshot(); a.DataRxBlocked {
		t.Fatal("single in-flight backend write reported as data_rx_blocked")
	}
	s.mu.Lock()
	for i := 1; i < s.maxInflight; i++ {
		s.upPendingFrames[uint64(1000*i)] = &InflightUpFrame{Offset: uint64(1000 * i), Length: 1}
	}
	s.mu.Unlock()
	if a := s.Snapshot(); !a.DataRxBlocked {
		t.Fatal("full in-flight queue not reported as data_rx_blocked")
	}
	s.mu.Lock()
	s.upPendingFrames = map[uint64]*InflightUpFrame{}
	s.mu.Unlock()
}
