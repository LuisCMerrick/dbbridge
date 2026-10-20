package session

import (
	"crypto/sha256"
	"dbbridge/internal/protocol"
	"encoding/hex"
	"net"
	"testing"
	"time"
)

func auditSession(t *testing.T, recovery bool) (*Session, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	s := NewSession("p", "db", "inst", a, 1024, 1024, 8, 64, recovery)
	t.Cleanup(func() { s.Close(); b.Close() })
	return s, b
}
func auditReq(id string) *protocol.ResumeRequest {
	return &protocol.ResumeRequest{ResumeRequestID: id, ExpectedEpoch: "1", IngressInstanceID: "ing", Transport: protocol.TransportMode{Mode: "ws"}, KnownUpAckOffset: "0", UpNextOffset: "0", DownDeliveredOffset: "0"}
}
func TestRegressionWriteBeforeReady(t *testing.T) {
	s, b := auditSession(t, false)
	p := []byte("x")
	h := sha256.Sum256(p)
	_, _, _, err := s.IngestUpFrame(1, 0, 1, hex.EncodeToString(h[:]), p)
	if err != nil {
		return
	}
	b.SetReadDeadline(time.Now().Add(80 * time.Millisecond))
	buf := make([]byte, 1)
	n, _ := b.Read(buf)
	if n != 0 {
		t.Fatalf("backend received %q before any stream reservation/ready ACK", buf[:n])
	}
}
func TestRegressionSameEpochSlotReuse(t *testing.T) {
	s, _ := auditSession(t, true)
	s.SetTransportMode("ws")
	first, e := s.ReserveStream(1, "ws")
	if e != nil {
		t.Fatal(e)
	}
	s.ActivateDataPlane(first)
	s.Suspend(time.Second, nil)
	second, err := s.ReserveStream(1, "ws")
	if err == nil {
		t.Fatalf("used epoch 1 slot reopened: first=%s second=%s", first, second)
	}
}
func TestRegressionAckUnemittedData(t *testing.T) {
	s, _ := auditSession(t, true)
	s.AppendDownBytes([]byte("not sent"))
	err := s.AdvanceDownAckAtomic(1, 8, false)
	if err == nil {
		t.Fatalf("accepted ACK for 8 never-emitted bytes; retained=%d", s.BufferSize())
	}
}
func TestRegressionForgedUpConfirmation(t *testing.T) {
	s, _ := auditSession(t, true)
	s.ConfirmUpAdvancesReceiptFloor(999)
	if floor := s.receiptTable.ReceiptFloor(); floor > s.UpAckOffset() {
		t.Fatalf("receipt floor=%d exceeds written ACK=%d", floor, s.UpAckOffset())
	}
}
func TestRegressionResumeWithoutDeadline(t *testing.T) {
	s, _ := auditSession(t, true)
	s.SetTransportMode("ws")
	id, _ := s.ReserveStream(1, "ws")
	s.ActivateDataPlane(id)
	resp, err := s.Resume(auditReq("r1"), 20*time.Millisecond)
	if err == nil && s.ResumeDeadline.IsZero() {
		t.Fatalf("resume advanced to epoch=%s with no recovery/activation deadline", resp.Epoch)
	}
}
func TestRegressionResumeIdempotencyDownFIN(t *testing.T) {
	s, _ := auditSession(t, true)
	s.SetTransportMode("ws")
	s.Suspend(time.Second, nil)
	_, err := s.Resume(auditReq("r1"), 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	changed := auditReq("r1")
	changed.DownFIN.Seen = true
	v := "99"
	changed.DownFIN.FinalOffset = &v
	_, err = s.Resume(changed, 20*time.Millisecond)
	if err == nil {
		t.Fatal("same resume ID accepted conflicting down_fin seen/final_offset")
	}
}
func TestRegressionSuspendQuotaDeadlock(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	s := NewSession("p", "db", "inst", a, 1024, 1024, 8, 64, true)
	r := NewRegistry(1, 1024)
	r.Register(s)
	s.Suspend(time.Second, nil)
	r.MarkSuspended(s)
	done := make(chan struct{})
	go func() { s.AppendDownBytes([]byte("xx")); close(done) }()
	select {
	case <-done:
		s.Close()
	case <-time.After(100 * time.Millisecond):
		t.Fatal("AppendDownBytes deadlocked while suspended quota eviction closes the same session")
	}
}
func TestRegressionInflightPartialOverlap(t *testing.T) {
	s, b := auditSession(t, true)
	s.SetTransportMode("ws")
	id, _ := s.ReserveStream(1, "ws")
	s.ActivateDataPlane(id)
	p := []byte("abcd")
	h := sha256.Sum256(p)
	s.IngestUpFrame(1, 0, 4, hex.EncodeToString(h[:]), p)
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		writing := s.inflightWriting
		s.mu.Unlock()
		if writing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writer not started")
		}
		time.Sleep(time.Millisecond)
	}
	q := []byte("ZZ")
	h2 := sha256.Sum256(q)
	_, accepted, _, err := s.IngestUpFrame(1, 2, 2, hex.EncodeToString(h2[:]), q)
	b.Close()
	if err == nil && accepted {
		t.Fatal("accepted frame [2,4) overlapping inflight [0,4)")
	}
}
func TestRegressionConcurrentResumeSingleCommit(t *testing.T) {
	s, b := auditSession(t, true)
	s.SetTransportMode("ws")
	id, _ := s.ReserveStream(1, "ws")
	s.ActivateDataPlane(id)
	p := []byte("abcd")
	h := sha256.Sum256(p)
	s.IngestUpFrame(1, 0, 4, hex.EncodeToString(h[:]), p)
	until := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		busy := s.inflightWriting
		s.mu.Unlock()
		if busy {
			break
		}
		if time.Now().After(until) {
			t.Fatal("not writing")
		}
		time.Sleep(time.Millisecond)
	}
	results := make(chan error, 2)
	for _, id := range []string{"r1", "r2"} {
		req := auditReq(id)
		req.UpNextOffset = "4"
		go func() { _, err := s.Resume(req, time.Second); results <- err }()
	}
	// Both recovery requests wait on the same in-flight write. Let both reach the barrier.
	time.Sleep(30 * time.Millisecond)
	buf := make([]byte, 4)
	b.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := b.Read(buf); err != nil {
		t.Fatal(err)
	}
	successes := 0
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			if err == nil {
				successes++
			}
		case <-time.After(time.Second):
			t.Fatal("resume stuck")
		}
	}
	if successes > 1 || s.GetEpoch() > 2 {
		t.Fatalf("two controllers committed %d recoveries, final epoch=%d", successes, s.GetEpoch())
	}
}
func TestRegressionActiveCreateMappingExpiry(t *testing.T) {
	s, _ := auditSession(t, true)
	r := NewRegistry(1024, 8192)
	r.Register(s)
	lim := protocol.SessionLimits{}
	rec := protocol.RecoveryNegotiation{}
	r.ReserveCreate("key", "db", lim, rec, "ws")
	r.CompleteCreate("key", &protocol.CreateSessionResponse{SessionID: s.ID})
	r.mu.Lock()
	r.createIdempotency["key"].CreatedAt = time.Now().Add(-11 * time.Minute)
	r.mu.Unlock()
	status, _, _, _ := r.ReserveCreate("key", "db", lim, rec, "ws")
	if status == CreateReserved {
		t.Fatal("same create ID re-reserved while original session still active after 10 minutes")
	}
}
func TestRegressionNormalCompletionFinalizes(t *testing.T) {
	s, _ := auditSession(t, false)
	r := NewRegistry(1024, 8192)
	r.Register(s)
	s.SetTransportMode("ws")
	id, _ := s.ReserveStream(1, "ws")
	s.ActivateDataPlane(id)
	s.IngestUpFIN(1, 0)
	until := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		applied := s.UpFIN.Applied
		s.mu.Unlock()
		if applied {
			break
		}
		if time.Now().After(until) {
			t.Fatal("FIN not applied")
		}
		time.Sleep(time.Millisecond)
	}
	s.MarkDownFinal(0)
	s.BeginFinEmit(1)
	s.FinishFinEmit(1, nil)
	s.AdvanceDownAckAtomic(1, 0, true)
	if s.GetState() != StateClosed {
		t.Fatal("not closed")
	}
	if r.Get(s.ID) != nil {
		t.Fatal("normal completion left CLOSED session in live registry")
	}
}
