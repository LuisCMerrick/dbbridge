package session

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
)

// afSink is a backend that accepts every write and never produces data.
type afSink struct{ net.Conn }

func (afSink) Write(b []byte) (int, error)      { return len(b), nil }
func (afSink) Read([]byte) (int, error)         { select {} }
func (afSink) Close() error                     { return nil }
func (afSink) SetWriteDeadline(time.Time) error { return nil }

func afWait(t *testing.T, what string, f func() bool) {
	t.Helper()
	until := time.Now().Add(2 * time.Second)
	for !f() {
		if time.Now().After(until) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func afSHA(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// afNormalCloseUntilFinalAck drives a WS session to the point where the ingress
// sends the final ACK: one upstream frame written but not yet confirmed, up FIN
// applied, the down frame and down FIN emitted. It returns the stream ID.
func afNormalCloseUntilFinalAck(t *testing.T, s *Session) string {
	t.Helper()
	st, err := s.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ack(1, st, protocol.AckRequest{StreamID: st, ReadyReceived: true, DownDeliveredOffset: "0", UpConfirmedOffset: "0"}); err != nil {
		t.Fatal(err)
	}
	data := []byte("hello")
	if _, _, _, err := s.IngestUpFrame(1, 0, 5, afSHA(data), data); err != nil {
		t.Fatal(err)
	}
	afWait(t, "up write", func() bool { return s.UpAckOffset() == 5 })
	if _, err := s.IngestUpFIN(1, 5); err != nil {
		t.Fatal(err)
	}
	afWait(t, "up fin", func() bool { u, _ := s.FINs(); return u.Applied })
	if !s.ReserveDownRead(32 << 10) {
		t.Fatal("reserve")
	}
	buf := make([]byte, 32<<10)
	copy(buf, "world")
	if _, err := s.FinishDownRead(buf[:5]); err != nil {
		t.Fatal(err)
	}
	s.MarkDownFinal(5)
	if err := s.BeginEmit(1, 0, 5); err != nil {
		t.Fatal(err)
	}
	s.FinishEmit(1, 5, nil)
	if err := s.BeginFinEmit(1); err != nil {
		t.Fatal(err)
	}
	s.FinishFinEmit(1, nil)
	return st
}

// The ACK that completes the normal close also carries the first
// confirmation of the last upstream frame; it must succeed, not BAD_ACK.
func TestFinalAckCompletesNormalClose(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	go io.Copy(io.Discard, b)
	s := NewSession("p", "t", "inst", a, 4<<20, 2<<20, 8, 64, true)
	defer s.Close()
	st := afNormalCloseUntilFinalAck(t, s)
	resp, err := s.Ack(1, st, protocol.AckRequest{StreamID: st, DownDeliveredOffset: "5", DownFinApplied: true, UpConfirmedOffset: "5"})
	if err != nil {
		t.Fatalf("final ACK: %v", err)
	}
	if !resp.DownFinAcked || !resp.UpFinApplied || s.GetState() != StateClosed || s.TerminalCode() != protocol.ErrSessionClosed {
		t.Fatalf("resp=%+v state=%s", resp, s.GetState())
	}
	// A retransmitted final ACK on the closed session still answers 200.
	if _, err := s.Ack(1, st, protocol.AckRequest{StreamID: st, DownDeliveredOffset: "5", DownFinApplied: true, UpConfirmedOffset: "5"}); err != nil {
		t.Fatalf("repeated final ACK: %v", err)
	}
}

// Closed sessions leave a compact tombstone; the default 32 MiB metadata
// now sustains several times more create/close cycles per TTL than the 3825
// measured with the old 8 KiB reservation.
func TestCreateChurnCapacity(t *testing.T) {
	m := resource.NewManager(256<<20, 32<<20, 32<<20, 200)
	r := NewRegistry(64<<20, 256<<20)
	r.SetResources(m.Payload, m.Metadata, 10*time.Minute)
	defer r.CloseAll()
	n := 0
	for ; n < 40000; n++ {
		key := fmt.Sprintf(`["peer","ing","req-%06d"]`, n)
		st, _, _, _ := r.ReserveCreate(key, "db", protocol.SessionLimits{}, protocol.RecoveryNegotiation{}, "ws")
		if st != CreateReserved {
			break
		}
		s := NewSession("p", "db", "i", afSink{}, 1024, 1024, 8, 64, false)
		r.Register(s)
		r.CompleteCreate(key, &protocol.CreateSessionResponse{SessionID: s.ID})
		s.Close()
	}
	t.Logf("create/close cycles within one TTL: %d (~%.1f/s over 10m)", n, float64(n)/600)
	if n < 15000 {
		t.Fatalf("only %d create/close cycles fit the default metadata budget", n)
	}
}

// The tombstone is a detached closed view that still answers the final
// ACK and DELETE paths. Normal and abnormal ends
// both keep tombstone and create record for the configured guarantee; there is
// no shorter normal-close TTL.
func TestTombstoneViewAndNormalRetention(t *testing.T) {
	metadata := resource.New(1 << 20)
	r := NewRegistry(1<<20, 4<<20)
	r.SetResources(nil, metadata, 10*time.Minute)
	defer r.CloseAll()
	started := time.Now()

	a, b := net.Pipe()
	defer b.Close()
	go io.Copy(io.Discard, b)
	normal := NewSession("p", "t", "inst", a, 4<<20, 2<<20, 8, 64, true)
	if st, _, _, _ := r.ReserveCreate("kn", "t", protocol.SessionLimits{}, protocol.RecoveryNegotiation{}, "ws"); st != CreateReserved {
		t.Fatal(st)
	}
	r.Register(normal)
	r.CompleteCreate("kn", &protocol.CreateSessionResponse{SessionID: normal.ID})
	st := afNormalCloseUntilFinalAck(t, normal)
	if _, err := normal.Ack(1, st, protocol.AckRequest{StreamID: st, DownDeliveredOffset: "5", DownFinApplied: true, UpConfirmedOffset: "5"}); err != nil {
		t.Fatal(err)
	}
	view := r.GetAny(normal.ID)
	if view == nil || view == normal || view.GetState() != StateClosed || view.BackendConn != nil || view.SessionToken != normal.SessionToken || view.GetEpoch() != 1 {
		t.Fatalf("tombstone view: %+v", view)
	}
	if _, err := view.Ack(1, st, protocol.AckRequest{StreamID: st, DownDeliveredOffset: "5", DownFinApplied: true, UpConfirmedOffset: "5"}); err != nil {
		t.Fatalf("final ACK on tombstone: %v", err)
	}
	if err := view.CloseWithEpoch(1); err != nil {
		t.Fatalf("DELETE on tombstone: %v", err)
	}
	select {
	case <-view.Done():
	default:
		t.Fatal("tombstone view not done")
	}

	abnormal := NewSession("p", "t", "inst", afSink{}, 1024, 1024, 8, 64, false)
	if st, _, _, _ := r.ReserveCreate("ka", "t", protocol.SessionLimits{}, protocol.RecoveryNegotiation{}, "ws"); st != CreateReserved {
		t.Fatal(st)
	}
	r.Register(abnormal)
	r.CompleteCreate("ka", &protocol.CreateSessionResponse{SessionID: abnormal.ID})
	abnormal.Close()

	for _, c := range []struct{ id, key string }{{normal.ID, "kn"}, {abnormal.ID, "ka"}} {
		if r.GetAny(c.id) == nil || r.GetCachedCreate(c.key) == nil {
			t.Fatalf("%s lost its tombstone or create record inside the guarantee", c.key)
		}
		r.mu.Lock()
		expiry, entry := r.tombstoneExpires[c.id], r.createIdempotency[c.key]
		r.mu.Unlock()
		if expiry.Before(started.Add(10*time.Minute)) || entry.retainUntil.Before(started.Add(10*time.Minute)) {
			t.Fatalf("%s retained until %v / %v; guarantee is 10m", c.key, expiry.Sub(started), entry.retainUntil.Sub(started))
		}
	}
	if want := int64(2*createRecordCost + len("kn") + len("ka") + 4 + 2*len("t")); metadata.Used() != want {
		t.Fatalf("metadata used=%d want %d (both records)", metadata.Used(), want)
	}
}

// A resume that reports the down FIN as applied while the up FIN is
// already applied closes the session normally instead of opening a new epoch.
func TestResumeWithBothFINsClosesNormally(t *testing.T) {
	s := NewSession("p", "db", "i", afSink{}, 1<<20, 1<<20, 8, 64, true)
	defer s.Close()
	s.Configure(5*time.Second, time.Minute, "ing", nil)
	id, _ := s.ReserveStream(1, "ws")
	if err := s.ActivateDataPlane(id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.IngestUpFIN(1, 0); err != nil {
		t.Fatal(err)
	}
	afWait(t, "up fin", func() bool { u, _ := s.FINs(); return u.Applied })
	s.MarkDownFinal(0)
	if err := s.BeginFinEmit(1); err != nil {
		t.Fatal(err)
	}
	s.FinishFinEmit(1, nil)
	s.DropTransport(1)
	zero := "0"
	req := testReq("r1")
	req.UpFIN = protocol.DirectionFIN{Seen: true, FinalOffset: &zero, Applied: true}
	req.DownFIN = protocol.DirectionFIN{Seen: true, FinalOffset: &zero, Applied: true}
	if _, err := s.Resume(req, time.Second); err != ErrSessionClosed {
		t.Fatalf("resume err=%v", err)
	}
	if s.GetState() != StateClosed || s.GetEpoch() != 1 {
		t.Fatalf("state=%s epoch=%d", s.GetState(), s.GetEpoch())
	}
	s.mu.Lock()
	reason := s.closeReason
	s.mu.Unlock()
	if reason != "closed_normal" || s.TerminalCategory() != "normal" {
		t.Fatalf("close reason %q", reason)
	}
}

// The create-time unattached timer does not close a resumed session.
func TestUnattachedTimerStoppedByResume(t *testing.T) {
	s := NewSession("p", "t", "inst", afSink{}, 4<<20, 2<<20, 8, 64, true)
	defer s.Close()
	s.Configure(10*time.Second, time.Second, "ing", nil)
	s.StartUnattachedTimer(50*time.Millisecond, func(x *Session) { x.Close() })
	if _, err := s.ReserveStream(1, "ws"); err != nil {
		t.Fatal(err)
	}
	s.DropTransport(1)
	if _, err := s.Resume(testReq("r1"), time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	if s.GetState() == StateClosed {
		t.Fatal("resumed session closed by the create-time unattached timer")
	}
}
