package session

import (
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"strings"
	"testing"
	"time"
	"unsafe"
)

func TestResumeIDRetainedOutsideTombstoneBudget(t *testing.T) {
	s, _ := testSession(t, true)
	metadata := resource.New(1 << 20)
	r := NewRegistry(1<<20, 4<<20)
	r.SetResources(nil, metadata, 10*time.Minute)
	defer r.CloseAll()
	if status, _, _, _ := r.ReserveCreate("create-key", "db", protocol.SessionLimits{}, protocol.RecoveryNegotiation{}, "ws"); status != CreateReserved {
		t.Fatal(status)
	}
	r.Register(s)
	r.CompleteCreate("create-key", &protocol.CreateSessionResponse{SessionID: s.ID})
	// Production active-session metadata is returned by onFinalize. Its create
	// record is the remaining reservation during the tombstone guarantee.
	q := testReq(strings.Repeat("r", 12<<10))
	if _, err := s.Resume(q, time.Second); err == nil {
		t.Fatal("accepted a 12 KiB resume ID")
	}
	if s.GetEpoch() != 1 {
		t.Fatal("rejected request changed the epoch")
	}
	q = testReq(strings.Repeat("r", 128))
	response, err := s.Resume(q, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := s.Resume(q, time.Second); err != nil || replay != response {
		t.Fatalf("bounded resume ID lost idempotency: %v", err)
	}
	charged := metadata.Used()
	s.Close()
	tombstone := r.GetAny(s.ID)
	if tombstone == nil {
		t.Fatal("tombstone missing")
	}
	retained := len(tombstone.lastResumeRequestID)
	t.Logf("resume ID retained=%d bytes; tombstone/create metadata charged=%d bytes", retained, metadata.Used())
	if int64(retained) > metadata.Used() {
		t.Error("resume ID alone exceeds the tombstone reservation after active-session metadata is released")
	}
	minimum := int64(unsafe.Sizeof(*tombstone)) + int64(retained)
	if retained != 128 || metadata.Used() != charged || charged < minimum {
		t.Fatal("tombstone lost its bounded resume ID or persistent metadata reservation")
	}
}

func TestResumeTombstoneReservationRefunded(t *testing.T) {
	s, _ := testSession(t, true)
	metadata := resource.New(1 << 20)
	r := NewRegistry(1<<20, 4<<20)
	r.SetResources(nil, metadata, 20*time.Millisecond)
	defer r.CloseAll()
	if status, _, _, _ := r.ReserveCreate("key", "db", protocol.SessionLimits{}, protocol.RecoveryNegotiation{}, "ws"); status != CreateReserved {
		t.Fatal(status)
	}
	r.Register(s)
	r.CompleteCreate("key", &protocol.CreateSessionResponse{SessionID: s.ID})
	if _, err := s.Resume(testReq(strings.Repeat("r", 128)), time.Second); err != nil {
		t.Fatal(err)
	}
	s.Close()
	until := time.Now().Add(time.Second)
	for metadata.Used() != 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if metadata.Used() != 0 || r.GetAny(s.ID) != nil || r.GetCachedCreate("key") != nil {
		t.Fatal("expired tombstone leaked its persistent metadata reservation")
	}
}
