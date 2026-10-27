package session

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"testing"
	"time"

	"dbbridge/internal/resource"
)

// queuedData returns the buffer the session holds for the frame at off,
// whether it is pending or being written.
func queuedData(s *Session, off uint64) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.upPendingFrames[off]; f != nil {
		return f.Data
	}
	if f := s.inflightFrame; f != nil && f.Offset == off {
		return f.Data
	}
	return nil
}

func activeTestSession(t *testing.T) *Session {
	a, b := net.Pipe() // nobody reads b: the first frame stays in flight
	s := NewSession("p", "db", "inst", a, 1<<20, 1<<20, 8, 64, false)
	t.Cleanup(func() { s.Close(); b.Close() })
	s.SetTransportMode("ws")
	id, _ := s.ReserveStream(1, "ws")
	s.ActivateDataPlane(id)
	return s
}

func TestIngestUpFrameOwnedAdoptsBuffer(t *testing.T) {
	s := activeTestSession(t)
	owned := []byte("owned-frame")
	sum := sha256.Sum256(owned)
	if _, ok, _, _, err := s.IngestUpFrameOwned(1, 0, hex.EncodeToString(sum[:]), owned, false); err != nil || !ok {
		t.Fatal(ok, err)
	}
	copied := []byte("copied")
	sum = sha256.Sum256(copied)
	if _, ok, _, err := s.IngestUpFrame(1, uint64(len(owned)), len(copied), hex.EncodeToString(sum[:]), copied); err != nil || !ok {
		t.Fatal(ok, err)
	}
	until := time.Now().Add(time.Second)
	for queuedData(s, 0) == nil && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if got := queuedData(s, 0); len(got) == 0 || &got[0] != &owned[0] {
		t.Fatal("owned frame was copied")
	}
	if got := queuedData(s, uint64(len(owned))); len(got) == 0 || &got[0] == &copied[0] {
		t.Fatal("caller buffer of IngestUpFrame was adopted")
	}
}

func TestIngestUpFrameStillVerifiesDigest(t *testing.T) {
	s := activeTestSession(t)
	if _, _, _, err := s.IngestUpFrame(1, 0, 1, hex.EncodeToString(make([]byte, 32)), []byte("x")); err != ErrFrameConflict {
		t.Fatalf("bad digest: %v", err)
	}
}

func TestWatchDownReserveWakesOnRelease(t *testing.T) {
	s := activeTestSession(t)
	if _, _, stop, ok := s.WatchDownReserve(); ok {
		stop()
		t.Fatal("session without a payload budget claims to notify")
	}
	payload := resource.New(100)
	r := NewRegistry(0, 0)
	r.SetResources(payload, resource.New(1<<20), 0)
	s.SetRegistry(r)
	if !payload.Try(100) {
		t.Fatal("setup")
	}
	ch, pch, stop, ok := s.WatchDownReserve()
	defer stop()
	if !ok {
		t.Fatal("payload budget does not notify")
	}
	if s.ReserveDownRead(10) {
		t.Fatal("reserved past an exhausted global budget")
	}
	go payload.Put(50)
	select {
	case <-ch:
	case <-pch:
	case <-time.After(time.Second):
		t.Fatal("no wake-up on release")
	}
	if !s.ReserveDownRead(10) {
		t.Fatal("reserve after release")
	}
}
