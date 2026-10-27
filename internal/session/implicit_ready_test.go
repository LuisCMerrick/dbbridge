package session

import (
	"errors"
	"testing"
	"time"
)

// An upstream frame naming the stream activates the initial attach (the
// ready message alone carries the stream ID), but never a resumed epoch, a
// wrong stream or a session whose initial attach expired.
func TestActivateImplicit(t *testing.T) {
	s := NewSession("p", "t", "inst", afSink{}, 4<<20, 2<<20, 8, 64, true)
	defer s.Close()
	s.Configure(10*time.Second, time.Second, "ing", nil)
	s.StartUnattachedTimer(time.Minute, nil)
	stream, err := s.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateImplicit(1, "other"); !errors.Is(err, ErrNotActive) {
		t.Fatalf("wrong stream: %v", err)
	}
	if err := s.ActivateImplicit(2, stream); err == nil {
		t.Fatal("wrong epoch activated")
	}
	if err := s.ActivateImplicit(1, stream); err != nil {
		t.Fatal(err)
	}
	if !s.IsDataPlaneActive() {
		t.Fatal("not active")
	}
	if err := s.ActivateImplicit(1, stream); err != nil {
		t.Fatalf("repeat: %v", err)
	}

	// A resumed epoch still needs its ready ACK.
	s.DropTransport(1)
	if _, err := s.Resume(testReq("r1"), time.Second); err != nil {
		t.Fatal(err)
	}
	stream2, err := s.ReserveStream(2, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateImplicit(2, stream2); !errors.Is(err, ErrNotActive) {
		t.Fatalf("resumed epoch activated implicitly: %v", err)
	}
}
