package session

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestReviewStaleAttachCannotCancelNewEpoch(t *testing.T) {
	s, _ := auditSession(t, true)
	s.SetTransportMode("ws")
	old, err := s.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	// The old handler is delayed between ReserveStream and cancellation binding.
	if _, err = s.Resume(auditReq("review-stale-attach"), time.Second); err != nil {
		t.Fatal(err)
	}
	current, err := s.ReserveStream(2, "ws")
	if err != nil {
		t.Fatal(err)
	}
	var newCanceled atomic.Bool
	if err = s.SetStreamCancel(2, current, func() { newCanceled.Store(true) }); err != nil {
		t.Fatal(err)
	}
	if err = s.SetStreamCancel(1, old, func() {}); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale registration: %v", err)
	}
	if newCanceled.Load() {
		t.Fatalf("late epoch-1 cancel registration canceled epoch %d transport", s.GetEpoch())
	}
}

func TestReviewStreamCancelBindingRejectsWrongStreamAndClosed(t *testing.T) {
	s, _ := auditSession(t, true)
	s.SetTransportMode("ws")
	id, err := s.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	var canceled atomic.Bool
	if err := s.SetStreamCancel(1, id, func() { canceled.Store(true) }); err != nil {
		t.Fatal(err)
	}
	if err := s.SetStreamCancel(1, "wrong-stream", func() {}); err == nil || canceled.Load() {
		t.Fatal("wrong stream changed cancellation ownership")
	}
	s.Close()
	if !canceled.Load() {
		t.Fatal("current stream not canceled on closure")
	}
	if err := s.SetStreamCancel(1, id, func() {}); err == nil {
		t.Fatal("closed session accepted new cancellation owner")
	}
}

func TestReviewStreamCancelCallbackRunsOutsideSessionLock(t *testing.T) {
	s, _ := auditSession(t, true)
	s.SetTransportMode("ws")
	id, err := s.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetStreamCancel(1, id, func() { _ = s.GetEpoch() }); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.SetStreamCancel(1, id, func() {}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel callback deadlocked on the session lock")
	}
}
