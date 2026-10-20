package resource

import (
	"context"
	"testing"
	"time"
)

func TestShareIsBoundedByItsLimitAndParent(t *testing.T) {
	parent := New(100)
	a := NewShare(parent, 60, nil)
	b := NewShare(parent, 60, nil)
	if !a.Try(60) || a.Try(1) {
		t.Fatal("share limit not applied")
	}
	if b.Try(41) || !b.Try(40) {
		t.Fatal("parent limit not applied")
	}
	if parent.Used() != 100 || a.Used() != 60 || b.Used() != 40 {
		t.Fatalf("used parent=%d a=%d b=%d", parent.Used(), a.Used(), b.Used())
	}
	a.Put(60)
	b.Put(40)
	if parent.Used() != 0 || a.Used() != 0 || b.Used() != 0 {
		t.Fatal("release did not reach both budgets")
	}
}

func TestShareLeavesParentReserve(t *testing.T) {
	parent := New(100)
	keep := int64(25)
	s := NewShare(parent, 100, func() int64 { return keep })
	if s.Try(76) || !s.Try(75) {
		t.Fatal("share took the parent reserve")
	}
	if !parent.Try(25) {
		t.Fatal("direct parent user cannot use the reserve")
	}
	parent.Put(25)
	if s.TryKeep(1, 0) {
		t.Fatal("TryKeep on a share ignored the parent reserve")
	}
	if e := s.Take(context.Background(), 76); e != ErrLimit {
		t.Fatalf("Take beyond the reserve: %v", e)
	}
}

func TestShareTakeWakesOnParentRelease(t *testing.T) {
	parent := New(10)
	s := NewShare(parent, 10, nil)
	if !parent.Try(10) {
		t.Fatal("setup")
	}
	done := make(chan error, 1)
	go func() { done <- s.Take(context.Background(), 5) }()
	time.Sleep(20 * time.Millisecond)
	parent.Put(5)
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter on a share not woken by a release of its parent")
	}
}

func TestSetLimitLowersAndRaisesShare(t *testing.T) {
	parent := New(100)
	s := NewShare(parent, 10, nil)
	if !s.Try(10) {
		t.Fatal("setup")
	}
	s.SetLimit(5)
	s.Put(10)
	if s.Try(6) || !s.Try(5) {
		t.Fatal("lowered limit not applied")
	}
	s.SetLimit(50)
	if !s.Try(45) {
		t.Fatal("raised limit not applied")
	}
}
