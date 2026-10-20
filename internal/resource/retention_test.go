package resource

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestRetentionSharesBudgetAndEvictsFirstFault(t *testing.T) {
	budget := NewRetention(200)
	var egress, ingress atomic.Int64
	egress.Store(150)
	ingress.Store(150)
	var a, b *RetentionLease
	a = NewRetentionLease(time.Now().Add(-time.Second), egress.Load, func() { a.End(); egress.Store(0) })
	b = NewRetentionLease(time.Now(), ingress.Load, func() { b.End(); ingress.Store(0) })
	budget.Register(a)
	budget.Register(b)
	if a.Active() || !b.Active() || budget.Used() != 150 {
		t.Fatalf("FIFO/shared quota failed: first=%t second=%t used=%d", a.Active(), b.Active(), budget.Used())
	}
	b.End()
	budget.Enforce()
	if budget.Used() != 0 {
		t.Fatal("completed recovery lease leaked")
	}
}
func TestLateRetentionRegistrationCannotEvictHealthySession(t *testing.T) {
	budget := NewRetention(1)
	var evicted atomic.Bool
	l := NewRetentionLease(time.Now(), func() int64 { return 100 }, func() { evicted.Store(true) })
	l.End()
	budget.Register(l)
	if evicted.Load() || budget.Used() != 0 {
		t.Fatal("completed cycle was re-added by a late callback")
	}
}
