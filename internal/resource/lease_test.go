package resource

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// A lease that retains nothing is never evicted; eviction stops once the
// total fits.
func TestZeroSizedLeaseNotEvicted(t *testing.T) {
	r := NewRetention(100)
	var aEv, bEv atomic.Bool
	var a, b *RetentionLease
	a = NewRetentionLease(time.Now().Add(-time.Second), func() int64 { return 0 }, func() { aEv.Store(true); a.End() })
	b = NewRetentionLease(time.Now(), func() int64 { return 150 }, func() { bEv.Store(true); b.End() })
	r.Register(a)
	r.Register(b)
	if aEv.Load() || !bEv.Load() {
		t.Fatalf("evicted zero=%v big=%v", aEv.Load(), bEv.Load())
	}
	if used := r.EnforceUsed(); used != 0 {
		t.Fatalf("used=%d", used)
	}
}

// EnforceUsed returns the post-eviction total and skips the lock path
// entirely when no lease is registered.
func TestEnforceUsedFastPath(t *testing.T) {
	r := NewRetention(1000)
	if r.EnforceUsed() != 0 || r.Used() != 0 {
		t.Fatal("empty retention reported usage")
	}
	var calls atomic.Int64
	l := NewRetentionLease(time.Now(), func() int64 { calls.Add(1); return 40 }, func() {})
	r.Register(l)
	if got := r.EnforceUsed(); got != 40 {
		t.Fatalf("used=%d", got)
	}
	l.End()
	r.Enforce()
	calls.Store(0)
	for i := 0; i < 100; i++ {
		r.EnforceUsed()
	}
	if calls.Load() != 0 {
		t.Fatal("ended lease still swept")
	}
}

// Put only replaces the wake channel when a Take is waiting, and a
// waiting Take is still woken.
func TestPutWakesOnlyWaiters(t *testing.T) {
	b := New(1)
	ch := b.changed
	b.Try(1)
	b.Put(1)
	if b.changed != ch {
		t.Fatal("uncontended Put replaced the wake channel")
	}
	b.Try(1)
	done := make(chan error, 1)
	go func() { done <- b.Take(context.Background(), 1) }()
	for {
		b.mu.Lock()
		w := b.waiters
		b.mu.Unlock()
		if w == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	b.Put(1)
	select {
	case err := <-done:
		if err != nil || b.Used() != 1 {
			t.Fatalf("take: %v used=%d", err, b.Used())
		}
	case <-time.After(time.Second):
		t.Fatal("waiter not woken")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.waiters != 0 {
		t.Fatalf("waiters=%d", b.waiters)
	}
}

// Separate connection metadata pool and an ingress reserve
// that remote-peer (egress) admission cannot consume.
func TestRoleBudgets(t *testing.T) {
	m := NewManager(256<<20, 32<<20, 32<<20, 10)
	if m.ConnMetadata == nil || m.ConnMetadata.Limit() != m.InboundConnections.Limit()*ConnMetadataPerConn {
		t.Fatal("connection metadata pool not sized for every inbound slot")
	}
	for m.InboundConnections.Try(1) {
		if !m.ConnMetadata.Try(ConnMetadataPerConn) {
			t.Fatal("connection pool smaller than connection limit")
		}
	}
	if m.Metadata.Used() != 0 {
		t.Fatal("socket admission charged session metadata")
	}
	m.SetIngressReserve(3, 1<<20)
	n := 0
	for m.TryEgressSession() {
		n++
	}
	if n != 7 || !m.Sessions.Try(1) {
		t.Fatalf("egress took %d of 10 sessions; ingress reserve unusable", n)
	}
	if !m.TryEgressMetadata(m.Metadata.Limit()-(1<<20)) || m.TryEgressMetadata(1) || !m.Metadata.Try(1<<20) {
		t.Fatal("metadata ingress reserve not honoured")
	}
	if s, md := m.IngressReserve(); s != 3 || md != 1<<20 {
		t.Fatal("reserve not reported")
	}
	if New(5).TryKeep(1, -1) || !New(5).TryKeep(5, 0) || New(5).TryKeep(5, 1) {
		t.Fatal("TryKeep bounds")
	}
}
