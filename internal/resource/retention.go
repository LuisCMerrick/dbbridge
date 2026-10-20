package resource

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Retention accounts all recovery states of both roles in one process. Callbacks
// run outside its lock and must validate the lease at the session's terminal gate.
type RetentionLease struct {
	active  atomic.Bool
	started time.Time
	size    func() int64
	evict   func()
}

func NewRetentionLease(start time.Time, size func() int64, evict func()) *RetentionLease {
	l := &RetentionLease{started: start, size: size, evict: evict}
	l.active.Store(true)
	return l
}
func (l *RetentionLease) End() {
	if l != nil {
		l.active.Store(false)
	}
}
func (l *RetentionLease) Active() bool { return l != nil && l.active.Load() }

type Retention struct {
	mu     sync.Mutex
	limit  int64
	leases map[*RetentionLease]struct{}
	count  atomic.Int64 // len(leases), readable without mu
}

func NewRetention(limit int64) *Retention {
	return &Retention{limit: limit, leases: map[*RetentionLease]struct{}{}}
}
func (r *Retention) Register(l *RetentionLease) {
	if l == nil {
		return
	}
	r.mu.Lock()
	if l.Active() {
		r.leases[l] = struct{}{}
		r.count.Store(int64(len(r.leases)))
	}
	r.mu.Unlock()
	r.Enforce()
}
func (r *Retention) Used() int64 {
	if r.count.Load() == 0 {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var total int64
	for l := range r.leases {
		if l.Active() {
			total += l.size()
		}
	}
	return total
}
func (r *Retention) Limit() int64 { return r.limit }

// Enforce evicts recovery states until the retained total fits the limit.
func (r *Retention) Enforce() { r.EnforceUsed() }

// EnforceUsed is Enforce that also returns the retained total after eviction,
// sparing callers a second locked pass through Used. With no registered
// lease it returns without taking the lock.
//
// Policy: leases are considered oldest recovery first, and only a lease
// that currently retains bytes is evicted; evicting an empty one frees nothing.
// Each lease's size is read once per pass so the bookkeeping stays consistent.
func (r *Retention) EnforceUsed() int64 {
	if r.count.Load() == 0 {
		return 0
	}
	type entry struct {
		l  *RetentionLease
		sz int64
	}
	r.mu.Lock()
	var total int64
	var live []entry
	for l := range r.leases {
		if !l.Active() {
			delete(r.leases, l)
			continue
		}
		sz := l.size()
		total += sz
		live = append(live, entry{l, sz})
	}
	var evicted []*RetentionLease
	if r.limit > 0 && total > r.limit {
		sort.SliceStable(live, func(i, j int) bool { return live[i].l.started.Before(live[j].l.started) })
		for _, e := range live {
			if total <= r.limit {
				break
			}
			if e.sz <= 0 {
				continue
			}
			total -= e.sz
			delete(r.leases, e.l)
			evicted = append(evicted, e.l)
		}
	}
	r.count.Store(int64(len(r.leases)))
	r.mu.Unlock()
	for _, l := range evicted {
		l.evict()
	}
	return total
}
