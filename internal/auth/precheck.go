package auth

import (
	"sync"
	"time"
)

const (
	precheckConcurrency = 2
	precheckBurst       = 4
	prechecksPerSecond  = 4
)

// The quota belongs to an authenticated identity snapshot. Ordinary credential
// rotation reuses it; removed identities leave no permanent admission-map entry.
type precheckQuota struct {
	mu      sync.Mutex
	active  int
	tokens  float64
	updated time.Time
}

func newPrecheckQuota() *precheckQuota {
	return &precheckQuota{tokens: precheckBurst, updated: time.Now()}
}

func (q *precheckQuota) reserve(now time.Time) (func(), bool) {
	q.mu.Lock()
	if elapsed := now.Sub(q.updated).Seconds(); elapsed > 0 {
		q.tokens = min(precheckBurst, q.tokens+elapsed*prechecksPerSecond)
		q.updated = now
	}
	if q.active >= precheckConcurrency || q.tokens < 1 {
		q.mu.Unlock()
		return nil, false
	}
	q.active++
	q.tokens--
	q.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			q.mu.Lock()
			q.active--
			q.mu.Unlock()
		})
	}, true
}

// ReservePrecheck enforces both concurrency and request rate on the server;
// an authenticated caller cannot bypass it by using a different Go client.
func (a *Authenticator) ReservePrecheck(peer string) (func(), bool) {
	a.mu.RLock()
	p := a.inboundPeers[peer]
	if p == nil || !p.Enabled {
		a.mu.RUnlock()
		return nil, false
	}
	quota := p.prechecks
	a.mu.RUnlock()
	return quota.reserve(time.Now())
}
