package auth

import "sync"

// inflightQuota counts one peer's in-flight requests per operation kind
// (probe, create, resume). It survives credential reloads like precheckQuota.
type inflightQuota struct {
	mu     sync.Mutex
	active map[string]int64
}

func newInflightQuota() *inflightQuota { return &inflightQuota{active: map[string]int64{}} }

// PeerShare is the largest part of a process-wide budget of size global that
// one authenticated peer may hold when several peers are enabled. Every peer
// is guaranteed a floor of global/(2*peers) (at least one unit): any
// peers-1 greedy peers together can hold at most global-floor, so the last
// peer always finds its floor free. With two peers this is the
// previous "leave a quarter" rule. With a single enabled peer, or a budget
// too small to partition, the whole budget is available.
func PeerShare(global int64, peers int) int64 {
	if peers <= 1 || global <= 1 {
		return global
	}
	n := int64(peers)
	floor := max(1, global/(2*n))
	return max(1, (global-floor)/(n-1))
}

// ReserveFair admits one in-flight operation of the given kind for peer
// unless that peer already holds its share of the global budget. The caller
// must still take the global budget itself; this only partitions it.
func (a *Authenticator) ReserveFair(peer, kind string, global int64) (func(), bool) {
	return a.ReserveFairN(peer, kind, global, 1)
}

// ReserveFairN is ReserveFair for an amount n of a divisible budget (bytes of
// metadata or codec memory). n larger than the peer's share is refused.
func (a *Authenticator) ReserveFairN(peer, kind string, global, n int64) (func(), bool) {
	if n < 0 {
		n = 0
	}
	a.mu.RLock()
	p := a.inboundPeers[peer]
	if p == nil || !p.Enabled {
		a.mu.RUnlock()
		return nil, false
	}
	enabled := a.enabledLocked()
	q := p.inflight
	a.mu.RUnlock()
	limit := PeerShare(global, enabled)
	q.mu.Lock()
	if q.active[kind]+n > limit {
		q.mu.Unlock()
		return nil, false
	}
	q.active[kind] += n
	q.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			q.mu.Lock()
			q.active[kind] -= n
			if q.active[kind] <= 0 {
				delete(q.active, kind)
			}
			q.mu.Unlock()
		})
	}, true
}

func (a *Authenticator) enabledLocked() int {
	enabled := 0
	for _, v := range a.inboundPeers {
		if v.Enabled {
			enabled++
		}
	}
	return enabled
}

// PeerShareOf returns PeerShare(global, n) for the inbound peers enabled in
// the current snapshot, or 0 when peer is unknown or disabled.
func (a *Authenticator) PeerShareOf(peer string, global int64) int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if p := a.inboundPeers[peer]; p == nil || !p.Enabled {
		return 0
	}
	return PeerShare(global, a.enabledLocked())
}
