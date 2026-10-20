package egress

import (
	"sync"
	"time"
)

// idemTracker bounds, per peer, the create records that peer makes the shared
// idempotency table retain. Every record of a new create_request_id is
// counted from admission until the registry drops it: while the create is in
// flight, while its session is live, and for idempotency_tombstone_ttl after
// the create failed or the session closed. Two limits apply:
//
//   - all of the peer's records together stay within its share of the create
//     record budget, so one peer's create/close churn cannot refuse every
//     other peer's creates for the tombstone TTL;
//   - in-flight and failed records alone stay within peerIdemLimit, so a peer
//     cannot fill its share with records that never held a session.
//
// Deterministic refusals (unknown or forbidden target, invalid limits) and
// transient ones (quota, budgets) never reach the table and are not counted.
//
// Ownership of a counted key follows the table reservation, not admission:
// concurrent requests with the same key share one entry (refs), only the
// request that reserved the table record decides its fate (claim), and an
// entry no request claimed disappears when the last reference is released.
type idemTracker struct {
	mu    sync.Mutex
	peers map[string]map[string]*idemEntry
}

type idemState uint8

const (
	idemPending idemState = iota // admitted or reserved, outcome not known yet
	idemLive                     // the create succeeded and its session is open
	idemFailed                   // the create failed; kept until the record expires
	idemClosed                   // the session closed; kept until the record expires
)

type idemEntry struct {
	refs  int  // requests currently holding the key
	owned bool // a request reserved the table record for this key
	state idemState
	until time.Time // expiry of a failed or closed record
}

// idemLimits are the per-peer bounds checked when a new key is admitted.
type idemLimits struct {
	total   int // every record of the peer
	pending int // in-flight and failed records of the peer
}

func newIdemTracker() *idemTracker {
	return &idemTracker{peers: map[string]map[string]*idemEntry{}}
}

// peerIdemLimit is the per-peer number of retained failed/in-flight records.
func peerIdemLimit(maxSessions int) int {
	return max(64, 4*maxSessions)
}

// admit takes a reference on key for peer. A key the tracker already counts
// is always admitted and reported as not added; a new key is admitted only
// within lim. The caller must call release once for every successful admit.
func (t *idemTracker) admit(peer, key string, lim idemLimits, now time.Time) (added, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	keys := t.peers[peer]
	if keys == nil {
		keys = map[string]*idemEntry{}
		t.peers[peer] = keys
	}
	pending := 0
	for k, e := range keys {
		if e.refs == 0 && !e.until.IsZero() && !now.Before(e.until) {
			delete(keys, k)
			continue
		}
		if e.state == idemPending || e.state == idemFailed {
			pending++
		}
	}
	if e := keys[key]; e != nil {
		e.refs++
		return false, true
	}
	if len(keys) >= lim.total || pending >= lim.pending {
		if len(keys) == 0 {
			delete(t.peers, peer)
		}
		return false, false
	}
	keys[key] = &idemEntry{refs: 1}
	return true, true
}

func (t *idemTracker) entry(peer, key string) *idemEntry {
	if keys := t.peers[peer]; keys != nil {
		return keys[key]
	}
	return nil
}

// claim records that the caller reserved the table record for key.
func (t *idemTracker) claim(peer, key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e := t.entry(peer, key); e != nil {
		e.owned = true
	}
}

// live records that the claimed create succeeded and registered its session.
func (t *idemTracker) live(peer, key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e := t.entry(peer, key); e != nil && e.state == idemPending {
		e.state = idemLive
	}
}

// failed keeps key counted as a failed record until until.
func (t *idemTracker) failed(peer, key string, until time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e := t.entry(peer, key); e != nil {
		e.owned, e.state, e.until = true, idemFailed, until
	}
}

// closed keeps the record of a finished session counted until until.
func (t *idemTracker) closed(peer, key string, until time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e := t.entry(peer, key); e != nil {
		e.owned, e.state, e.until = true, idemClosed, until
	}
}

// release drops one reference. With its last reference a key is forgotten
// unless a request claimed it and recorded an outcome (live, failed or
// closed); such a key stays counted until its record expires.
func (t *idemTracker) release(peer, key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	keys := t.peers[peer]
	e := keys[key]
	if e == nil {
		return
	}
	e.refs--
	if e.refs > 0 {
		return
	}
	e.refs = 0
	if !e.owned || e.state == idemPending {
		delete(keys, key)
		if len(keys) == 0 {
			delete(t.peers, peer)
		}
	}
}

// count returns how many records peer is charged for.
func (t *idemTracker) count(peer string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.peers[peer])
}
