package session

import (
	"container/heap"
	"time"
)

// maxCreateIdempotencyEntries bounds the create idempotency table. Entries are
// also charged against the metadata budget when one is configured.
const maxCreateIdempotencyEntries = 10000

type expiryKind uint8

const (
	expireTombstone expiryKind = iota
	expireCreate
)

// expiryItem is a lazily invalidated deadline: when it is popped, the current
// map state decides whether it still applies, so a superseded item is skipped.
type expiryItem struct {
	at   time.Time
	kind expiryKind
	key  string // session ID for tombstones, idempotency key for creates
}

type expiryHeap []expiryItem

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h expiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *expiryHeap) Push(x any)        { *h = append(*h, x.(expiryItem)) }
func (h *expiryHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = expiryItem{}
	*h = old[:n-1]
	return it
}

func (r *Registry) pushExpiryLocked(kind expiryKind, key string, at time.Time) {
	heap.Push(&r.expiry, expiryItem{at: at, kind: kind, key: key})
}

// expireLocked removes every tombstone and idempotency entry whose deadline has
// passed. It only visits due heap items, so the cost is O(expired * log n).
func (r *Registry) expireLocked(now time.Time) {
	for len(r.expiry) > 0 && !now.Before(r.expiry[0].at) {
		it := heap.Pop(&r.expiry).(expiryItem)
		switch it.kind {
		case expireTombstone:
			deadline, ok := r.tombstoneExpires[it.key]
			if !ok || now.Before(deadline) {
				continue
			}
			delete(r.tombstoneExpires, it.key)
			delete(r.tombstones, it.key)
			if key, ok := r.createBySession[it.key]; ok {
				if entry, exists := r.createIdempotency[key]; exists {
					delete(r.createIdempotency, key)
					if r.metadata != nil {
						r.metadata.Put(entry.metadataCost)
					}
				}
				delete(r.createBySession, it.key)
			}
		case expireCreate:
			entry, ok := r.createIdempotency[it.key]
			if !ok {
				continue
			}
			live := entry.Response != nil && r.sessions[entry.Response.SessionID] != nil
			// In-flight and live entries are re-queued by FailCreate,
			// CompleteCreate or Unregister when they become expirable.
			deadline := entry.CreatedAt.Add(r.ttl)
			if !entry.retainUntil.IsZero() {
				deadline = entry.retainUntil
			}
			if entry.InFlight || live || now.Before(deadline) {
				continue
			}
			delete(r.createIdempotency, it.key)
			if entry.Response != nil {
				id := entry.Response.SessionID
				delete(r.tombstones, id)
				delete(r.tombstoneExpires, id)
				delete(r.createBySession, id)
			}
			if r.metadata != nil {
				r.metadata.Put(entry.metadataCost)
			}
		}
	}
}

func (r *Registry) scheduleExpiryLocked() {
	if r.cleanupTimer != nil {
		r.cleanupTimer.Stop()
		r.cleanupTimer = nil
	}
	if len(r.expiry) == 0 {
		return
	}
	next := r.expiry[0].at
	r.cleanupTimer = time.AfterFunc(max(time.Nanosecond, time.Until(next)), func() {
		r.mu.Lock()
		r.expireLocked(time.Now())
		r.scheduleExpiryLocked()
		r.mu.Unlock()
	})
}
