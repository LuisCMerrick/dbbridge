package session

import (
	"sync"
	"sync/atomic"
	"time"

	"dbbridge/internal/observability"
	"dbbridge/internal/resource"
)

// Registry manages active sessions, suspended LRU list, and tombstone records.
type Registry struct {
	mu                   sync.Mutex
	sessions             map[string]*Session
	createIdempotency    map[string]*CreateIdempEntry // key -> entry
	retention            *resource.Retention
	suspendedBufferLimit int64
	globalBufferLimit    int64
	payload              *resource.Budget
	metadata             *resource.Budget
	cleanupTimer         *time.Timer
	tombstoneExpires     map[string]time.Time
	createBySession      map[string]string
	tombstones           map[string]*tombstone
	expiry               expiryHeap // deadlines of tombstones and idempotency entries
	ttl                  time.Duration
	draining             bool
	maxSessions          int
	pendingCreates       int // in-flight creates
	currentGlobalMem     int64
}

func NewRegistry(suspendedBufferLimit, globalBufferLimit int64) *Registry {
	return &Registry{
		sessions:   make(map[string]*Session),
		tombstones: make(map[string]*tombstone), ttl: 10 * time.Minute,
		tombstoneExpires: map[string]time.Time{}, createBySession: map[string]string{},
		createIdempotency:    make(map[string]*CreateIdempEntry),
		retention:            resource.NewRetention(suspendedBufferLimit),
		suspendedBufferLimit: suspendedBufferLimit,
		globalBufferLimit:    globalBufferLimit,
	}
}

func (r *Registry) SetMaxSessions(max int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.maxSessions = max
}

func (r *Registry) AcquireGlobalBuffer(bytes int64) bool {
	if r.payload != nil {
		ok := r.payload.Try(bytes)
		atomic.StoreInt64(&observability.DefaultMetrics.BufferMemoryBytes, r.payload.Used())
		return ok
	}
	if r.globalBufferLimit <= 0 {
		atomic.AddInt64(&r.currentGlobalMem, bytes)
		atomic.StoreInt64(&observability.DefaultMetrics.BufferMemoryBytes, atomic.LoadInt64(&r.currentGlobalMem))
		return true
	}
	for {
		cur := atomic.LoadInt64(&r.currentGlobalMem)
		if cur+bytes > r.globalBufferLimit {
			return false
		}
		if atomic.CompareAndSwapInt64(&r.currentGlobalMem, cur, cur+bytes) {
			atomic.StoreInt64(&observability.DefaultMetrics.BufferMemoryBytes, cur+bytes)
			return true
		}
	}
}

func (r *Registry) ReleaseGlobalBuffer(bytes int64) {
	if r.payload != nil {
		r.payload.Put(bytes)
		atomic.StoreInt64(&observability.DefaultMetrics.BufferMemoryBytes, r.payload.Used())
		return
	}
	for {
		cur := atomic.LoadInt64(&r.currentGlobalMem)
		newVal := max(cur-bytes, 0)
		if atomic.CompareAndSwapInt64(&r.currentGlobalMem, cur, newVal) {
			if cur < bytes {
				resource.RecordUnbalancedRelease(cur, bytes)
			}
			atomic.StoreInt64(&observability.DefaultMetrics.BufferMemoryBytes, newVal)
			return
		}
	}
}

func (r *Registry) CurrentGlobalBuffer() int64 {
	if r.payload != nil {
		return r.payload.Used()
	}
	return atomic.LoadInt64(&r.currentGlobalMem)
}

func (r *Registry) Register(s *Session) {
	s.SetRegistry(r)
	r.mu.Lock()
	r.sessions[s.ID] = s
	r.mu.Unlock()
}

func (r *Registry) Get(id string) *Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sessions[id]
}

func (r *Registry) GetAllSessions() []*Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	res := make([]*Session, 0, len(r.sessions))
	for _, s := range r.sessions {
		res = append(res, s)
	}
	return res
}

func (r *Registry) Unregister(id string) *Session {
	r.mu.Lock()
	s := r.sessions[id]
	r.mu.Unlock()
	if s == nil {
		r.UpdateSuspended()
		return nil
	}
	// Snapshot outside r.mu: Registry.mu is never held while taking Session.mu.
	tb := s.tombstoneSnapshot()
	r.mu.Lock()
	if r.sessions[id] == s {
		delete(r.sessions, id)
		if r.tombstones == nil {
			if r.cleanupTimer != nil {
				r.cleanupTimer.Stop()
				r.cleanupTimer = nil
			}
			r.tombstoneExpires = map[string]time.Time{}
			r.createBySession = map[string]string{}
			r.tombstones = map[string]*tombstone{}
			r.expiry = nil
		}
		now := time.Now()
		// normal and abnormal ends share the configured guarantee.
		ttl := r.ttl
		r.tombstones[id] = tb
		r.tombstoneExpires[id] = now.Add(ttl)
		r.pushExpiryLocked(expireTombstone, id, now.Add(ttl))
		if key, ok := r.createBySession[id]; ok {
			if e := r.createIdempotency[key]; e != nil {
				e.CreatedAt = now
				e.retainUntil = now.Add(ttl)
				r.pushExpiryLocked(expireCreate, key, now.Add(ttl))
			}
		}
		r.scheduleExpiryLocked()
	}
	r.mu.Unlock()
	r.UpdateSuspended()
	return s
}

func (r *Registry) AddSuspendedMem(n int64) { r.UpdateSuspended() }

func (r *Registry) ReleaseSuspendedMem(n int64) { r.UpdateSuspended() }

func (r *Registry) MarkSuspended(s *Session) {
	s.mu.Lock()
	lease := s.recoveryLease
	s.mu.Unlock()
	r.retention.Register(lease)
}

func (r *Registry) UnmarkSuspended(s *Session) { r.UpdateSuspended() }

func (r *Registry) SetRetention(retention *resource.Retention) { r.retention = retention }

func (r *Registry) CloseAll() {
	r.mu.Lock()
	sessions := make([]*Session, 0, len(r.sessions))
	for _, s := range r.sessions {
		sessions = append(sessions, s)
	}
	r.draining = true
	if r.metadata != nil {
		for _, entry := range r.createIdempotency {
			r.metadata.Put(entry.metadataCost)
		}
	}
	if r.cleanupTimer != nil {
		r.cleanupTimer.Stop()
		r.cleanupTimer = nil
	}
	r.tombstoneExpires = map[string]time.Time{}
	r.createBySession = map[string]string{}
	r.tombstones = map[string]*tombstone{}
	r.expiry = nil
	r.sessions = make(map[string]*Session)
	r.createIdempotency = make(map[string]*CreateIdempEntry)
	r.pendingCreates = 0
	atomic.StoreInt64(&observability.DefaultMetrics.SuspendedMemBytes, 0)
	r.mu.Unlock()

	for _, s := range sessions {
		s.Close()
	}
}

func (r *Registry) SetResources(payload, metadata *resource.Budget, ttl time.Duration) {
	r.payload = payload
	r.metadata = metadata
	if ttl > 0 {
		r.ttl = ttl
	}
}

func (r *Registry) GetAny(id string) *Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s := r.sessions[id]; s != nil {
		return s
	}
	if deadline, ok := r.tombstoneExpires[id]; ok && !time.Now().Before(deadline) {
		r.expireLocked(time.Now())
		r.scheduleExpiryLocked()
	}
	if tb := r.tombstones[id]; tb != nil {
		return tb.view()
	}
	return nil
}

func (r *Registry) SetDraining() { r.mu.Lock(); r.draining = true; r.mu.Unlock() }

func (r *Registry) Accepting() bool { r.mu.Lock(); defer r.mu.Unlock(); return !r.draining }

func (r *Registry) UpdateSuspended() {
	// one pass (and none at all without recovery leases).
	atomic.StoreInt64(&observability.DefaultMetrics.SuspendedMemBytes, r.retention.EnforceUsed())
}
