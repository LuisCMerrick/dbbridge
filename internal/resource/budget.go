package resource

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

var ErrLimit = errors.New("resource budget exhausted")

// unbalancedReleases counts Put calls, across all budgets, that released more
// than was held. Each one is an accounting bug; the budget is clamped at zero.
var unbalancedReleases atomic.Int64

// RecordUnbalancedRelease logs and counts an over-release of held bytes by
// released bytes. Accounting paths that do not use a Budget call it directly so
// every path reports the bug the same way.
func RecordUnbalancedRelease(held, released int64) {
	unbalancedReleases.Add(1)
	slog.Error("unbalanced resource release", "held", held, "released", released)
}

// UnbalancedReleases returns the process-wide count of over-releases.
func UnbalancedReleases() int64 { return unbalancedReleases.Load() }

type Budget struct {
	limit atomic.Int64
	used  atomic.Int64
	// parent, when set, is a larger budget this one is carved from: every
	// unit held here is also held in parent, and parentKeep units of parent
	// stay free for holders that use parent directly.
	parent     *Budget
	parentKeep func() int64
	mu         sync.Mutex
	changed    chan struct{}
	waiters    int // Take calls parked on changed; guarded by mu
	waits      atomic.Int64
	waitNanos  atomic.Int64
	overflows  atomic.Int64
}

func New(limit int64) *Budget {
	b := &Budget{changed: make(chan struct{})}
	b.limit.Store(limit)
	return b
}

// NewShare returns a budget of at most limit units carved from parent: a
// unit is granted only when both this budget and parent have it free, and
// parent keeps keep() units free for its direct users (keep may be nil).
// Releasing returns the units to both. Several shares of one parent
// partition it between holders (peers, record kinds) without a second
// accounting path.
func NewShare(parent *Budget, limit int64, keep func() int64) *Budget {
	b := New(limit)
	b.parent, b.parentKeep = parent, keep
	return b
}

// SetLimit changes the limit. Units already held above a lowered limit stay
// held; new requests are refused until enough is released.
func (b *Budget) SetLimit(limit int64) {
	b.limit.Store(max(limit, 0))
	b.wake()
}

func (b *Budget) keep() int64 {
	if b.parentKeep == nil {
		return 0
	}
	return max(b.parentKeep(), 0)
}

func (b *Budget) tryLocal(n int64) bool {
	for {
		u := b.used.Load()
		if n > b.limit.Load()-u {
			return false
		}
		if b.used.CompareAndSwap(u, u+n) {
			return true
		}
	}
}

func (b *Budget) Try(n int64) bool {
	if n < 0 {
		return false
	}
	if !b.tryLocal(n) {
		return false
	}
	if b.parent != nil && !b.parent.TryKeep(n, b.keep()) {
		b.release(n)
		return false
	}
	return true
}

// fits reports whether n could ever be granted (with everything else free).
func (b *Budget) fits(n int64) bool {
	if n < 0 || n > b.limit.Load() {
		return false
	}
	return b.parent == nil || n <= b.parent.Limit()-b.keep()
}
func (b *Budget) Take(ctx context.Context, n int64) error {
	var started time.Time
	defer func() {
		if !started.IsZero() {
			b.waitNanos.Add(time.Since(started).Nanoseconds())
		}
	}()
	if !b.fits(n) {
		return ErrLimit
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Waiters register on this budget and on its parent under their
		// locks before the Try: a Put that frees units after this point, on
		// either budget, sees the waiter and wakes it.
		ch, pch := b.register()
		if b.Try(n) {
			b.unregister()
			return nil
		}
		if started.IsZero() {
			started = time.Now()
			b.waits.Add(1)
		}
		var err error
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-ch:
		case <-pch:
		}
		b.unregister()
		if err != nil {
			return err
		}
	}
}

func (b *Budget) register() (ch, pch <-chan struct{}) {
	b.mu.Lock()
	ch = b.changed
	b.waiters++
	b.mu.Unlock()
	if p := b.parent; p != nil {
		p.mu.Lock()
		pch = p.changed
		p.waiters++
		p.mu.Unlock()
	}
	return ch, pch
}

func (b *Budget) unregister() {
	b.mu.Lock()
	b.waiters--
	b.mu.Unlock()
	if p := b.parent; p != nil {
		p.mu.Lock()
		p.waiters--
		p.mu.Unlock()
	}
}
func (b *Budget) Put(n int64) {
	if n <= 0 {
		return
	}
	b.release(n)
	if b.parent != nil {
		b.parent.Put(n)
	}
}

// release returns n units to this budget only and wakes its waiters.
func (b *Budget) release(n int64) {
	for {
		u := b.used.Load()
		next := u - n
		if next < 0 {
			// An over-release is a bug in the caller, but crashing the process
			// would take down every session on the node. Clamp and record it.
			next = 0
		}
		if b.used.CompareAndSwap(u, next) {
			if u < n {
				b.overflows.Add(1)
				RecordUnbalancedRelease(u, n)
			}
			break
		}
	}
	b.wake()
}

// wake releases parked Take calls. Only a budget with waiters replaces its
// channel, so the common uncontended Put neither allocates nor broadcasts.
func (b *Budget) wake() {
	b.mu.Lock()
	if b.waiters > 0 {
		close(b.changed)
		b.changed = make(chan struct{})
	}
	b.mu.Unlock()
}

// TryKeep acquires n only if at least keep units stay free afterwards. A role
// that must not starve another role sharing the same budget (for example remote
// peers' egress sessions versus this node's local ingress listeners)
// admits with TryKeep while the protected role keeps using Try.
func (b *Budget) TryKeep(n, keep int64) bool {
	if n < 0 || keep < 0 {
		return false
	}
	for {
		u := b.used.Load()
		if n > b.limit.Load()-u-keep {
			return false
		}
		if b.used.CompareAndSwap(u, u+n) {
			break
		}
	}
	if b.parent != nil && !b.parent.TryKeep(n, b.keep()) {
		b.release(n)
		return false
	}
	return true
}

// Available returns the currently unused part of the budget.
func (b *Budget) Available() int64 { return max(b.limit.Load()-b.used.Load(), 0) }
func (b *Budget) Used() int64      { return b.used.Load() }

func (b *Budget) Limit() int64 { return b.limit.Load() }

// Overreleases returns how many Put calls on this budget released more than was held.
func (b *Budget) Overreleases() int64 { return b.overflows.Load() }

func (b *Budget) WaitStats() (int64, time.Duration) {
	return b.waits.Load(), time.Duration(b.waitNanos.Load())
}

type Manager struct {
	Payload, Codec, Metadata, ControlMetadata, ProbeMetadata, Sessions, Probes, Recovery, Cleanup, Creates *Budget
	Suspended                                                                                              *Retention
	InboundConnections, DataConnections, ControlConnections, DataRequests, ControlRequests                 *Budget
	// ConnMetadata is a pool dedicated to per-socket admission metadata
	// (ConnMetadataPerConn per accepted HTTP connection, sized for every
	// InboundConnections slot). Passing it to LimitListener instead of
	// Metadata keeps unauthenticated sockets from exhausting the session
	// metadata pool.
	ConnMetadata *Budget
	// ingressSessionFloor/ingressMetadataFloor are the Sessions and Metadata
	// units reserved for this node's local ingress role. Zero keeps
	// the historical fully shared behaviour.
	ingressSessionFloor  atomic.Int64
	ingressMetadataFloor atomic.Int64
	// ingressCodecFloor is the Codec reserved for the local ingress role;
	// remote-peer codec charges go through shares of Codec that keep it free.
	ingressCodecFloor atomic.Int64
	idleMu            sync.Mutex
	idleEvictors      []func(bool)
}

// ConnMetadataPerConn is the metadata charged for one admitted socket.
const ConnMetadataPerConn = 16 << 10

// SetIngressReserve reserves part of Sessions and Metadata for the local
// ingress role. Egress admission then uses TryEgressSession/TryEgressMetadata,
// which leave the reserve free, while local ingress keeps using Sessions.Try and
// Metadata.Try. Values are clamped to the budget limits.
func (m *Manager) SetIngressReserve(sessions, metadata int64) {
	m.ingressSessionFloor.Store(min(max(sessions, 0), m.Sessions.Limit()))
	m.ingressMetadataFloor.Store(min(max(metadata, 0), m.Metadata.Limit()))
}

// IngressReserve returns the reserve set by SetIngressReserve.
func (m *Manager) IngressReserve() (sessions, metadata int64) {
	return m.ingressSessionFloor.Load(), m.ingressMetadataFloor.Load()
}

// TryEgressSession admits one remote-peer session without consuming the local
// ingress reserve.
func (m *Manager) TryEgressSession() bool { return m.Sessions.TryKeep(1, m.ingressSessionFloor.Load()) }

// TryEgressMetadata charges n bytes of remote-peer metadata without consuming
// the local ingress reserve.
func (m *Manager) TryEgressMetadata(n int64) bool {
	return m.Metadata.TryKeep(n, m.ingressMetadataFloor.Load())
}

// SetIngressCodecReserve reserves n bytes of Codec for the local ingress
// role (clamped to the Codec limit). Egress codec charges use shares created
// by EgressShare, which leave it free; the local ingress keeps using Codec.
func (m *Manager) SetIngressCodecReserve(n int64) {
	m.ingressCodecFloor.Store(min(max(n, 0), m.Codec.Limit()))
	m.Codec.wake()
}

// IngressCodecReserve returns the reserve set by SetIngressCodecReserve.
func (m *Manager) IngressCodecReserve() int64 { return m.ingressCodecFloor.Load() }

// EgressMetadataLimit is the part of Metadata remote peers may hold: the
// whole budget minus the local ingress reserve.
func (m *Manager) EgressMetadataLimit() int64 {
	return max(m.Metadata.Limit()-m.ingressMetadataFloor.Load(), 0)
}

// EgressCodecLimit is the part of Codec remote peers may hold.
func (m *Manager) EgressCodecLimit() int64 {
	return max(m.Codec.Limit()-m.ingressCodecFloor.Load(), 0)
}

// EgressMetadataShare returns a share of at most limit bytes of Metadata
// that never takes the local ingress reserve.
func (m *Manager) EgressMetadataShare(limit int64) *Budget {
	return NewShare(m.Metadata, limit, m.ingressMetadataFloor.Load)
}

// EgressCodecShare returns a share of at most limit bytes of Codec that never
// takes the local ingress reserve.
func (m *Manager) EgressCodecShare(limit int64) *Budget {
	return NewShare(m.Codec, limit, m.ingressCodecFloor.Load)
}

func (m *Manager) RegisterIdleEvictor(f func(bool)) {
	m.idleMu.Lock()
	defer m.idleMu.Unlock()
	m.idleEvictors = append(m.idleEvictors, f)
}

func (m *Manager) EvictIdle(control bool) {
	m.idleMu.Lock()
	callbacks := append([]func(bool){}, m.idleEvictors...)
	m.idleMu.Unlock()
	for _, f := range callbacks {
		f(control)
	}
}

func NewManager(payload, codec, metadata int64, sessions int, suspendedLimit ...int64) *Manager {
	limit := min(payload/4, int64(64<<20))
	if len(suspendedLimit) > 0 {
		limit = suspendedLimit[0]
	}
	reserve := metadata / 4
	if reserve > 1<<20 {
		reserve = 1 << 20
	}
	physical := int64(min(max(sessions, 1), 8192))
	return &Manager{Suspended: NewRetention(limit), ProbeMetadata: New(reserve), ControlMetadata: New(reserve), Creates: New(16), Payload: New(payload), Codec: New(codec), Metadata: New(metadata - 2*reserve), Sessions: New(int64(sessions)), Probes: New(16), Recovery: New(16), Cleanup: New(16),
		InboundConnections: New(4*physical + 64), ConnMetadata: New((4*physical + 64) * ConnMetadataPerConn), DataConnections: New(2*physical + 32), ControlConnections: New(min(physical+32, 256)), DataRequests: New(2*physical + 32), ControlRequests: New(min(physical+32, 256)),
	}
}
