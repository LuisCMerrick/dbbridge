package ingress

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/link"
	"dbbridge/internal/resource"
	"dbbridge/internal/transport"
)

type activeConnEntry struct {
	cancel  context.CancelFunc
	closeFn func()
	session *ingressSession
}
type IngressManager struct {
	ctx              context.Context
	cancel           context.CancelFunc
	cfg              *config.Config
	auth             *auth.Authenticator
	clients          map[string]*transport.TripleTransportClient
	linkManagers     map[string]*link.PeerLinkManager
	listeners        []net.Listener
	activeConns      sync.Map
	mu               sync.Mutex
	stopCh           chan struct{}
	wg               sync.WaitGroup
	cleanups         cleanupTracker // remote DELETEs; may start while CloseContext waits
	closed, draining bool
	started          bool
	fatal            func(error)
	shutdownCtx      context.Context
	cleanupCtx       context.Context
	cleanupCancel    context.CancelFunc
	probeFlights     map[string]*probeCall
	probeCache       map[string]probeCacheEntry
	precheckUsed     map[string]time.Time
	probeSlots       map[string]chan struct{}
	preMu            sync.Mutex
	prechecks        map[string]*precheckCall
	cache            map[string]wsCacheEntry
	diagMu           sync.Mutex
	phases           map[phaseKey]phaseValue
	graceWarned      sync.Map // peer ID -> warned about grace <= cooldown
}

func NewIngressManager(c *config.Config, a *auth.Authenticator) *IngressManager {
	if c.Resources == nil {
		c.Resources = resource.NewManager(256<<20, 32<<20, 32<<20, 200)
	}
	m := &IngressManager{cfg: c, auth: a, clients: map[string]*transport.TripleTransportClient{}, linkManagers: map[string]*link.PeerLinkManager{}, stopCh: make(chan struct{}), probeSlots: map[string]chan struct{}{}, prechecks: map[string]*precheckCall{}, cache: map[string]wsCacheEntry{}}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.cleanupCtx, m.cleanupCancel = context.WithCancel(context.Background())
	m.probeFlights = map[string]*probeCall{}
	m.probeCache = map[string]probeCacheEntry{}
	m.precheckUsed = map[string]time.Time{}
	for _, p := range c.Peers {
		id := p.ID
		cl := transport.NewTripleTransportClient(&p, &c.Transport, c.Node.InstanceID)
		cl.SetResources(c.Resources)
		if a != nil {
			cl.SetTokenProvider(func() string { return a.GetOutboundToken(id) })
		}
		m.clients[id] = cl
		m.linkManagers[id] = link.NewPeerLinkManager(p)
		n := p.Failover.MaxConcurrentProbes
		if n <= 0 {
			n = 2
		}
		m.probeSlots[id] = make(chan struct{}, n)
	}
	return m
}

// SetReverseOpener makes reverse peer id open its connections as streams
// from open (the tunnels that peer dialed in). Call before Start.
func (m *IngressManager) SetReverseOpener(id string, open transport.StreamOpener) {
	m.mu.Lock()
	cl := m.clients[id]
	m.mu.Unlock()
	if cl != nil {
		cl.SetPeerDialer(transport.TunnelDialer{Open: open, Timeout: m.cfg.Transport.PeerDial()})
	}
}

// Start opens every configured listener and starts the health loops. It may
// succeed only once: a second call (or a call after Close/Drain) returns an
// error and never touches listeners that are already running. When one
// listener fails to open, only the listeners opened by this call are closed.
func (m *IngressManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.draining {
		return errors.New("ingress manager is closed")
	}
	if m.started {
		return errors.New("ingress manager already started")
	}
	opened := make([]net.Listener, 0, len(m.cfg.Listeners))
	for _, l := range m.cfg.Listeners {
		ln, e := net.Listen("tcp", l.Listen)
		if e != nil {
			for _, x := range opened {
				x.Close()
			}
			m.listeners = m.listeners[:len(m.listeners)-len(opened)]
			return e
		}
		warnNonLoopback(l, ln.Addr())
		opened = append(opened, ln)
		m.listeners = append(m.listeners, ln)
		m.wg.Add(1)
		go m.acceptLoop(ln, l)
	}
	m.started = true
	for _, p := range m.cfg.Peers {
		m.wg.Add(1)
		go m.healthLoop(p)
	}
	return nil
}

// warnNonLoopback logs when a local listener is reachable from other hosts:
// such a listener is an open path to the remote database.
func warnNonLoopback(l config.ListenerConfig, addr net.Addr) {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok || tcp.IP.IsLoopback() {
		return
	}
	slog.Warn("ingress listener is bound to a non-loopback address; any host that can reach it can use this peer's credentials", "listener", l.ID, "listen", l.Listen)
}
func (m *IngressManager) Drain() {
	m.mu.Lock()
	if !m.draining {
		m.draining = true
		for _, ln := range m.listeners {
			ln.Close()
		}
	}
	m.mu.Unlock()
}
func (m *IngressManager) Count() int {
	n := 0
	m.activeConns.Range(func(k, v any) bool { n++; return true })
	return n
}

// Close stops every session and waits for remote cleanup within the default
// teardown budget (see CloseContext).
func (m *IngressManager) Close() error {
	budget := m.cfg.Transport.ControlRequestTimeout
	if budget <= 0 || budget > defaultTeardownBudget {
		budget = defaultTeardownBudget
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	return m.CloseContext(ctx)
}

// defaultTeardownBudget bounds how long Close waits for remote session
// cleanup (DELETE) after all local sessions have been stopped.
const defaultTeardownBudget = 3 * time.Second

// closeAbortGrace is how long CloseContext still waits for goroutines after
// cancelling them at the deadline.
const closeAbortGrace = time.Second

// CloseContext stops every session. Remote cleanup DELETEs started by the
// shutdown wait for the cleanup budget instead of being skipped, and all of
// them are bounded by ctx: when ctx ends, outstanding cleanup is cancelled so
// shutdown never exceeds the caller's deadline by more than a short grace.
func (m *IngressManager) CloseContext(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.shutdownCtx = ctx
	m.cancel()
	close(m.stopCh)
	for _, ln := range m.listeners {
		ln.Close()
	}
	m.mu.Unlock()
	// Sessions stop concurrently: closing one carrier can block for a while
	// (a WS close frame waits behind a blocked writer), and the shutdown
	// deadline below must bound the whole stop phase, not each session.
	var stops sync.WaitGroup
	m.activeConns.Range(func(k, v any) bool {
		a := v.(*activeConnEntry)
		stops.Add(1)
		go func() { defer stops.Done(); a.closeFn(); a.cancel() }()
		return true
	})
	done := make(chan struct{})
	go func() { stops.Wait(); m.wg.Wait(); <-m.cleanups.idle(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		// Out of time: abort the remaining cleanup requests.
		m.cleanupCancel()
		select {
		case <-done:
		case <-time.After(closeAbortGrace):
			slog.Warn("ingress shutdown: background work still running after the deadline")
		}
	}
	m.cleanupCancel()
	for _, c := range m.clients {
		c.Close()
	}
	return nil
}

// shutdownContext returns the context of an in-progress CloseContext, or nil.
func (m *IngressManager) shutdownContext() context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.shutdownCtx
}

// SetFatalHandler registers a callback for unrecoverable background failures
// (a listener whose Accept keeps failing with a non-transient error). The
// owner should treat it as a reason to exit so a supervisor restarts it.
func (m *IngressManager) SetFatalHandler(fn func(error)) {
	m.mu.Lock()
	m.fatal = fn
	m.mu.Unlock()
}

func (m *IngressManager) reportFatal(err error) bool {
	m.mu.Lock()
	fn := m.fatal
	m.mu.Unlock()
	if fn == nil {
		return false
	}
	fn(err)
	return true
}

func (m *IngressManager) stopping() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed || m.draining
}

func generateID(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// SnapshotMetrics returns bounded aggregate gauges; never includes session IDs.
func (m *IngressManager) SnapshotMetrics() map[string]int64 {
	result := map[string]int64{}
	m.activeConns.Range(func(k, v any) bool { result["connections"]++; return true })
	result["payload_bytes"] = m.cfg.Resources.Payload.Used()
	result["codec_bytes"] = m.cfg.Resources.Codec.Used()
	result["accept_rejected_total"] = acceptRejected.Load()
	return result
}
