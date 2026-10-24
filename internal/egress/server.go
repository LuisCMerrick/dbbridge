package egress

import (
	"context"
	"crypto/tls"
	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/resource"
	"dbbridge/internal/session"
	"dbbridge/internal/transport"
	"dbbridge/internal/tunnel"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type Server struct {
	ctx          context.Context
	cancel       context.CancelFunc
	cfg          *config.Config
	auth         *auth.Authenticator
	registry     *session.Registry
	httpServer   *http.Server
	listener     net.Listener
	mu           sync.Mutex
	downNotifier sync.Map
	connections  sync.Map
	wg           sync.WaitGroup
	stopped      bool
	started      bool
	onServeError func(error)
	// stopReload unregisters the credentials reload sweep on Close.
	stopReload func()
	// idem bounds the create records each peer keeps in the shared
	// idempotency table.
	idem *idemTracker
	// records is the part of Metadata the idempotency table may hold for
	// create records and tombstones (see createRecordBudget).
	records *resource.Budget
	// peerCodec holds each peer's share of the egress Codec (peer -> *resource.Budget).
	peerCodec sync.Map
	// sessions holds egress-only per-session state (live sessions only).
	sessions sync.Map // id -> *sessState
	// preAuthTimeout bounds unauthenticated sockets; 0 = default.
	preAuthTimeout time.Duration
	// hub accepts reverse tunnels on /v1/reverse (public side of reverse
	// peers); nil when this node has none.
	hub *tunnel.Hub
	// tunnelLn delivers the streams of the tunnels this node dials out
	// (reverse.enabled); nil when it dials none.
	tunnelLn *tunnel.Listener
}

// Addr returns the address of the http.listen socket once started, or nil.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// SetReverseHub routes GET /v1/reverse to h. Call before Start.
func (s *Server) SetReverseHub(h *tunnel.Hub) {
	s.mu.Lock()
	s.hub = h
	s.mu.Unlock()
}

// SetTunnelListener makes Start also serve the streams delivered to ln, the
// tunnels this node dials out. Call before Start.
func (s *Server) SetTunnelListener(ln *tunnel.Listener) {
	s.mu.Lock()
	s.tunnelLn = ln
	s.mu.Unlock()
}

// sessState is per-session state the egress keeps beside the session.
type sessState struct {
	upLimit  int64
	upActive atomic.Int64
}

// closeGrace bounds how long Close waits for in-flight handlers, and then for
// handler and per-session goroutines, before giving up.
const closeGrace = 5 * time.Second

func NewServer(c *config.Config, a *auth.Authenticator, r *session.Registry) *Server {
	if c.Resources == nil {
		c.Resources = resource.NewManager(256<<20, 32<<20, 32<<20, 200)
	}
	// On a bidirectional node remote peers must not take the Sessions,
	// Metadata and Codec the local listeners need. Idempotent: config
	// already applies the session and metadata reserve when it built
	// Resources.
	c.ApplyIngressReserve()
	applyIngressCodecReserve(c)
	s := &Server{cfg: c, auth: a, registry: r, idem: newIdemTracker()}
	s.records = c.Resources.EgressMetadataShare(s.createRecordBudget())
	// Normal close, abnormal close and create failure all keep their create
	// record and tombstone for idempotency_tombstone_ttl; they are
	// charged to the create record share of Metadata.
	r.SetResources(c.Resources.Payload, s.records, c.Transport.IdempotencyTombstoneTTL)
	r.SetRetention(c.Resources.Suspended)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.stopReload = a.OnReload(s.onAuthReload)
	return s
}

// createRecordBudget sizes the part of Metadata that create records and
// tombstones may hold: what remains of the egress metadata once every egress
// session slot has its session metadata, and at least an eighth of the egress
// metadata. Records are retained for the tombstone TTL after a session ends,
// so without this bound create/close churn could take the metadata every
// live session needs; the bound also caps the sustained new-session rate at
// about budget / (record cost x idempotency_tombstone_ttl).
func (s *Server) createRecordBudget() int64 {
	res := s.cfg.Resources
	egress := res.EgressMetadataLimit()
	sessionFloor, _ := res.IngressReserve()
	slots := max(res.Sessions.Limit()-sessionFloor, 0)
	need := slots * s.sessionMetadataCost()
	budget := egress - need
	if need > egress {
		budget = 0
	}
	return min(max(budget, egress/8), egress)
}

// SetServeErrorHandler registers a callback for a listener failure other than
// a normal shutdown, so the owner can flip readiness.
func (s *Server) SetServeErrorHandler(fn func(error)) {
	s.mu.Lock()
	s.onServeError = fn
	s.mu.Unlock()
}

// enter tracks a handler in wg unless the server is stopped. Add happens under
// s.mu while stopped is false, so it always precedes Close's Wait.
func (s *Server) enter() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return false
	}
	s.wg.Add(1)
	return true
}

func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return errors.New("egress server is closed and cannot be restarted")
	}
	if s.started {
		return errors.New("egress server already started")
	}
	srv := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 16 << 10, ConnContext: guardConnContext}
	// Socket admission is charged to a connection-only metadata budget sized
	// for the connection limit, never to the data-plane Metadata that creates,
	// /up and streams need. Unauthenticated sockets are further
	// bounded by connGuard. Tunnel streams share both budgets.
	conns, connMeta := s.cfg.Resources.InboundConnections, s.cfg.Resources.ConnMetadata
	if connMeta == nil {
		connMeta = resource.New(conns.Limit() * resource.ConnMetadataPerConn)
	}
	s.httpServer = srv
	if s.tunnelLn != nil {
		// Streams arrive over a tunnel that is already authenticated;
		// the guard keys them by tunnel (their remote address), and one
		// tunnel may use the whole unauthenticated share.
		g := newConnGuard(conns.Limit(), s.preAuthTimeout)
		g.maxPerIP = g.maxUnauth
		var tl net.Listener = resource.LimitListener(s.tunnelLn, conns, connMeta)
		tl = &guardListener{Listener: tl, g: g}
		go func() { s.serveDone(srv.Serve(tl)) }()
	}
	// A node that only dials tunnels (http.enabled: false) opens no socket;
	// without tunnels Start always listens on http.listen, as before.
	if s.tunnelLn != nil && !s.cfg.HTTP.Enabled {
		s.started = true
		return nil
	}
	ln, e := net.Listen("tcp", s.cfg.HTTP.Listen)
	if e != nil {
		if s.tunnelLn != nil {
			_ = srv.Close()
		}
		return e
	}
	s.listener = ln
	ln = resource.LimitListener(ln, conns, connMeta)
	ln = &guardListener{Listener: ln, g: newConnGuard(conns.Limit(), s.preAuthTimeout)}
	ln = transport.IdleWriteListener(ln)
	if s.cfg.HTTP.TLS.Enabled {
		pair, e := s.cfg.LoadServerCertificate()
		if e != nil {
			ln.Close()
			return e
		}
		srv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
		go func() { s.serveDone(srv.ServeTLS(ln, "", "")) }()
	} else {
		go func() { s.serveDone(srv.Serve(ln)) }()
	}
	s.started = true
	return nil
}

// serveDone reports a listener failure. ErrServerClosed is the normal result
// of Close and is not an error.
func (s *Server) serveDone(err error) {
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return
	}
	slog.Error("egress server stopped serving", "err", err)
	s.mu.Lock()
	fn := s.onServeError
	s.mu.Unlock()
	if fn != nil {
		fn(err)
	}
}

func (s *Server) Drain() { s.registry.SetDraining() }

func (s *Server) Close() error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	s.cancel()
	if s.stopReload != nil {
		s.stopReload()
	}
	s.registry.SetDraining()
	srv := s.httpServer
	s.mu.Unlock()
	s.registry.CloseAll()
	s.connections.Range(func(k, v any) bool { _ = k.(*websocket.Conn).Close(); return true })
	var err error
	if srv != nil {
		// Graceful first: idle keep-alive connections close at once and
		// in-flight handlers (whose sessions are already closed) can finish.
		ctx, cancel := context.WithTimeout(context.Background(), closeGrace)
		if err = srv.Shutdown(ctx); err != nil {
			err = srv.Close()
		}
		cancel()
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(closeGrace):
		slog.Warn("egress server close timed out waiting for handlers and session goroutines")
	}
	return err
}
