package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/egress"
	"dbbridge/internal/ingress"
	"dbbridge/internal/observability"
	"dbbridge/internal/session"
)

// Node represents a running dbbridge peer instance.
type Node struct {
	cfg         *config.Config
	adminServer *observability.AdminServer
	auth        *auth.Authenticator
	registry    *session.Registry
	egressSrv   *egress.Server
	ingressMgr  *ingress.IngressManager
	mu          sync.Mutex
	running     bool
	// spent is set once Stop or a failed Start has closed the authenticator,
	// registry and egress server. Those cannot be reopened, so a spent Node
	// refuses to start again instead of half-starting.
	spent bool
	// fatal carries the first background failure that leaves the node unable
	// to serve; the process should stop and exit non-zero.
	fatal chan error
}

// Fatal returns a channel that receives the first fatal background error,
// for example a failed egress HTTP server or an ingress accept loop that gave
// up. The caller should Stop the node and exit with an error so a service
// manager restarts it.
func (n *Node) Fatal() <-chan error { return n.fatal }

func (n *Node) reportFatal(err error) {
	if n.adminServer != nil {
		n.adminServer.SetReady(false)
	}
	select {
	case n.fatal <- err:
	default:
	}
}

// ErrNodeSpent is returned by Start after Stop or after a failed Start.
var ErrNodeSpent = errors.New("node was stopped or failed to start; create a new Node to restart")

func NewNode(cfg *config.Config) *Node {
	n := &Node{
		cfg:   cfg,
		fatal: make(chan error, 1),
	}

	if cfg.Admin.Enabled {
		n.adminServer = observability.NewAdminServer(cfg.Admin.Listen)
	}

	n.auth = auth.NewAuthenticator(cfg)
	n.registry = session.NewRegistry(cfg.Transport.SuspendedBufferLimit, cfg.Transport.GlobalBufferLimit)
	n.registry.SetMaxSessions(cfg.Limits.MaxSessions)

	if cfg.HTTP.Enabled {
		n.egressSrv = egress.NewServer(cfg, n.auth, n.registry)
	}

	if len(cfg.Listeners) > 0 {
		n.ingressMgr = ingress.NewIngressManager(cfg, n.auth)
	}

	if n.adminServer != nil {
		n.adminServer.SetCollector(n.writeMetrics)
	}
	return n
}

// ReloadCredentials re-reads the node's credentials now, as on SIGHUP or a
// Windows service parameter-change request. A rejected candidate keeps the
// previous credentials and is returned.
func (n *Node) ReloadCredentials() error { return n.auth.Reload() }

func (n *Node) Start(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.running {
		return nil
	}
	if n.spent {
		return ErrNodeSpent
	}
	if err := ctx.Err(); err != nil {
		// Cancelled before anything started: release what NewNode created.
		n.releaseUnstarted()
		return err
	}

	if err := n.auth.Err(); err != nil {
		// Release the credential reload goroutine started by NewNode.
		n.releaseUnstarted()
		return fmt.Errorf("credentials not ready: %w", err)
	}
	slog.Info("starting dbbridge peer node", "node_id", n.cfg.Node.ID)

	var rollbackCleanups []func()
	rollback := func() {
		n.spent = true
		for i := len(rollbackCleanups) - 1; i >= 0; i-- {
			rollbackCleanups[i]()
		}
		if n.auth != nil {
			n.auth.Close()
		}
		if n.registry != nil {
			n.registry.CloseAll()
		}
	}

	// 1. Start Admin Server
	if n.adminServer != nil {
		if err := n.adminServer.Start(); err != nil {
			rollback()
			return fmt.Errorf("failed to start admin server: %w", err)
		}
		rollbackCleanups = append(rollbackCleanups, func() { _ = n.adminServer.Close() })
		slog.Info("admin server started", "listen", n.cfg.Admin.Listen)
	}

	// 2. Start HTTP Egress Server
	if n.egressSrv != nil {
		n.egressSrv.SetServeErrorHandler(func(err error) {
			n.reportFatal(fmt.Errorf("egress HTTP server stopped: %w", err))
		})
		if err := n.egressSrv.Start(); err != nil {
			rollback()
			return fmt.Errorf("failed to start egress server: %w", err)
		}
		rollbackCleanups = append(rollbackCleanups, func() { _ = n.egressSrv.Close() })
	}

	// 3. Start TCP Ingress Manager
	if n.ingressMgr != nil {
		n.ingressMgr.SetFatalHandler(func(err error) { n.reportFatal(fmt.Errorf("ingress: %w", err)) })
		if err := n.ingressMgr.Start(); err != nil {
			// Close listeners and goroutines a partial Start left behind
			//.
			rollbackCleanups = append(rollbackCleanups, func() { _ = n.ingressMgr.Close() })
			rollback()
			return fmt.Errorf("failed to start ingress manager: %w", err)
		}
		rollbackCleanups = append(rollbackCleanups, func() { _ = n.ingressMgr.Close() })
	}

	if n.adminServer != nil {
		n.adminServer.SetReady(true)
	}

	n.running = true
	return nil
}

func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.running {
		n.releaseUnstarted()
		return
	}
	n.shutdownLocked()
}

// releaseUnstarted closes what NewNode created for a node that never started
// (or failed before starting anything), so its credential reload goroutine
// does not leak.
func (n *Node) releaseUnstarted() {
	if n.spent || n.running {
		return
	}
	n.spent = true
	if n.auth != nil {
		n.auth.Close()
	}
	if n.registry != nil {
		n.registry.CloseAll()
	}
}

func (n *Node) shutdownLocked() {
	if !n.running {
		return
	}
	slog.Info("shutting down dbbridge peer node", "node_id", n.cfg.Node.ID)
	// Everything below, drain included, finishes within
	// cfg.MaxShutdownDuration(), which the systemd unit's TimeoutStopSec
	// is derived from.
	hardDeadline := time.Now().Add(n.cfg.MaxShutdownDuration())

	if n.adminServer != nil {
		n.adminServer.SetReady(false)
	}

	if n.egressSrv != nil {
		n.egressSrv.Drain()
	}
	if n.ingressMgr != nil {
		n.ingressMgr.Drain()
	}
	n.drain(time.Now().Add(n.cfg.Transport.ShutdownTimeout))
	var steps []func()
	if n.ingressMgr != nil {
		// Remote cleanup of every ingress session runs under the hard
		// shutdown deadline, not Close's 3s default. One
		// second is left for the abort grace and the remaining steps.
		steps = append(steps, func() {
			ctx, cancel := context.WithDeadline(context.Background(), ingressCloseDeadline(hardDeadline))
			defer cancel()
			_ = n.ingressMgr.CloseContext(ctx)
		})
	}
	if n.egressSrv != nil {
		steps = append(steps, func() { _ = n.egressSrv.Close() })
	}
	if n.registry != nil {
		steps = append(steps, n.registry.CloseAll)
	}
	if n.auth != nil {
		steps = append(steps, n.auth.Close)
	}
	if n.adminServer != nil {
		steps = append(steps, func() { _ = n.adminServer.Close() })
	}
	if !runBounded(hardDeadline, steps...) {
		slog.Error("shutdown teardown exceeded its budget; abandoning remaining cleanup", "budget", n.cfg.MaxShutdownDuration())
	}

	n.running = false
	n.spent = true
	slog.Info("dbbridge peer node stopped")
}

// drainProgressInterval is how often the drain reports what it waits for.
const drainProgressInterval = 5 * time.Second

// drain waits until no local client connection and no remote peer's session
// is left, or until deadline (transport.shutdown_timeout). Database clients
// usually keep pooled connections open, so the wait often runs to the
// deadline: it reports what it waits for when it starts and every
// drainProgressInterval, so an operator can tell a drain from a hang and
// knows a second signal forces the exit.
func (n *Node) drain(deadline time.Time) {
	next := time.Now()
	for {
		ing := 0
		if n.ingressMgr != nil {
			ing = n.ingressMgr.Count()
		}
		remote := len(n.registry.GetAllSessions())
		if ing == 0 && remote == 0 {
			return
		}
		now := time.Now()
		if !now.Before(deadline) {
			slog.Warn("drain timeout reached; closing the remaining sessions", "local_connections", ing, "remote_sessions", remote, "shutdown_timeout", n.cfg.Transport.ShutdownTimeout)
			return
		}
		if !now.Before(next) {
			slog.Info("draining: waiting for open sessions to finish (send the signal again to force exit)", "local_connections", ing, "remote_sessions", remote, "remaining", deadline.Sub(now).Round(time.Second))
			next = now.Add(drainProgressInterval)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ingressCloseDeadline is the deadline given to the ingress manager's
// CloseContext: the hard deadline minus a second for its abort grace, but
// never in the past.
func ingressCloseDeadline(hard time.Time) time.Time {
	d := hard.Add(-time.Second)
	if now := time.Now(); d.Before(now) {
		return now
	}
	return d
}

// runBounded runs steps in order on another goroutine and waits until they
// finish or the deadline passes. It reports whether all steps completed; a
// step still running after the deadline is abandoned.
func runBounded(deadline time.Time, steps ...func()) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, step := range steps {
			step()
		}
	}()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}
