package ingress

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/protocol"
)

// cleanupFailoverEnv builds a session bound to a primary endpoint that
// answers every request with 503 and a backup endpoint that probes as the
// owner and accepts the DELETE.
func cleanupFailoverEnv(t *testing.T) (*IngressManager, *ingressSession, *atomic.Int64, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	var primaryDeletes, probes, deletes atomic.Int64
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			primaryDeletes.Add(1)
		}
		w.WriteHeader(503)
	}))
	t.Cleanup(primary.Close)
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/probe" {
			probes.Add(1)
			auditProbeResponse(w, r, true)
			return
		}
		if r.Method == http.MethodDelete {
			deletes.Add(1)
			w.Header().Set(protocol.HeaderInstanceID, "bi")
			_ = json.NewEncoder(w).Encode(protocol.DeleteSessionResponse{Closed: true, SessionID: "s", InstanceID: "bi", Epoch: "1"})
			return
		}
		w.WriteHeader(404)
	}))
	t.Cleanup(backup.Close)
	m, p := auditChooser(t, primary.URL, backup.URL)
	local, remote := net.Pipe()
	t.Cleanup(func() { remote.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := &ingressSession{m: m, p: p, conn: local, ctx: ctx, cancel: cancel, changed: make(chan struct{}), sid: "s", token: "token", owner: "bi", epoch: 1, g: &generation{c: m.clients[p.ID].WithURL(primary.URL)}}
	return m, s, &primaryDeletes, &probes, &deletes
}

func TestCleanupFailoverOutsideShutdownReachesBackup(t *testing.T) {
	m, s, _, _, deletes := cleanupFailoverEnv(t)
	s.stop(context.Canceled)
	until := time.Now().Add(3 * time.Second)
	for deletes.Load() == 0 && time.Now().Before(until) {
		time.Sleep(5 * time.Millisecond)
	}
	<-m.cleanups.idle()
	if deletes.Load() != 1 {
		t.Fatalf("want 1 backup DELETE, got %d", deletes.Load())
	}
}

func TestShutdownCleanupReachesBackupEndpoint(t *testing.T) {
	m, s, _, probes, deletes := cleanupFailoverEnv(t)
	m.activeConns.Store("x", &activeConnEntry{cancel: s.cancel, closeFn: func() { s.stop(errNodeStopped) }, session: s})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := m.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	if deletes.Load() != 1 {
		t.Fatalf("shutdown cleanup did not reach the backup endpoint: probes=%d deletes=%d", probes.Load(), deletes.Load())
	}
}

// A session that was recovering is bound to the carrier that just failed:
// its cleanup asks the backup first and does not need the broken primary.
func TestShutdownCleanupOfRecoveringSessionTriesBackupFirst(t *testing.T) {
	m, s, primaryDeletes, _, deletes := cleanupFailoverEnv(t)
	s.deadline = time.Now().Add(time.Minute)
	m.activeConns.Store("x", &activeConnEntry{cancel: s.cancel, closeFn: func() { s.stop(errNodeStopped) }, session: s})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := m.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	if deletes.Load() != 1 || primaryDeletes.Load() != 0 {
		t.Fatalf("recovering session: backup deletes=%d primary deletes=%d, want 1 and 0", deletes.Load(), primaryDeletes.Load())
	}
}

// Stopping sessions must not be serial: each stop below blocks for 150 ms
// (as a WS close behind a blocked writer does), and twenty of them must
// still finish well inside the shutdown budget.
func TestCloseContextStopsSessionsConcurrently(t *testing.T) {
	m, _ := auditChooser(t, "http://127.0.0.1:1")
	var stopped atomic.Int64
	for i := 0; i < 20; i++ {
		m.activeConns.Store(i, &activeConnEntry{cancel: func() {}, closeFn: func() { time.Sleep(150 * time.Millisecond); stopped.Add(1) }})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	if err := m.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if stopped.Load() != 20 || elapsed > 1500*time.Millisecond {
		t.Fatalf("stopped=%d in %v; want all 20 well under the serial cost of 3s", stopped.Load(), elapsed)
	}
}

// The shutdown deadline bounds the stop phase too.
func TestCloseContextDeadlineBoundsBlockedStops(t *testing.T) {
	m, _ := auditChooser(t, "http://127.0.0.1:1")
	release := make(chan struct{})
	defer close(release)
	m.activeConns.Store("blocked", &activeConnEntry{cancel: func() {}, closeFn: func() { <-release }})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_ = m.CloseContext(ctx)
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond+closeAbortGrace+500*time.Millisecond {
		t.Fatalf("CloseContext took %v with a blocked session stop", elapsed)
	}
}
