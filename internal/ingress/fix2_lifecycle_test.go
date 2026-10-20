package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/link"
	"dbbridge/internal/protocol"
)

type fix2FlakyListener struct {
	net.Listener
	calls atomic.Int64
	errs  []error
}

func (l *fix2FlakyListener) Accept() (net.Conn, error) {
	n := int(l.calls.Add(1))
	if n <= len(l.errs) {
		return nil, l.errs[n-1]
	}
	return l.Listener.Accept()
}

// ING-2: transient Accept errors (descriptor exhaustion, aborted handshakes)
// are retried with backoff; only a closed listener ends the loop.
func TestFix2AcceptLoopSurvivesTransientErrors(t *testing.T) {
	e := fixSetup(t, "http-sse", false, nil)
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	emfile := &net.OpError{Op: "accept", Net: "tcp", Err: syscall.EMFILE}
	fl := &fix2FlakyListener{Listener: raw, errs: []error{emfile, emfile, &net.OpError{Op: "accept", Net: "tcp", Err: syscall.ECONNABORTED}}}
	done := make(chan struct{})
	e.m.wg.Add(1)
	go func() { e.m.acceptLoop(fl, e.m.cfg.Listeners[0]); close(done) }()
	c, err := net.Dial("tcp", raw.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	deadline := time.Now().Add(2 * time.Second)
	for e.m.Count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if e.m.Count() == 0 {
		t.Fatalf("connection not accepted after transient errors (Accept calls=%d)", fl.calls.Load())
	}
	raw.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("acceptLoop did not return after its listener was closed")
	}
}

// ING-2 / X-LC-1: a listener that keeps failing permanently is reported to
// the fatal handler instead of dying silently.
func TestFix2AcceptLoopReportsPermanentFailure(t *testing.T) {
	e := fixSetup(t, "http-sse", false, nil)
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	errs := make([]error, acceptFatalAfter)
	for i := range errs {
		errs[i] = errors.New("accept: permanent failure")
	}
	fl := &fix2FlakyListener{Listener: raw, errs: errs}
	old := acceptBackoffMax
	acceptBackoffMax = 10 * time.Millisecond
	t.Cleanup(func() { acceptBackoffMax = old })
	fatal := make(chan error, 1)
	e.m.SetFatalHandler(func(err error) { fatal <- err })
	e.m.wg.Add(1)
	go e.m.acceptLoop(fl, e.m.cfg.Listeners[0])
	select {
	case err := <-fatal:
		if !strings.Contains(err.Error(), "accept keeps failing") {
			t.Fatalf("fatal error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("permanent accept failure was never reported")
	}
}

// ING-13: a second Start fails without touching the running listeners.
func TestFix2StartTwiceKeepsListeners(t *testing.T) {
	e := fixSetup(t, "http-sse", false, nil)
	if err := e.m.Start(); err != nil {
		t.Fatal(err)
	}
	addr := e.m.listeners[0].Addr().String()
	if err := e.m.Start(); err == nil {
		t.Fatal("second Start succeeded")
	}
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("second Start closed the running listener: %v", err)
	}
	c.Close()
}

// X-SEC-7: binding a local listener to a non-loopback address is logged.
func TestFix2NonLoopbackListenerWarns(t *testing.T) {
	logs := captureLog(t)
	l := config.ListenerConfig{ID: "open", Listen: "0.0.0.0:3306"}
	warnNonLoopback(l, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1})
	if logs.String() != "" {
		t.Fatalf("loopback listener warned: %s", logs.String())
	}
	warnNonLoopback(l, &net.TCPAddr{IP: net.IPv4zero, Port: 3306})
	if !strings.Contains(logs.String(), "non-loopback") {
		t.Fatalf("missing warning for a non-loopback listener: %q", logs.String())
	}
}

// ING-6: a normal close needs no remote cleanup; a terminated session is
// deleted on its bound endpoint without an extra probe.
//
// Runs over WS now that X-DP-1 (session Ack order) is merged; a normal close
// finished through a 410 "normal" resume answer sends no DELETE either.
func TestFix2CleanupWithoutProbe(t *testing.T) {
	e, deletes := fix2Setup(t, "ws", true, nil, nil, nil)
	e.conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := fixEcho2(t, e.conn, "x"); err != nil {
		t.Fatal(err)
	}
	e.conn.(*net.TCPConn).CloseWrite()
	_, _ = e.conn.Read(make([]byte, 1))
	for i := 0; i < 200 && e.m.Count() > 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if n := deletes.Load(); n != 0 {
		t.Fatalf("a normal close sent %d DELETE(s)", n)
	}
	c, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	if err := fixEcho2(t, c, "y"); err != nil {
		t.Fatal(err)
	}
	probes := e.probes.Load()
	e.m.Close()
	if n := deletes.Load(); n != 1 {
		t.Fatalf("terminated session sent %d DELETE(s), want 1", n)
	}
	if n := e.probes.Load() - probes; n != 0 {
		t.Fatalf("cleanup probed the bound endpoint %d time(s)", n)
	}
	if n := len(e.reg.GetAllSessions()); n != 0 {
		t.Fatalf("remote sessions left after cleanup: %d", n)
	}
}

// ING-7: with failover disabled and no generation yet, the DELETE still goes
// to the (only) endpoint.
func TestFix2CleanupWithoutGeneration(t *testing.T) {
	var deletes atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deletes.Add(1)
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()
	m, p := auditChooser(t, srv.URL)
	p.Failover.Enabled = config.BoolPtr(false)
	local, remote := net.Pipe()
	defer remote.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &ingressSession{m: m, p: p, conn: local, ctx: ctx, cancel: cancel, changed: make(chan struct{}), sid: "s", token: "token", owner: "bi", epoch: 1}
	s.stop(errors.New("node stopped"))
	m.wg.Wait()
	<-m.cleanups.idle()
	if deletes.Load() != 1 {
		t.Fatalf("DELETE attempts=%d, want 1", deletes.Load())
	}
}

// 10.3.3 / FD-14 / TF-38 (DBB-AUD-08): when the last resume's outcome is
// unknown, the cleanup DELETE uses only the last fully verified epoch; a
// trusted STALE_EPOCH ends the task as unknown without guessing epoch+1.
func TestFix2CleanupDoesNotGuessEpochAfterUnknownResume(t *testing.T) {
	var mu sync.Mutex
	var epochs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.WriteHeader(404)
			return
		}
		ep := r.URL.Query().Get("epoch")
		mu.Lock()
		epochs = append(epochs, ep)
		mu.Unlock()
		w.Header().Set(protocol.HeaderInstanceID, "bi")
		if ep == "1" {
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: protocol.ErrStaleEpoch})
			return
		}
		json.NewEncoder(w).Encode(protocol.DeleteSessionResponse{Closed: true, SessionID: "s", InstanceID: "bi", Epoch: ep})
	}))
	defer srv.Close()
	m, p := auditChooser(t, srv.URL)
	s := &ingressSession{m: m, p: p, sid: "s", token: "token", owner: "bi", epoch: 1, outcomeUnknown: true, changed: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r := s.remoteCleanup(ctx)
	mu.Lock()
	defer mu.Unlock()
	if r != cleanupUnknown || strings.Join(epochs, ",") != "1" {
		t.Fatalf("cleanup result=%v epochs=%v", r, epochs)
	}
}

// X-LC-2: shutdown with more sessions than the cleanup budget (16) deletes
// every remote session instead of skipping the rest.
func TestFix2ShutdownCleansUpEverySession(t *testing.T) {
	const n = 24
	more := func(b *config.Config) { b.Auth.InboundPeers[0].MaxSessions = 64 }
	e, deletes := fix2Setup(t, "http-sse", false, nil, more, nil)
	if err := e.m.Start(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		c, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(10 * time.Second))
		if err := fixEcho2(t, c, "z"); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(e.reg.GetAllSessions()); got != n {
		t.Fatalf("fixture: %d remote sessions", got)
	}
	e.m.Close()
	if left := len(e.reg.GetAllSessions()); left != 0 || deletes.Load() != n {
		t.Fatalf("after shutdown: %d remote sessions left, %d DELETEs", left, deletes.Load())
	}
}

// ING-10: remote cleanup during shutdown is bounded by the caller's deadline
// even when the peer never answers the DELETE.
func TestFix2CloseBoundedWhenPeerHangs(t *testing.T) {
	var hang atomic.Bool
	hook := func(w http.ResponseWriter, r *http.Request, _ http.Handler) bool {
		if r.Method == http.MethodDelete && hang.Load() {
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
			return true
		}
		return false
	}
	e, _ := fix2Setup(t, "http-sse", true, nil, nil, hook)
	e.conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := fixEcho2(t, e.conn, "x"); err != nil {
		t.Fatal(err)
	}
	hang.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	e.m.CloseContext(ctx)
	if d := time.Since(start); d > 300*time.Millisecond+closeAbortGrace {
		t.Fatalf("CloseContext took %v with a hanging peer (deadline 300ms)", d)
	}
}

// ING-8: a failed attach does not leave the destroyed generation current.
func TestFix2FailedAttachRestoresGeneration(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer srv.Close()
	m, p := auditChooser(t, srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prev := &generation{c: m.clients[p.ID].WithURL(srv.URL)}
	s := &ingressSession{m: m, p: p, ctx: ctx, cancel: cancel, changed: make(chan struct{}), sid: "s", token: "t", owner: "bi", epoch: 1, g: prev, limits: protocol.SessionLimits{MaxFrameSize: 1 << 16}}
	ep := m.linkManagers[p.ID].GetEndpoints()[0]
	actx, acancel := context.WithTimeout(ctx, time.Second)
	defer acancel()
	if _, err := s.attach(actx, m.clients[p.ID].WithURL(srv.URL), ep, 2, "http-sse", "r"); err == nil {
		t.Fatal("attach unexpectedly succeeded")
	}
	if s.g != prev || len(s.all) != 0 {
		t.Fatalf("failed attach left s.g=%p (prev %p), all=%d", s.g, prev, len(s.all))
	}
}

// ING-11: every family has HELP/TYPE and the endpoint state is a 0/1 series
// for each state.
func TestFix2MetricsFormat(t *testing.T) {
	m, p := auditChooser(t, "http://127.0.0.1:1")
	m.observePhase(p.ID, "http://127.0.0.1:1", "probe", time.Now(), true)
	var b bytes.Buffer
	m.WriteMetrics(&b)
	out := b.String()
	for _, want := range []string{"# TYPE dbb_endpoint_state gauge", "# TYPE dbb_phase_duration_seconds summary", "# TYPE dbb_ingress_bound_sessions gauge", `state="UNKNOWN"} 1`, `state="HEALTHY"} 0`, `state="MISMATCHED"} 0`, `phase="probe",success="true"} 1`} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q:\n%s", want, out)
		}
	}
	if strings.Count(out, "# TYPE dbb_phase_duration_seconds") != 1 {
		t.Fatalf("phase family declared more than once:\n%s", out)
	}
}

// ING-12: an idle session does not count unread buffer space as retained
// recovery data.
func TestFix2IdleSessionRetainsNothing(t *testing.T) {
	e := fixSetup(t, "ws", true, nil)
	e.conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := fixEcho2(t, e.conn, "idle"); err != nil {
		t.Fatal(err)
	}
	s := fix2Session(t, e.m)
	deadline := time.Now().Add(3 * time.Second)
	for s.retainedBytes.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := s.retainedBytes.Load(); n != 0 {
		t.Fatalf("idle session retains %d bytes for recovery", n)
	}
}

// ING-14 is covered in internal/link; this checks the selection side of it:
// a suspect endpoint (failed probe) goes after a healthy one.
func TestFix2SuspectEndpointOrderedLast(t *testing.T) {
	mgr := link.NewPeerLinkManager(config.PeerConfig{ID: "B", Failover: config.FailoverConfig{UnhealthyThreshold: 2}, Endpoints: []config.EndpointConfig{{ID: "a", URL: "http://a", Priority: 1}, {ID: "b", URL: "http://b", Priority: 2}}})
	mgr.RecordFailure("a", false)
	got := orderedEndpoints(mgr, true)
	if got[0].ID != "b" || got[1].ID != "a" {
		t.Fatalf("order after a failed primary probe: %s,%s", got[0].ID, got[1].ID)
	}
	if got := orderedEndpoints(mgr, false); len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("failover disabled must use only the first endpoint")
	}
}
