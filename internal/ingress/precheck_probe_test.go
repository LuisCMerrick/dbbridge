package ingress

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/egress"
	"dbbridge/internal/link"
	"dbbridge/internal/session"
	"dbbridge/internal/transport"
)

type testEnv struct {
	m        *IngressManager
	conn     net.Conn
	acks     atomic.Int64
	probes   atomic.Int64
	wsChecks atomic.Int64
	wsDelay  atomic.Int64 // ns
	reg      *session.Registry
}

func testEcho(t testing.TB) net.Listener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); io.Copy(c, c) }()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln
}

// testSetup builds one egress (default transport config: ack_max_delay 200ms,
// heartbeat 15s) behind a counting proxy and an ingress manager for it.
func testSetup(t *testing.T, mode string, dial bool, tweak func(*config.Config)) *testEnv {
	e := &testEnv{}
	ln := testEcho(t)
	t.Setenv("DBB_TEST_TOKEN", "test-token")
	b := &config.Config{Node: config.NodeConfig{ID: "B"}, HTTP: config.HTTPConfig{Enabled: true}, Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{{ID: "A", TokenEnvs: []string{"DBB_TEST_TOKEN"}, AllowedTargets: []string{"db"}, MaxSessions: 10}}}, Targets: map[string]config.TargetConfig{"db": {Address: ln.Addr().String()}}}
	if err := b.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	e.reg = session.NewRegistry(b.Transport.SuspendedBufferLimit, b.Transport.GlobalBufferLimit)
	ba := auth.NewAuthenticator(b)
	srv := egress.NewServer(b, ba, e.reg)
	base := srv.Handler()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/ack") {
			e.acks.Add(1)
		}
		if r.URL.Path == "/v1/peer/probe" {
			e.probes.Add(1)
		}
		if r.URL.Path == "/v1/transport/ws" {
			for k := range r.Header {
				if strings.Contains(strings.ToLower(k), "purpose") && r.Header.Get(k) == "check" {
					e.wsChecks.Add(1)
					if d := e.wsDelay.Load(); d > 0 {
						time.Sleep(time.Duration(d))
					}
				}
			}
		}
		base.ServeHTTP(w, r)
	}))
	a := &config.Config{Node: config.NodeConfig{ID: "A"}, Peers: []config.PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "DBB_TEST_TOKEN", Endpoints: []config.EndpointConfig{{ID: "e1", URL: ts.URL}}}}, Listeners: []config.ListenerConfig{{ID: "local", Listen: "127.0.0.1:0", Peer: "B", Target: "db"}}, Transport: config.TransportConfig{Protocol: mode}}
	if tweak != nil {
		tweak(a)
	}
	if err := a.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	aa := auth.NewAuthenticator(a)
	e.m = NewIngressManager(a, aa)
	if dial {
		if err := e.m.Start(); err != nil {
			t.Fatal(err)
		}
		c, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		e.conn = c
	}
	t.Cleanup(func() {
		if e.conn != nil {
			e.conn.Close()
		}
		e.m.Close()
		srv.Close()
		ts.Close()
		aa.Close()
		ba.Close()
	})
	return e
}

func testEcho2(t *testing.T, c net.Conn, msg string) error {
	t.Helper()
	if _, err := c.Write([]byte(msg)); err != nil {
		return err
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(c, buf); err != nil {
		return err
	}
	if string(buf) != msg {
		t.Fatalf("echo got %q want %q", buf, msg)
	}
	return nil
}

// An idle session must not POST /ack every ack_max_delay, but data must
// still be acknowledged.
func TestIdleSessionNoAckStorm(t *testing.T) {
	for _, mode := range []string{"ws", "http-sse"} {
		t.Run(mode, func(t *testing.T) {
			e := testSetup(t, mode, true, nil)
			e.conn.SetDeadline(time.Now().Add(8 * time.Second))
			if err := testEcho2(t, e.conn, "hi"); err != nil {
				t.Fatal(err)
			}
			time.Sleep(400 * time.Millisecond)
			before := e.acks.Load()
			time.Sleep(2 * time.Second)
			if n := e.acks.Load() - before; n > 2 {
				t.Fatalf("idle %s session sent %d ACK POSTs in 2s with nothing to acknowledge", mode, n)
			}
			// Activity re-arms the ack timer: the downstream bytes get acknowledged.
			before = e.acks.Load()
			if err := testEcho2(t, e.conn, "again"); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(2 * time.Second)
			for e.acks.Load() == before && time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
			}
			if e.acks.Load() == before {
				t.Fatalf("downstream data after idle period was never acknowledged")
			}
			if s := e.reg.GetAllSessions(); len(s) != 1 {
				t.Fatalf("sessions on egress=%d", len(s))
			}
		})
	}
}

// The precheck leader's own context timeout must not be cached and served
// to later callers as "WS upgrade rejected".
func TestPrecheckNotPoisonedByCallerCtx(t *testing.T) {
	e := testSetup(t, "auto", false, nil)
	e.wsDelay.Store(int64(400 * time.Millisecond))
	p := e.m.cfg.Peers[0]
	c := e.m.clients[p.ID]
	pr, err := e.m.probe(context.Background(), p.ID, c, p.ExpectedNodeID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err1 := e.m.precheck(ctx, p.ID, c, p.ExpectedNodeID, pr.InstanceID)
	cancel()
	if !errors.Is(err1, context.DeadlineExceeded) {
		t.Fatalf("leader should see its own deadline, got %v", err1)
	}
	e.wsDelay.Store(0)
	time.Sleep(600 * time.Millisecond) // shared check (detached) completes meanwhile
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if err2 := e.m.precheck(ctx2, p.ID, c, p.ExpectedNodeID, pr.InstanceID); err2 != nil {
		t.Fatalf("second caller after a cancelled leader got %v", err2)
	}
}

// A transient precheck failure is not cached; a definitive answer is.
func TestPrecheckCachesOnlyDefinitiveAnswers(t *testing.T) {
	transient := []error{
		context.Canceled,
		context.DeadlineExceeded,
		errLocalBusy,
		errors.New("websocket upgrade rejected by gateway or peer: read tcp 1->2: i/o timeout"),
		errors.New("websocket upgrade rejected by gateway or peer: context canceled"),
		errors.New("websocket upgrade rejected by gateway or peer: dial tcp: connection refused"),
		errors.New("websocket upgrade rejected by gateway or peer: status 503"),
		&transport.RemoteError{Status: 500, Trusted: true},
	}
	for _, e := range transient {
		if precheckCacheable(e) {
			t.Errorf("transient precheck error cached: %v", e)
		}
	}
	definitive := []error{
		nil,
		wrapRejected("status 404"),
		errors.New("node ID mismatch in hello"), // unclassified, non-network
	}
	for _, e := range definitive {
		if !precheckCacheable(e) {
			t.Errorf("definitive precheck answer not cached: %v", e)
		}
	}
}

func wrapRejected(detail string) error {
	return &wrapped{msg: transport.ErrWSUpgradeRejected.Error() + ": " + detail, inner: transport.ErrWSUpgradeRejected}
}

type wrapped struct {
	msg   string
	inner error
}

func (w *wrapped) Error() string { return w.msg }
func (w *wrapped) Unwrap() error { return w.inner }

// After one cancelled precheck a ws-only peer must still accept sessions.
func TestPrecheckCancelDoesNotBreakWSOnly(t *testing.T) {
	e := testSetup(t, "ws", false, nil)
	p := e.m.cfg.Peers[0]
	c := e.m.clients[p.ID]
	pr, err := e.m.probe(context.Background(), p.ID, c, p.ExpectedNodeID)
	if err != nil {
		t.Fatal(err)
	}
	e.wsDelay.Store(int64(300 * time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	_ = e.m.precheck(ctx, p.ID, c, p.ExpectedNodeID, pr.InstanceID)
	e.wsDelay.Store(0)
	if err := e.m.Start(); err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(4 * time.Second))
	if err := testEcho2(t, conn, "hi"); err != nil {
		t.Fatalf("ws-only session after a cancelled precheck: %v (sessions=%d)", err, len(e.reg.GetAllSessions()))
	}
}

// A health probe that cannot obtain a local probe slot is not an endpoint failure.
func TestLocalProbeContentionNotEndpointFailure(t *testing.T) {
	e := testSetup(t, "auto", false, func(a *config.Config) {
		a.Peers[0].Failover.HealthcheckIntervalRaw = "30ms"
		a.Peers[0].Failover.HealthcheckTimeoutRaw = "20ms"
	})
	p := e.m.cfg.Peers[0]
	slots := e.m.probeSlots[p.ID]
	for i := 0; i < cap(slots); i++ {
		slots <- struct{}{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	_, err := e.m.probe(ctx, p.ID, e.m.clients[p.ID], p.ExpectedNodeID)
	cancel()
	if !errors.Is(err, errLocalBusy) {
		t.Fatalf("slot contention should be errLocalBusy, got %v", err)
	}
	if err := e.m.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // several health rounds, all locally starved
	ep := e.m.linkManagers[p.ID].GetEndpoints()[0]
	if e.probes.Load() != 0 {
		t.Fatalf("probe reached the server despite full slots")
	}
	if st := ep.GetState(); st != link.StateUnknown || !ep.ProbeEligible(time.Now()) {
		t.Fatalf("local contention changed endpoint health: state=%s eligible=%v", st, ep.ProbeEligible(time.Now()))
	}
	// The session path treats it as temporary too: no failure recorded.
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, _, err := e.m.choose(ctx, p, "", "auto", true); err == nil || errors.Is(err, transport.ErrProtocol) {
		t.Fatalf("choose with no local capacity: %v", err)
	}
	if st := ep.GetState(); st != link.StateUnknown || !ep.ProbeEligible(time.Now()) {
		t.Fatalf("choose recorded local contention as endpoint failure: state=%s", st)
	}
}

// Creating a session probes the chosen endpoint once and reuses the result.
func TestSingleProbePerCreate(t *testing.T) {
	e := testSetup(t, "ws", true, nil)
	e.conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := testEcho2(t, e.conn, "hi"); err != nil {
		t.Fatal(err)
	}
	if n := e.probes.Load(); n != 1 {
		t.Fatalf("one session create issued %d probes, want 1", n)
	}
	if n := e.wsChecks.Load(); n != 1 {
		t.Fatalf("one session create issued %d WS prechecks, want 1", n)
	}
}

// Failures below unhealthy_threshold keep the endpoint selectable.
func TestCooldownRespectsUnhealthyThreshold(t *testing.T) {
	mgr := link.NewPeerLinkManager(config.PeerConfig{ID: "B", Failover: config.FailoverConfig{UnhealthyThreshold: 2, Cooldown: time.Minute}, Endpoints: []config.EndpointConfig{{ID: "e1", URL: "http://127.0.0.1:1"}}})
	ep := mgr.GetEndpoints()[0]
	mgr.RecordFailure("e1", false)
	if !ep.ProbeEligible(time.Now()) || !ep.Available(time.Now(), "", true) {
		t.Fatalf("one failure below unhealthy_threshold=2 put the endpoint in cooldown")
	}
	mgr.RecordFailure("e1", false)
	if ep.ProbeEligible(time.Now()) || ep.GetState() != link.StateUnhealthy {
		t.Fatalf("reaching unhealthy_threshold must start cooldown: state=%s", ep.GetState())
	}
	mgr2 := link.NewPeerLinkManager(config.PeerConfig{ID: "B", Failover: config.FailoverConfig{UnhealthyThreshold: 2, Cooldown: time.Minute}, Endpoints: []config.EndpointConfig{{ID: "e1", URL: "http://127.0.0.1:1"}}})
	mgr2.RecordFailure("e1", true)
	if mgr2.GetEndpoints()[0].ProbeEligible(time.Now()) {
		t.Fatalf("identity mismatch must cool down immediately")
	}
}

func testEgress(t *testing.T, target string) (*httptest.Server, *session.Registry) {
	b := &config.Config{Node: config.NodeConfig{ID: "B"}, HTTP: config.HTTPConfig{Enabled: true}, Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{{ID: "A", TokenEnvs: []string{"DBB_TEST_TOKEN"}, AllowedTargets: []string{"db"}, MaxSessions: 10}}}, Targets: map[string]config.TargetConfig{"db": {Address: target}}, Transport: config.TransportConfig{Recovery: config.RecoveryConfig{EnabledRaw: true, GraceRaw: "10s"}}}
	if err := b.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	reg := session.NewRegistry(b.Transport.SuspendedBufferLimit, b.Transport.GlobalBufferLimit)
	ba := auth.NewAuthenticator(b)
	srv := egress.NewServer(b, ba, reg)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); srv.Close(); ba.Close() })
	return ts, reg
}

// With a backup endpoint that is a different egress instance, a carrier
// drop on the owner (whose WS transport is briefly cooling down) must recover,
// not abort as a protocol fault.
func TestRecoveryWithBackupInstance(t *testing.T) {
	t.Setenv("DBB_TEST_TOKEN", "test-token")
	ln := testEcho(t)
	primary, preg := testEgress(t, ln.Addr().String())
	backup, breg := testEgress(t, ln.Addr().String())
	a := &config.Config{Node: config.NodeConfig{ID: "A"}, Peers: []config.PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "DBB_TEST_TOKEN", Failover: config.FailoverConfig{CooldownRaw: "200ms"}, Endpoints: []config.EndpointConfig{{ID: "primary", URL: primary.URL, Priority: 0}, {ID: "backup", URL: backup.URL, Priority: 10}}}}, Listeners: []config.ListenerConfig{{ID: "local", Listen: "127.0.0.1:0", Peer: "B", Target: "db"}}, Transport: config.TransportConfig{Protocol: "ws", Recovery: config.RecoveryConfig{EnabledRaw: true, GraceRaw: "3s", BarrierTimeoutRaw: "1s"}}}
	if err := a.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	aa := auth.NewAuthenticator(a)
	defer aa.Close()
	m := NewIngressManager(a, aa)
	defer m.Close()
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", m.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(6 * time.Second))
	if err := testEcho2(t, conn, "first"); err != nil {
		t.Fatal(err)
	}
	s := preg.GetAllSessions()[0]
	s.SetStreamCancel(s.GetEpoch(), s.CurrentStreamID(), func() {}) // drop the WS carrier on the primary
	time.Sleep(1500 * time.Millisecond)
	if err := testEcho2(t, conn, "after"); err != nil {
		t.Fatalf("recovery aborted although the owner endpoint was reachable: %v", err)
	}
	if s.GetEpoch() < 2 {
		t.Fatalf("session did not resume on the owner: epoch=%d", s.GetEpoch())
	}
	if n := len(breg.GetAllSessions()); n != 0 {
		t.Fatalf("backup instance got %d sessions", n)
	}
}

// An instance mismatch is still fatal when no endpoint reports the owner.
func TestRecoveryMismatchFatalWithoutOwner(t *testing.T) {
	t.Setenv("DBB_TEST_TOKEN", "test-token")
	ln := testEcho(t)
	backup, _ := testEgress(t, ln.Addr().String())
	a := &config.Config{Node: config.NodeConfig{ID: "A"}, Peers: []config.PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "DBB_TEST_TOKEN", Endpoints: []config.EndpointConfig{{ID: "backup", URL: backup.URL}}}}, Listeners: []config.ListenerConfig{{ID: "local", Listen: "127.0.0.1:0", Peer: "B", Target: "db"}}, Transport: config.TransportConfig{Protocol: "http-sse"}}
	if err := a.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	aa := auth.NewAuthenticator(a)
	defer aa.Close()
	m := NewIngressManager(a, aa)
	defer m.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, _, err := m.choose(ctx, m.cfg.Peers[0], "some-other-owner", "http-sse", false)
	if !errors.Is(err, transport.ErrProtocol) {
		t.Fatalf("owner absent from every reachable endpoint should be a protocol fault, got %v", err)
	}
}
