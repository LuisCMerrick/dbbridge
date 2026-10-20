package ingress

import (
	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/egress"
	"dbbridge/internal/session"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testRouteEnv struct {
	manager                                                         *IngressManager
	conn                                                            net.Conn
	registry                                                        *session.Registry
	badCreate, badResume                                            atomic.Bool
	primaryCreate, backupCreate, primaryResume, backupResume, dials atomic.Int64
}

func testRoutes(t *testing.T, badCreate bool, mode string) *testRouteEnv {
	t.Helper()
	e := &testRouteEnv{}
	e.badCreate.Store(badCreate)
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var sockets sync.Map
	go func() {
		for {
			c, err := backend.Accept()
			if err != nil {
				return
			}
			e.dials.Add(1)
			sockets.Store(c, true)
			go func() {
				defer c.Close()
				defer sockets.Delete(c)
				_, _ = c.Write([]byte("hello"))
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	t.Setenv("DBB_TEST_ROUTE_TOKEN", "test-credential")
	b := &config.Config{Node: config.NodeConfig{ID: "B"}, Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{{ID: "A", TokenEnvs: []string{"DBB_TEST_ROUTE_TOKEN"}, AllowedTargets: []string{"db"}}}}, Targets: map[string]config.TargetConfig{"db": {Address: backend.Addr().String()}}, Transport: config.TransportConfig{HeartbeatRaw: "50ms", FailureDetectTimeoutRaw: "500ms", Recovery: config.RecoveryConfig{GraceRaw: "1400ms", BarrierTimeoutRaw: "100ms"}}}
	if err = b.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	ba := auth.NewAuthenticator(b)
	e.registry = session.NewRegistry(b.Transport.SuspendedBufferLimit, b.Transport.GlobalBufferLimit)
	srv := egress.NewServer(b, ba, e.registry)
	base := srv.Handler()
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/sessions" {
			e.primaryCreate.Add(1)
			if e.badCreate.Load() {
				base.ServeHTTP(httptest.NewRecorder(), r)
				http.Error(w, "lost create reply at primary gateway", http.StatusBadGateway)
				return
			}
		}
		if strings.HasSuffix(r.URL.Path, "/resume") {
			e.primaryResume.Add(1)
			if e.badResume.Load() {
				http.Error(w, "primary resume path unavailable", http.StatusBadGateway)
				return
			}
		}
		base.ServeHTTP(w, r)
	}))
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/sessions" {
			e.backupCreate.Add(1)
		}
		if strings.HasSuffix(r.URL.Path, "/resume") {
			e.backupResume.Add(1)
		}
		base.ServeHTTP(w, r)
	}))
	a := &config.Config{Node: config.NodeConfig{ID: "A"}, Peers: []config.PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "DBB_TEST_ROUTE_TOKEN", Endpoints: []config.EndpointConfig{{ID: "primary", URL: primary.URL}, {ID: "backup", URL: backup.URL, Priority: 10}}, Failover: config.FailoverConfig{CooldownRaw: "0s"}}}, Listeners: []config.ListenerConfig{{ID: "db", Listen: "127.0.0.1:0", Peer: "B", Target: "db"}}, Transport: config.TransportConfig{Protocol: mode, HeartbeatRaw: "50ms", FailureDetectTimeoutRaw: "500ms", AckMaxDelayRaw: "30ms", ControlRequestTimeoutRaw: "200ms", CreateRetryTimeoutRaw: "1400ms", Recovery: config.RecoveryConfig{GraceRaw: "1400ms", BarrierTimeoutRaw: "100ms"}}}
	if err = a.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	aa := auth.NewAuthenticator(a)
	e.manager = NewIngressManager(a, aa)
	if err = e.manager.Start(); err != nil {
		t.Fatal(err)
	}
	e.conn, err = net.Dial("tcp", e.manager.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		e.conn.Close()
		e.manager.Close()
		srv.Close()
		primary.Close()
		backup.Close()
		aa.Close()
		ba.Close()
		backend.Close()
		sockets.Range(func(k, v any) bool { k.(net.Conn).Close(); return true })
	})
	return e
}

func TestCreateReplyLossDoesNotTryBackup(t *testing.T) {
	for _, mode := range []string{"auto", "ws", "http-sse"} {
		t.Run(mode, func(t *testing.T) {
			e := testRoutes(t, true, mode)
			e.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			b := make([]byte, 5)
			_, err := io.ReadFull(e.conn, b)
			t.Logf("create result=%v primary=%d backup=%d backend_dials=%d", err, e.primaryCreate.Load(), e.backupCreate.Load(), e.dials.Load())
			if err != nil || string(b) != "hello" || e.primaryCreate.Load() == 0 || e.backupCreate.Load() == 0 || e.dials.Load() != 1 {
				t.Fatal("backup did not retrieve the original create result without another backend connection")
			}
		})
	}
}

func TestResumeFailureDoesNotTryBackup(t *testing.T) {
	for _, mode := range []string{"auto", "ws", "http-sse"} {
		t.Run(mode, func(t *testing.T) { testResumeBackup(t, mode) })
	}
}

func testResumeBackup(t *testing.T, mode string) {
	e := testRoutes(t, false, mode)
	e.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	b := make([]byte, 5)
	if _, err := io.ReadFull(e.conn, b); err != nil {
		t.Fatal(err)
	}
	all := e.registry.GetAllSessions()
	if len(all) != 1 {
		t.Fatal("fixture session count")
	}
	s := all[0]
	originalBackend := s.BackendConn
	e.badResume.Store(true)
	if err := s.SetStreamCancel(s.GetEpoch(), s.CurrentStreamID(), func() {}); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(3 * time.Second)
	for (s.GetEpoch() == 1 || !s.IsDataPlaneActive()) && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if s.GetEpoch() != 2 || !s.IsDataPlaneActive() || e.primaryResume.Load() == 0 || e.backupResume.Load() == 0 {
		t.Fatal("healthy probe kept selecting the broken primary resume path until session expired")
	}
	e.conn.SetDeadline(time.Now().Add(time.Second))
	if _, err := e.conn.Write([]byte("after-resume")); err != nil {
		t.Fatal(err)
	}
	b = make([]byte, len("after-resume"))
	if _, err := io.ReadFull(e.conn, b); err != nil || string(b) != "after-resume" {
		t.Fatalf("resumed byte stream did not progress: %q %v", b, err)
	}
	if e.dials.Load() != 1 || s.BackendConn != originalBackend || e.registry.Get(s.ID) != s {
		t.Fatal("resume replaced the original session or backend socket")
	}
	t.Logf("after transport loss: exact echo=%q primary_resume=%d backup_resume=%d backend_dials=%d epoch=%d original_backend=true", b, e.primaryResume.Load(), e.backupResume.Load(), e.dials.Load(), s.GetEpoch())
}

func TestCodecWaitMisclassifiedAsNetworkLoss(t *testing.T) {
	for _, mode := range []string{"ws", "http-sse"} {
		t.Run(mode, func(t *testing.T) { testCodecWait(t, mode) })
	}
}

func testCodecWait(t *testing.T, mode string) {
	e := e2eIngress(t, mode, "")
	until := time.Now().Add(time.Second)
	var s *ingressSession
	for time.Now().Before(until) {
		e.manager.activeConns.Range(func(_, v any) bool { s = v.(*activeConnEntry).session; return false })
		if s != nil {
			s.mu.Lock()
			ready := s.creationDone
			s.mu.Unlock()
			if ready {
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if s == nil {
		t.Fatal("fixture session missing")
	}
	budget := e.cfg.Resources.Codec
	for !budget.Try(budget.Limit()) {
		if time.Now().After(until) {
			t.Fatal("codec fixture")
		}
		time.Sleep(time.Millisecond)
	}
	var release sync.Once
	free := func() { release.Do(func() { budget.Put(budget.Limit()) }) }
	defer free()
	s.mu.Lock()
	controlBefore := s.lastControl
	s.mu.Unlock()
	all := e.reg.GetAllSessions()
	if len(all) != 1 {
		t.Fatal("fixture backend missing")
	}
	if _, err := all[0].AppendDownBytes([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	timer := time.AfterFunc(350*time.Millisecond, free)
	defer timer.Stop()
	e.conn.SetReadDeadline(time.Now().Add(time.Second))
	b := make([]byte, 5)
	_, err := io.ReadFull(e.conn, b)
	s.mu.Lock()
	controlAfter := s.lastControl
	s.mu.Unlock()
	t.Logf("codec wait: result=%v elapsed=%s detect=%s backpressure=%s control_request_timeout=%s control_progress=%t last_control_age=%s", err, time.Since(started), e.cfg.Transport.FailureDetectTimeout, e.cfg.Transport.BackpressureTimeout, e.cfg.Transport.ControlRequestTimeout, controlAfter.After(controlBefore), time.Since(controlAfter))
	if !controlAfter.After(controlBefore) {
		t.Fatal("fixture did not observe independent control progress")
	}
	if err != nil {
		t.Error("local codec contention terminated the socket before its backpressure deadline despite a healthy independent control path")
	}
}
