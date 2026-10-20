package ingress

import (
	"bytes"
	"context"
	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/egress"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"encoding/json"
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

func TestAudit20261017RecoveryStopsAfterTrustedSelectionRefusal(t *testing.T) {
	var refused atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/probe" {
			auditProbeResponse(w, r, true)
			return
		}
		refused.Add(1)
		w.Header().Set(protocol.HeaderInstanceID, "bi")
		w.WriteHeader(401)
		json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: protocol.ErrUnauthorized, Retryable: false})
	}))
	defer ts.Close()
	m, p := auditChooser(t, ts.URL)
	local, client := net.Pipe()
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &ingressSession{m: m, p: p, conn: local, ctx: ctx, cancel: cancel, changed: make(chan struct{}), gate: make(chan struct{}, 1), grace: 1400 * time.Millisecond, owner: "bi", sid: "existing-session", token: "session-token", recovery: true, epoch: 1}
	s.gate <- struct{}{}
	started := time.Now()
	if s.recover(io.ErrUnexpectedEOF, "ws") {
		t.Fatal("unauthorized recovery succeeded")
	}
	elapsed := time.Since(started)
	client.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := client.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("client socket was not fully closed: %v", err)
	}
	t.Logf("trusted nonretryable 401 refusals=%d; local close after %s with grace=1.4s", refused.Load(), elapsed)
	if refused.Load() == 0 {
		t.Fatal("fixture never returned a trusted refusal")
	}
	if elapsed > time.Second {
		t.Fatal("a deterministic authorization refusal was retried until grace expiry instead of immediately terminating")
	}
}

// End-to-end: HTTP control remains healthy, the first recovery carrier is
// unavailable at the primary endpoint, and the backup is fully operational.
func TestAudit20261017RecoverySkipsFailedReattach(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var sockets sync.Map
	var dials atomic.Int64
	go func() {
		for {
			c, e := backend.Accept()
			if e != nil {
				return
			}
			dials.Add(1)
			sockets.Store(c, true)
			go func() { defer sockets.Delete(c); defer c.Close(); io.Copy(c, c) }()
		}
	}()
	t.Setenv("AUDIT_20261017_TOKEN", "isolated-audit-credential")
	b := &config.Config{Node: config.NodeConfig{ID: "B"}, HTTP: config.HTTPConfig{Enabled: true}, Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{{ID: "A", TokenEnvs: []string{"AUDIT_20261017_TOKEN"}, AllowedTargets: []string{"db"}, MaxSessions: 10}}}, Targets: map[string]config.TargetConfig{"db": {Address: backend.Addr().String()}}, Transport: config.TransportConfig{HeartbeatRaw: "50ms", FailureDetectTimeoutRaw: "500ms", Recovery: config.RecoveryConfig{EnabledRaw: true, GraceRaw: "1200ms", BarrierTimeoutRaw: "100ms"}}}
	if err = b.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	ba := auth.NewAuthenticator(b)
	reg := session.NewRegistry(b.Transport.SuspendedBufferLimit, b.Transport.GlobalBufferLimit)
	srv := egress.NewServer(b, ba, reg)
	base := srv.Handler()
	var failed atomic.Bool
	var primaryAttachFailures, backupRequests, resumes atomic.Int64
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/resume") {
			resumes.Add(1)
		}
		if failed.Load() && strings.HasSuffix(r.URL.Path, "/events") {
			primaryAttachFailures.Add(1)
			w.WriteHeader(503)
			return
		}
		base.ServeHTTP(w, r)
	}))
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { backupRequests.Add(1); base.ServeHTTP(w, r) }))
	a := &config.Config{Node: config.NodeConfig{ID: "A"}, Peers: []config.PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "AUDIT_20261017_TOKEN", Endpoints: []config.EndpointConfig{{ID: "primary", URL: primary.URL, Priority: 0}, {ID: "backup", URL: backup.URL, Priority: 1}}, Failover: config.FailoverConfig{CooldownRaw: "5s"}}}, Listeners: []config.ListenerConfig{{ID: "local", Listen: "127.0.0.1:0", Peer: "B", Target: "db"}}, Transport: config.TransportConfig{Protocol: "auto", HeartbeatRaw: "50ms", FailureDetectTimeoutRaw: "500ms", AckMaxDelayRaw: "30ms", ControlRequestTimeoutRaw: "250ms", Recovery: config.RecoveryConfig{EnabledRaw: true, GraceRaw: "1200ms", BarrierTimeoutRaw: "100ms"}}}
	if err = a.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	aa := auth.NewAuthenticator(a)
	m := NewIngressManager(a, aa)
	if err = m.Start(); err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", m.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		m.Close()
		srv.Close()
		primary.Close()
		backup.Close()
		aa.Close()
		ba.Close()
		backend.Close()
		sockets.Range(func(k, v any) bool { k.(net.Conn).Close(); return true })
	})
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	conn.Write([]byte("before"))
	got := make([]byte, 6)
	if _, err = io.ReadFull(conn, got); err != nil || !bytes.Equal(got, []byte("before")) {
		t.Fatalf("initial echo: %q %v", got, err)
	}
	var current *ingressSession
	m.activeConns.Range(func(k, v any) bool { current = v.(*activeConnEntry).session; return false })
	if current == nil {
		t.Fatal("missing active ingress")
	}
	current.mu.Lock()
	old := current.g
	current.mu.Unlock()
	if old.mode != "ws" {
		t.Fatal("initial carrier must be ws")
	}
	failed.Store(true)
	old.ws.Close()
	conn.Write([]byte("after!"))
	_, err = io.ReadFull(conn, got)
	t.Logf("after recovery: read_error=%v primary_failed_reattaches=%d backup_requests=%d resume_calls=%d backend_dials=%d", err, primaryAttachFailures.Load(), backupRequests.Load(), resumes.Load(), dials.Load())
	if err != nil {
		t.Fatalf("a healthy backup exists, but recovery exhausted without trying it: %v", err)
	}
	if !bytes.Equal(got, []byte("after!")) || dials.Load() != 1 {
		t.Fatalf("stream/socket changed: %q dials=%d", got, dials.Load())
	}
}
