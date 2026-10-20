package ingress

import (
	"bytes"
	"context"
	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/link"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"dbbridge/internal/transport"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func auditChooser(t *testing.T, urls ...string) (*IngressManager, config.PeerConfig) {
	t.Helper()
	t.Setenv("AUDIT_V2_TOKEN", "audit-v2-token")
	p := config.PeerConfig{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "AUDIT_V2_TOKEN"}
	for i, u := range urls {
		p.Endpoints = append(p.Endpoints, config.EndpointConfig{ID: string(rune('a' + i)), URL: u, Priority: i})
	}
	c := &config.Config{Node: config.NodeConfig{ID: "A"}, Peers: []config.PeerConfig{p}}
	if err := c.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	a := auth.NewAuthenticator(c)
	t.Cleanup(a.Close)
	m := NewIngressManager(c, a)
	t.Cleanup(func() {
		for _, cl := range m.clients {
			cl.Close()
		}
	})
	return m, c.Peers[0]
}
func auditProbeResponse(w http.ResponseWriter, r *http.Request, accept bool) {
	var q protocol.ProbeRequest
	_ = json.NewDecoder(r.Body).Decode(&q)
	_ = json.NewEncoder(w).Encode(protocol.ProbeResponse{ProtocolVersion: "1.4", NodeID: "B", InstanceID: "bi", Nonce: q.Nonce, AcceptingNewSessions: accept, RecoveryAvailable: true, Transports: []string{"ws", "http-sse"}})
}
func TestAuditV2SafeFallback(t *testing.T) {
	for _, mode := range []string{"reset", "ordinary_200", "trusted_unsupported", "raw_403"} {
		t.Run(mode, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/peer/probe" {
					auditProbeResponse(w, r, true)
					return
				}
				switch mode {
				case "reset":
					c, _, _ := w.(http.Hijacker).Hijack()
					c.Close()
				case "ordinary_200":
					w.Write([]byte("gateway ordinary response"))
				case "trusted_unsupported":
					w.Header().Set(protocol.HeaderInstanceID, "bi")
					w.WriteHeader(409)
					json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: protocol.ErrTransportUnsupported})
				case "raw_403":
					w.WriteHeader(403)
				}
			}))
			defer ts.Close()
			m, p := auditChooser(t, ts.URL)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, _, got, err := m.choose(ctx, p, "", "auto", true)
			if err != nil || got != "http-sse" {
				t.Fatalf("safe fallback rejected: mode=%s transport=%q err=%v", mode, got, err)
			}
		})
	}
}
func TestAuditV2BadCandidateDoesNotHideBackup(t *testing.T) {
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/probe" {
			auditProbeResponse(w, r, true)
			return
		}
		up := websocket.Upgrader{Subprotocols: []string{protocol.SecWebSocketProtocol}}
		c, e := up.Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer c.Close()
		c.WriteJSON(protocol.WSHelloMessage{Type: "hello", ProtocolVersion: "1.4", NodeID: "WRONG", InstanceID: "bi", Nonce: r.Header.Get(protocol.HeaderWSNonce), Transport: "ws"})
	}))
	defer primary.Close()
	var backupCalls atomic.Int64
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backupCalls.Add(1)
		var q protocol.ProbeRequest
		json.NewDecoder(r.Body).Decode(&q)
		json.NewEncoder(w).Encode(protocol.ProbeResponse{ProtocolVersion: "1.4", NodeID: "B", InstanceID: "bi", Nonce: q.Nonce, AcceptingNewSessions: true, RecoveryAvailable: true, Transports: []string{"http-sse"}})
	}))
	defer backup.Close()
	control, cp := auditChooser(t, backup.URL)
	controlCtx, controlCancel := context.WithTimeout(context.Background(), time.Second)
	_, _, controlMode, controlErr := control.choose(controlCtx, cp, "", "auto", true)
	controlCancel()
	if controlErr != nil || controlMode != "http-sse" {
		t.Fatalf("backup control failed: %v mode=%s", controlErr, controlMode)
	}
	backupCalls.Store(0)
	m, p := auditChooser(t, primary.URL, backup.URL)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, ep, _, err := m.choose(ctx, p, "", "auto", true)
	if err != nil || ep == nil || ep.URL != backup.URL {
		t.Fatalf("valid backup not considered: calls=%d endpoint=%v err=%v", backupCalls.Load(), ep, err)
	}
}
func TestAuditV2LocalBackpressureNotLinkFailure(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var controls atomic.Int64
	controlServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q protocol.AckRequest
		json.NewDecoder(r.Body).Decode(&q)
		controls.Add(1)
		json.NewEncoder(w).Encode(protocol.AckResponse{SessionID: "sid", InstanceID: "bi", Epoch: "1", StreamID: "stream", Transport: &protocol.TransportMode{Mode: "ws"}, AckUpOffset: "0", DownAckOffset: "0", ProbeID: q.ProbeID})
	}))
	defer controlServer.Close()
	cfg := &config.Config{Transport: config.TransportConfig{FailureDetectTimeout: 80 * time.Millisecond, SocketWriteTimeout: time.Second, AckMaxDelay: 20 * time.Millisecond, ControlRequestTimeout: 50 * time.Millisecond, DialTimeout: time.Second}}
	c := transport.NewTripleTransportClientForURL(controlServer.URL, "token", &cfg.Transport, "ai", true)
	defer c.Close()
	c.BindStream("sid", 1, "stream", "ws")
	s := &ingressSession{m: &IngressManager{cfg: cfg}, conn: a, ctx: ctx, cancel: cancel, sid: "sid", owner: "bi", changed: make(chan struct{}), faults: make(chan sessionFault, 8), gate: make(chan struct{}, 1)}
	s.gate <- struct{}{}
	g := &generation{ctx: ctx, cancel: cancel, c: c, ep: 1, mode: "ws", stream: "stream", active: true, lastActivity: time.Now(), pending: []wireMessage{{kind: "data", data: []byte("blocked downstream payload")}}}
	s.g = g
	done := make(chan struct{})
	go func() { s.receive(g); close(done) }()
	go s.watchActivity(g)
	go s.control(g)
	defer func() { s.closed.Store(true); cancel(); a.Close(); <-done }()
	select {
	case f := <-s.faults:
		t.Fatalf("healthy local write backpressure classified as transport failure before write/stall deadline: %v; successful HTTP controls=%d", f.err, controls.Load())
	case <-time.After(180 * time.Millisecond):
	}
}
func TestAuditV2ReleasedFrameStillRetained(t *testing.T) {
	res := resource.NewManager(1<<20, 1<<20, 1<<20, 10)
	payload := bytes.Repeat([]byte("x"), 256<<10)
	res.Payload.Try(int64(len(payload)))
	f := &retainedChunk{data: payload, cost: int64(len(payload))}
	s := &ingressSession{m: &IngressManager{cfg: &config.Config{Resources: res}}, frames: []*retainedChunk{f}, used: int64(len(payload)), upNext: uint64(len(payload))}
	if e := s.advanceUpLocked(uint64(len(payload))); e != nil {
		t.Fatal(e)
	}
	if res.Payload.Used() != 0 {
		t.Fatal("probe expected accounting release")
	}
	full := s.frames[:cap(s.frames)]
	if len(full) > 0 && full[0] != nil && len(full[0].data) > 0 {
		t.Fatalf("payload budget=0 but backing slice retains %d payload bytes", len(full[0].data))
	}
}
func TestAuditV2DrainingCreationRetryCandidate(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { auditProbeResponse(w, r, false) }))
	defer ts.Close()
	m, p := auditChooser(t, ts.URL)
	// This is the same choose(..., true) call used after a submitted create lost its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, _, err := m.choose(ctx, p, "bi", "http-sse", false)
	if err != nil {
		t.Fatalf("existing submitted create cannot reach cached result while draining: %v", err)
	}
}

func TestAuditV2HealthThresholdAffectsActualChoice(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { auditProbeResponse(w, r, true) }))
	defer ts.Close()
	m, p := auditChooser(t, ts.URL)
	mgr := m.linkManagers[p.ID]
	mgr.RecordFailure("a", false)
	mgr.RecordFailure("a", false)
	// A fresh past-cooldown state, with no concurrent health workers in this fixture.
	ep := mgr.GetEndpoints()[0]
	ep.CooldownUntil = time.Now().Add(-time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, selected, _, err := m.choose(ctx, p, "", "http-sse", true)
	if err == nil && selected == ep && ep.GetState() == link.StateUnhealthy {
		t.Fatalf("actual scheduler selected UNHEALTHY endpoint after one success (required=%d)", p.Failover.HealthyThreshold)
	}
}

func TestAuditV2CreateDeadlineClosesSilentWSReady(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/peer/probe":
			auditProbeResponse(w, r, true)
		case "/v1/sessions":
			var q protocol.CreateSessionRequest
			json.NewDecoder(r.Body).Decode(&q)
			w.WriteHeader(201)
			json.NewEncoder(w).Encode(protocol.CreateSessionResponse{ProtocolVersion: "1.4", NodeID: "B", InstanceID: "bi", SessionID: "silent-ready-session", SessionToken: "session-token", Epoch: "1", Transport: q.Transport, Recovery: q.Recovery, Limits: q.Limits, UnattachedTimeoutMs: 30000})
		case "/v1/transport/ws":
			up := websocket.Upgrader{Subprotocols: []string{protocol.SecWebSocketProtocol}}
			c, e := up.Upgrade(w, r, nil)
			if e != nil {
				return
			}
			defer c.Close()
			if r.Header.Get(protocol.HeaderWSPurpose) == "check" {
				c.WriteJSON(protocol.WSHelloMessage{Type: "hello", ProtocolVersion: "1.4", NodeID: "B", InstanceID: "bi", Nonce: r.Header.Get(protocol.HeaderWSNonce), Transport: "ws"})
			}
			c.ReadMessage()
		}
	}))
	defer ts.Close()
	t.Setenv("AUDIT_V2_CREATE", "create-token")
	cfg := &config.Config{Node: config.NodeConfig{ID: "A"}, Peers: []config.PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "AUDIT_V2_CREATE", Endpoints: []config.EndpointConfig{{ID: "e", URL: ts.URL}}}}, Listeners: []config.ListenerConfig{{ID: "l", Listen: "127.0.0.1:0", Peer: "B", Target: "db"}}, Transport: config.TransportConfig{Protocol: "ws", CreateRetryTimeoutRaw: "150ms", ControlRequestTimeoutRaw: "2s", WS: config.WSConfig{HandshakeTimeoutRaw: "1400ms"}, Recovery: config.RecoveryConfig{EnabledRaw: false}}}
	if e := cfg.ApplyDefaultsAndValidate(); e != nil {
		t.Fatal(e)
	}
	a := auth.NewAuthenticator(cfg)
	defer a.Close()
	m := NewIngressManager(cfg, a)
	if e := m.Start(); e != nil {
		t.Fatal(e)
	}
	defer m.Close()
	start := time.Now()
	c, e := net.Dial("tcp", m.listeners[0].Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetReadDeadline(start.Add(1250 * time.Millisecond))
	_, e = c.Read(make([]byte, 1))
	if ne, ok := e.(net.Error); ok && ne.Timeout() {
		t.Fatalf("client remains open %s after accept; creation deadline=150ms, terminal closure allowance=1s", time.Since(start))
	}
}

func TestAuditV2CleanupHonorsDisabledFailover(t *testing.T) {
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer primary.Close()
	var backups atomic.Int64
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backups.Add(1)
		if r.URL.Path == "/v1/peer/probe" {
			auditProbeResponse(w, r, true)
			return
		}
		w.WriteHeader(200)
	}))
	defer backup.Close()
	m, p := auditChooser(t, primary.URL, backup.URL)
	p.Failover.Enabled = config.BoolPtr(false)
	local, remote := net.Pipe()
	defer remote.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &ingressSession{m: m, p: p, conn: local, ctx: ctx, cancel: cancel, changed: make(chan struct{}), sid: "s", token: "token", owner: "bi", epoch: 1, g: &generation{c: m.clients[p.ID].WithURL(primary.URL)}}
	s.stop(context.Canceled)
	until := time.Now().Add(time.Second)
	for m.cfg.Resources.Cleanup.Used() != 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if n := backups.Load(); n != 0 {
		t.Fatalf("disabled failover still contacted backup %d time(s) during remote cleanup", n)
	}
}
