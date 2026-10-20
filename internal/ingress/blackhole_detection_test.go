package ingress

import (
	"bytes"
	"context"
	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/egress"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"dbbridge/internal/session"
	"dbbridge/internal/transport"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type independentReviewEnv struct {
	m                                     *IngressManager
	reg                                   *session.Registry
	c                                     net.Conn
	dials, resumes, dropped, downMessages atomic.Int64
	shortBudgetAt                         chan time.Time
}

func independentReview(t *testing.T, kind string) *independentReviewEnv {
	t.Helper()
	e := &independentReviewEnv{shortBudgetAt: make(chan time.Time, 1)}
	recovery := kind != "drop_up_no_recovery"
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
			go func() { defer sockets.Delete(c); defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	t.Setenv("DBB_TEST_TOKEN", "test-token")
	b := &config.Config{Node: config.NodeConfig{ID: "B"}, HTTP: config.HTTPConfig{Enabled: true},
		Auth:      config.AuthConfig{InboundPeers: []config.InboundPeerConfig{{ID: "A", TokenEnvs: []string{"DBB_TEST_TOKEN"}, AllowedTargets: []string{"db"}, MaxSessions: 10}}},
		Targets:   map[string]config.TargetConfig{"db": {Address: backend.Addr().String()}},
		Transport: config.TransportConfig{HeartbeatRaw: "30ms", FailureDetectTimeoutRaw: "150ms", Recovery: config.RecoveryConfig{EnabledRaw: recovery, GraceRaw: "1200ms", BarrierTimeoutRaw: "100ms"}}}
	if err := b.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	ba := auth.NewAuthenticator(b)
	e.reg = session.NewRegistry(b.Transport.SuspendedBufferLimit, b.Transport.GlobalBufferLimit)
	srv := egress.NewServer(b, ba, e.reg)
	base := srv.Handler()
	direct := httptest.NewServer(base)
	var malformed atomic.Bool
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/resume") {
			e.resumes.Add(1)
			if kind == "short_budget" {
				rec := httptest.NewRecorder()
				base.ServeHTTP(rec, r)
				if rec.Code != 200 {
					t.Errorf("resume fixture status: %d", rec.Code)
					w.WriteHeader(rec.Code)
					return
				}
				var result protocol.ResumeResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
					t.Error(err)
					return
				}
				result.ResumeRemainingMs = 10
				for key, values := range rec.Header() {
					w.Header()[key] = values
				}
				_ = json.NewEncoder(w).Encode(result)
				w.(http.Flusher).Flush()
				select {
				case e.shortBudgetAt <- time.Now():
				default:
				}
				return
			}
		}
		// The resumed carrier never becomes ready, whichever transport the
		// recovery picks.
		if kind == "short_budget" && r.URL.Path == "/v1/transport/ws" && r.Header.Get(protocol.HeaderWSPurpose) == "session" && r.Header.Get(protocol.HeaderWSEpoch) != "1" {
			select {
			case <-r.Context().Done():
			case <-time.After(500 * time.Millisecond):
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if kind == "short_budget" && strings.HasSuffix(r.URL.Path, "/events") && r.URL.Query().Get("epoch") != "1" {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		if r.URL.Path == "/v1/transport/ws" && r.Header.Get(protocol.HeaderWSPurpose) == "session" {
			if kind == "malformed_ready" && !malformed.Swap(true) {
				sess := e.reg.Get(r.Header.Get(protocol.HeaderWSSessionID))
				if sess == nil {
					t.Error("missing fixture session")
					return
				}
				if _, err := sess.ReserveStream(1, "ws"); err != nil {
					t.Error(err)
					return
				}
				upgrader := websocket.Upgrader{Subprotocols: []string{protocol.SecWebSocketProtocol}}
				c, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer c.Close()
				defer sess.DropTransport(1)
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"ready","type":"ready"}`))
				return
			}
			if strings.HasPrefix(kind, "drop_up") {
				h := http.Header{}
				for key, values := range r.Header {
					if key == "Authorization" || strings.HasPrefix(key, "X-Dbb-") {
						h[key] = values
					}
				}
				dialer := websocket.Dialer{Subprotocols: []string{protocol.SecWebSocketProtocol}, HandshakeTimeout: time.Second}
				up, _, err := dialer.Dial("ws"+strings.TrimPrefix(direct.URL, "http")+"/v1/transport/ws", h)
				if err != nil {
					t.Error(err)
					return
				}
				defer up.Close()
				upgrader := websocket.Upgrader{Subprotocols: []string{protocol.SecWebSocketProtocol}}
				down, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer down.Close()
				go func() {
					defer down.Close()
					for {
						typ, p, err := up.ReadMessage()
						if err != nil {
							return
						}
						e.downMessages.Add(1)
						if down.WriteMessage(typ, p) != nil {
							return
						}
					}
				}()
				for {
					typ, _, err := down.ReadMessage()
					if err != nil {
						return
					}
					if typ == websocket.BinaryMessage {
						e.dropped.Add(1)
					}
				}
			}
		}
		base.ServeHTTP(w, r)
	}))
	a := &config.Config{Node: config.NodeConfig{ID: "A"},
		Peers:     []config.PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "DBB_TEST_TOKEN", Endpoints: []config.EndpointConfig{{ID: "primary", URL: gateway.URL}}}},
		Listeners: []config.ListenerConfig{{ID: "local", Listen: "127.0.0.1:0", Peer: "B", Target: "db"}},
		Transport: config.TransportConfig{Protocol: "auto", HeartbeatRaw: "30ms", FailureDetectTimeoutRaw: "150ms", AckMaxDelayRaw: "20ms", ControlRequestTimeoutRaw: "250ms", BackpressureTimeoutRaw: "3s", Recovery: config.RecoveryConfig{EnabledRaw: recovery, GraceRaw: "1200ms", BarrierTimeoutRaw: "100ms"}}}
	if err := a.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	aa := auth.NewAuthenticator(a)
	e.m = NewIngressManager(a, aa)
	if err := e.m.Start(); err != nil {
		t.Fatal(err)
	}
	e.c, err = net.Dial("tcp", e.m.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		e.c.Close()
		e.m.Close()
		srv.Close()
		gateway.Close()
		direct.Close()
		aa.Close()
		ba.Close()
		backend.Close()
		sockets.Range(func(k, v any) bool { k.(net.Conn).Close(); return true })
	})
	return e
}

func TestMalformedInitialReadyMustTerminate(t *testing.T) {
	e := independentReview(t, "malformed_ready")
	_ = e.c.SetDeadline(time.Now().Add(3 * time.Second))
	p := []byte("must not survive an invalid protocol message")
	_, _ = e.c.Write(p)
	got := make([]byte, len(p))
	_, err := io.ReadFull(e.c, got)
	all := e.reg.GetAllSessions()
	epoch := uint64(0)
	mode := ""
	if len(all) > 0 {
		epoch = all[0].GetEpoch()
		mode = all[0].GetTransportMode()
	}
	t.Logf("read_error=%v exact_echo=%t resumes=%d backend_dials=%d epoch=%d mode=%s", err, bytes.Equal(got, p), e.resumes.Load(), e.dials.Load(), epoch, mode)
	if err == nil {
		t.Fatal("malformed duplicate-key ready was tolerated through recovery and carrier fallback")
	}
	// The backend's Accept goroutine may count the dial after the session
	// already failed; wait for it instead of racing it.
	for deadline := time.Now().Add(time.Second); e.dials.Load() == 0 && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	if e.resumes.Load() != 0 || e.dials.Load() != 1 {
		t.Fatal("protocol rejection must not recover or replace the database connection")
	}
}

func TestUpstreamWSBlackholeMustTriggerRecovery(t *testing.T) {
	e := independentReview(t, "drop_up")
	_, _ = e.c.Write([]byte("pending upstream frame"))
	deadline := time.Now().Add(time.Second)
	for e.dropped.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if e.dropped.Load() == 0 {
		t.Fatal("fixture did not drop any upstream frame")
	}
	// More than four detection intervals; control and downstream remain active.
	time.Sleep(650 * time.Millisecond)
	var pending uint64
	var controlAge time.Duration
	e.m.activeConns.Range(func(_, v any) bool {
		s := v.(*activeConnEntry).session
		if s != nil {
			s.mu.Lock()
			pending = s.upNext - s.upAck
			controlAge = time.Since(s.lastControl)
			s.mu.Unlock()
		}
		return true
	})
	all := e.reg.GetAllSessions()
	epoch := uint64(0)
	if len(all) > 0 {
		epoch = all[0].GetEpoch()
	}
	t.Logf("dropped_up_frames=%d downstream_messages=%d unacked_up_bytes=%d control_age=%s resume_calls=%d epoch=%d", e.dropped.Load(), e.downMessages.Load(), pending, controlAge, e.resumes.Load(), epoch)
	if e.resumes.Load() == 0 {
		t.Fatal("upstream data blackhole was not detected while independent controls and downstream pings stayed healthy")
	}
	if err := e.c.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("pending upstream frame"))
	if _, err := io.ReadFull(e.c, got); err != nil || string(got) != "pending upstream frame" || e.dials.Load() != 1 {
		t.Fatalf("recovery did not preserve original bytes/socket: %q dials=%d error=%v", got, e.dials.Load(), err)
	}
}

func TestResumeRemainingMustTightenDeadline(t *testing.T) {
	e := independentReview(t, "short_budget")
	_ = e.c.SetDeadline(time.Now().Add(3 * time.Second))
	_, _ = e.c.Write([]byte("before"))
	if _, err := io.ReadFull(e.c, make([]byte, 6)); err != nil {
		t.Fatal(err)
	}
	var current *ingressSession
	e.m.activeConns.Range(func(_, v any) bool { current = v.(*activeConnEntry).session; return false })
	if current == nil {
		t.Fatal("missing ingress fixture")
	}
	current.mu.Lock()
	old := current.g
	current.mu.Unlock()
	old.ws.Close()
	var received time.Time
	select {
	case received = <-e.shortBudgetAt:
	case <-time.After(time.Second):
		t.Fatal("no resume response")
	}
	select {
	case <-current.ctx.Done():
	case <-time.After(time.Until(received.Add(1010 * time.Millisecond))):
		t.Fatal("shortened budget did not close a blocked ready handshake within the terminal allowance")
	}
	current.mu.Lock()
	deadline := current.deadline
	epoch := current.epoch
	current.mu.Unlock()
	t.Logf("confirmed_epoch=%d peer_remaining=10ms local_deadline_after_response=%s", epoch, deadline.Sub(received))
	if epoch < 2 && !current.closed.Load() {
		t.Fatal("resume result has not been adopted")
	}
	if deadline.After(received.Add(100 * time.Millisecond)) {
		t.Fatal("peer's shorter recovery budget was ignored")
	}
}

func TestUpstreamBlackholeWithoutRecoveryMustClose(t *testing.T) {
	e := independentReview(t, "drop_up_no_recovery")
	_, _ = e.c.Write([]byte("unconfirmed bytes"))
	until := time.Now().Add(time.Second)
	for e.dropped.Load() == 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if e.dropped.Load() == 0 {
		t.Fatal("fixture did not drop an upstream frame")
	}
	// Include the detection interval and the full one-second terminal allowance.
	time.Sleep(1250 * time.Millisecond)
	_ = e.c.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
	_, err := e.c.Read(make([]byte, 1))
	t.Logf("recovery=false dropped_frames=%d down_messages=%d after_detection_and_close_allowance read_error=%v", e.dropped.Load(), e.downMessages.Load(), err)
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("client remained open beyond detection plus one-second closure allowance")
	}
}

func TestUpstreamDetectionAllowsHealthyWaiting(t *testing.T) {
	for _, kind := range []string{"idle", "queued", "backend-backpressure", "ack-progress"} {
		t.Run(kind, func(t *testing.T) {
			var calls atomic.Int64
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var q protocol.AckRequest
				_ = json.NewDecoder(r.Body).Decode(&q)
				n := calls.Add(1)
				ack := "0"
				if kind == "ack-progress" {
					ack = strconv.FormatInt(min(n, 8), 10)
				}
				_ = json.NewEncoder(w).Encode(protocol.AckResponse{
					SessionID: "sid", InstanceID: "bi", Epoch: "1", StreamID: "stream", Transport: &protocol.TransportMode{Mode: "ws"},
					AckUpOffset: ack, DownAckOffset: "0", ProbeID: q.ProbeID, DataRxBlocked: kind == "backend-backpressure",
				})
			}))
			defer ts.Close()
			cfg := &config.Config{Resources: resource.NewManager(1<<20, 1<<20, 1<<20, 10), Transport: config.TransportConfig{
				FailureDetectTimeout: 250 * time.Millisecond, BackpressureTimeout: 3 * time.Second,
				Heartbeat: 50 * time.Millisecond, AckMaxDelay: 50 * time.Millisecond, ControlRequestTimeout: time.Second, DialTimeout: time.Second,
			}}
			c := transport.NewTripleTransportClientForURL(ts.URL, "token", &cfg.Transport, "ai", true)
			defer c.Close()
			c.BindStream("sid", 1, "stream", "ws")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			g := &generation{ctx: ctx, cancel: cancel, c: c, ep: 1, mode: "ws", stream: "stream", active: true, lastActivity: time.Now(), sent: map[*retainedChunk]bool{}}
			s := &ingressSession{m: &IngressManager{cfg: cfg}, ctx: ctx, cancel: cancel, g: g, all: []*generation{g}, sid: "sid", owner: "bi",
				changed: make(chan struct{}), faults: make(chan sessionFault, 1), ackNotify: make(chan struct{}, 1)}
			if kind != "idle" {
				s.upNext = 8
				for i := uint64(0); i < 8; i++ {
					s.frames = append(s.frames, &retainedChunk{offset: i, data: []byte("x")})
				}
			}
			if kind == "backend-backpressure" || kind == "ack-progress" {
				g.upSentOffset, g.upPendingSince = 8, time.Now()
			}
			var wg sync.WaitGroup
			wg.Add(3)
			go func() { defer wg.Done(); s.control(g) }()
			go func() { defer wg.Done(); s.watchActivity(g) }()
			go func() {
				defer wg.Done()
				tick := time.NewTicker(10 * time.Millisecond)
				defer tick.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-tick.C:
						g.mu.Lock()
						g.lastActivity = time.Now()
						g.mu.Unlock()
					}
				}
			}()
			defer func() { cancel(); wg.Wait() }()
			select {
			case fault := <-s.faults:
				t.Fatalf("%s incorrectly classified as broken upstream: %v", kind, fault.err)
			case <-time.After(800 * time.Millisecond):
			}
			if calls.Load() < 8 {
				t.Fatal("control fixture made insufficient progress")
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if kind == "ack-progress" && (s.upAck != 8 || !g.upPendingSince.IsZero()) {
				t.Fatal("confirmed upstream bytes left an active stall timer")
			}
		})
	}
}

func TestPrecheckQuotaRemainsRetryableAndUncached(t *testing.T) {
	var checks atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/probe" {
			testProbeResponse(w, r, true)
			return
		}
		if checks.Add(1) == 1 {
			w.Header().Set(protocol.HeaderInstanceID, "bi")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			_ = json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: protocol.ErrQuotaExceeded, Retryable: true})
			return
		}
		up := websocket.Upgrader{Subprotocols: []string{protocol.SecWebSocketProtocol}}
		c, err := up.Upgrade(w, r, nil)
		if err == nil {
			defer c.Close()
			_ = c.WriteJSON(protocol.WSHelloMessage{Type: "hello", ProtocolVersion: "1.4", NodeID: "B", InstanceID: "bi", Nonce: r.Header.Get(protocol.HeaderWSNonce), Transport: "ws"})
		}
	}))
	defer ts.Close()
	m, p := testChooser(t, ts.URL)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, _, err := m.choose(ctx, p, "", "auto", true); !safeRetry(err) {
		t.Fatalf("temporary quota rejected as a fatal protocol error: %v", err)
	}
	if _, _, mode, err := m.choose(ctx, p, "", "auto", true); err != nil || mode != "ws" || checks.Load() != 2 {
		t.Fatalf("quota response cached or endpoint isolated: mode=%s checks=%d error=%v", mode, checks.Load(), err)
	}
}
