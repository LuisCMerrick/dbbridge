package ingress

import (
	"bytes"
	"context"
	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/egress"
	"dbbridge/internal/observability"
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

type fixedFixture struct {
	manager   *IngressManager
	registry  *session.Registry
	conn      net.Conn
	dials     atomic.Int64
	creates   atomic.Int64
	ack       chan time.Time
	mu        sync.Mutex
	protocols map[string]int
}

func newFixedFixture(t *testing.T, mode string, tls, dropCreate, ackThreshold bool) *fixedFixture {
	t.Helper()
	f := &fixedFixture{ack: make(chan time.Time, 8), protocols: map[string]int{}}
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
			f.dials.Add(1)
			sockets.Store(c, true)
			go func() { defer sockets.Delete(c); defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	t.Setenv("DBB_TEST_TOKEN", "test-credential")
	b := &config.Config{Node: config.NodeConfig{ID: "B"}, HTTP: config.HTTPConfig{Enabled: true}, Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{{ID: "A", TokenEnvs: []string{"DBB_TEST_TOKEN"}, AllowedTargets: []string{"db"}, MaxSessions: 10}}}, Targets: map[string]config.TargetConfig{"db": {Network: "tcp", Address: backend.Addr().String()}}, Transport: config.TransportConfig{HeartbeatRaw: "100ms", FailureDetectTimeoutRaw: "1s", Recovery: config.RecoveryConfig{EnabledRaw: false}}}
	b.Transport.ChunkSizeRaw = "7KiB"
	if err = b.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	ba := auth.NewAuthenticator(b)
	f.registry = session.NewRegistry(b.Transport.SuspendedBufferLimit, b.Transport.GlobalBufferLimit)
	srv := egress.NewServer(b, ba, f.registry)
	base := srv.Handler()
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kind := "control"
		switch {
		case r.URL.Path == "/v1/peer/probe":
			kind = "probe"
		case r.URL.Path == "/v1/transport/ws":
			kind = "ws"
		case strings.HasSuffix(r.URL.Path, "/events"):
			kind = "events"
		case strings.HasSuffix(r.URL.Path, "/up"):
			kind = "up"
		}
		f.mu.Lock()
		f.protocols[kind] = r.ProtoMajor
		f.mu.Unlock()
		if r.URL.Path == "/v1/sessions" && r.Method == "POST" {
			if f.creates.Add(1) == 1 && dropCreate {
				rec := httptest.NewRecorder()
				base.ServeHTTP(rec, r)
				if rec.Code != http.StatusCreated {
					t.Errorf("fixture create: %d %s", rec.Code, rec.Body.String())
				}
				f.registry.SetDraining()
				c, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					c.Close()
				}
				return
			}
		}
		if strings.HasSuffix(r.URL.Path, "/ack") {
			body, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(body))
			var q protocol.AckRequest
			_ = json.Unmarshal(body, &q)
			base.ServeHTTP(w, r)
			if q.DownDeliveredOffset == "4" {
				select {
				case f.ack <- time.Now():
				default:
				}
			}
			return
		}
		base.ServeHTTP(w, r)
	}))
	if tls {
		ts.EnableHTTP2 = true
		ts.StartTLS()
	} else {
		ts.Start()
	}
	// The self-signed certificate bypass is explicit and confined to this fixture;
	// TestWSSAfterHTTP2Probe separately verifies a trusted certificate path.
	a := &config.Config{Node: config.NodeConfig{ID: "A"}, Peers: []config.PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "DBB_TEST_TOKEN", TLS: config.TLSClientConfig{Verify: config.BoolPtr(!tls)}, Endpoints: []config.EndpointConfig{{ID: "primary", URL: ts.URL}}}}, Listeners: []config.ListenerConfig{{ID: "local", Listen: "127.0.0.1:0", Peer: "B", Target: "db"}}, Transport: config.TransportConfig{Protocol: mode, HeartbeatRaw: "100ms", FailureDetectTimeoutRaw: "1s", ControlRequestTimeoutRaw: "500ms", CreateRetryTimeoutRaw: "2s", Recovery: config.RecoveryConfig{EnabledRaw: false}}}
	if ackThreshold {
		a.Transport.AckBatchBytesRaw = "4B"
		a.Transport.AckMaxDelayRaw = "500ms"
	}
	if err = a.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	aa := auth.NewAuthenticator(a)
	f.manager = NewIngressManager(a, aa)
	if err = f.manager.Start(); err != nil {
		t.Fatal(err)
	}
	f.conn, err = net.Dial("tcp", f.manager.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		f.conn.Close()
		f.manager.Close()
		srv.Close()
		ts.Close()
		aa.Close()
		ba.Close()
		backend.Close()
		sockets.Range(func(k, v any) bool { k.(net.Conn).Close(); return true })
	})
	return f
}

func TestTLSDataAndControlProtocols(t *testing.T) {
	for _, mode := range []string{"ws", "http-sse"} {
		t.Run(mode, func(t *testing.T) {
			beforeACK, beforePOST := atomic.LoadInt64(&observability.DefaultMetrics.ExplicitAckTotal), atomic.LoadInt64(&observability.DefaultMetrics.PostCountTotal)
			f := newFixedFixture(t, mode, true, false, false)
			payload := bytes.Repeat([]byte{0, 255, 31, 9, 128}, 16000)
			f.conn.SetDeadline(time.Now().Add(3 * time.Second))
			done := make(chan error, 1)
			go func() { _, e := f.conn.Write(payload); done <- e }()
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(f.conn, got); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("TLS roundtrip: %v", err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			observed := make(map[string]int)
			for k, v := range f.protocols {
				observed[k] = v
			}
			f.mu.Unlock()
			if observed["probe"] != 2 || observed["control"] != 2 {
				t.Fatalf("control/probe protocols: %v", observed)
			}
			if mode == "ws" && observed["ws"] != 1 || mode == "http-sse" && (observed["events"] != 2 || observed["up"] != 2) {
				t.Fatalf("data protocols: %v", observed)
			}
			// Wait up to 2s for the explicit ACK to arrive on busy CI runners.
			until := time.Now().Add(2 * time.Second)
			for atomic.LoadInt64(&observability.DefaultMetrics.ExplicitAckTotal) <= beforeACK && time.Now().Before(until) {
				time.Sleep(10 * time.Millisecond)
			}
			if atomic.LoadInt64(&observability.DefaultMetrics.ExplicitAckTotal) <= beforeACK {
				t.Fatal("ACK metric did not update")
			}
			if mode == "http-sse" && atomic.LoadInt64(&observability.DefaultMetrics.PostCountTotal) <= beforePOST {
				t.Fatal("POST metric did not update")
			}
			if f.dials.Load() != 1 {
				t.Fatalf("backend connections=%d", f.dials.Load())
			}
			var metrics bytes.Buffer
			f.manager.WriteMetrics(&metrics)
			if !strings.Contains(metrics.String(), `phase="attach",success="true"`) || !strings.Contains(metrics.String(), `endpoint="primary"`) || strings.Contains(metrics.String(), "test-credential") {
				t.Fatalf("missing bounded phase/binding metrics or leaked credential: %s", metrics.String())
			}
			t.Logf("actual HTTP versions: %v; verified %d bytes each direction", observed, len(payload))
		})
	}
}

func TestLostCreateResponseCanBeRetrievedDuringDrain(t *testing.T) {
	f := newFixedFixture(t, "auto", false, true, false)
	f.conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := f.conn.Write([]byte("same session")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("same session"))
	if _, err := io.ReadFull(f.conn, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "same session" || f.dials.Load() != 1 || f.creates.Load() < 2 || f.registry.Accepting() {
		t.Fatalf("result=%q dials=%d creates=%d accepting=%v", got, f.dials.Load(), f.creates.Load(), f.registry.Accepting())
	}
}

func TestByteThresholdSchedulesAckBeforeTimer(t *testing.T) {
	f := newFixedFixture(t, "http-sse", false, false, true)
	f.conn.SetDeadline(time.Now().Add(3 * time.Second))
	start := time.Now()
	if _, err := f.conn.Write([]byte("four")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(f.conn, make([]byte, 4)); err != nil {
		t.Fatal(err)
	}
	select {
	case at := <-f.ack:
		if at.Sub(start) >= 300*time.Millisecond {
			t.Fatalf("ACK threshold waited %s for 500ms timer", at.Sub(start))
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("byte threshold did not wake control")
	}
}

func TestAdmittedButNotStartedConnectionCannotEscapeClose(t *testing.T) {
	m, p := testChooser(t, "http://127.0.0.1:1")
	local, client := net.Pipe()
	defer client.Close()
	ctx, cancel := context.WithCancel(m.ctx)
	id := "accepted-before-shutdown"
	m.mu.Lock()
	m.wg.Add(1)
	m.activeConns.Store(id, &activeConnEntry{cancel: cancel, closeFn: func() { local.Close() }})
	m.mu.Unlock()
	stopped := make(chan struct{})
	go func() { m.Close(); close(stopped) }()
	<-m.ctx.Done()
	go func() {
		defer m.wg.Done()
		m.handleConn(ctx, cancel, local, config.ListenerConfig{Peer: p.ID, Target: "db"}, id, time.Now())
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("shutdown missed an admitted connection")
	}
	if m.Count() != 0 {
		t.Fatal("late worker re-registered after shutdown")
	}
}
