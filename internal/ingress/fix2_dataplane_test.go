package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/egress"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
)

// fix2Hook may answer a request itself (returning true); base is the real
// egress handler.
type fix2Hook func(w http.ResponseWriter, r *http.Request, base http.Handler) bool

// fix2Setup is fixSetup with an egress tweak, an HTTP hook and DELETE
// counting (adapted from the pass-2 data-plane fixture).
func fix2Setup(t *testing.T, mode string, dial bool, tweak, btweak func(*config.Config), hook fix2Hook) (*fixEnv, *atomic.Int64) {
	t.Helper()
	e := &fixEnv{}
	deletes := &atomic.Int64{}
	ln := fixEcho(t)
	t.Setenv("FIX_TOKEN", "fix-token")
	b := &config.Config{Node: config.NodeConfig{ID: "B"}, HTTP: config.HTTPConfig{Enabled: true}, Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{{ID: "A", TokenEnvs: []string{"FIX_TOKEN"}, AllowedTargets: []string{"db"}, MaxSessions: 10}}}, Targets: map[string]config.TargetConfig{"db": {Address: ln.Addr().String()}}}
	if btweak != nil {
		btweak(b)
	}
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
		if r.Method == http.MethodDelete {
			deletes.Add(1)
		}
		if hook != nil && hook(w, r, base) {
			return
		}
		base.ServeHTTP(w, r)
	}))
	a := &config.Config{Node: config.NodeConfig{ID: "A"}, Peers: []config.PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "FIX_TOKEN", Endpoints: []config.EndpointConfig{{ID: "e1", URL: ts.URL}}}}, Listeners: []config.ListenerConfig{{ID: "local", Listen: "127.0.0.1:0", Peer: "B", Target: "db"}}, Transport: config.TransportConfig{Protocol: mode}}
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
	return e, deletes
}

type fix2Log struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *fix2Log) Write(p []byte) (int, error) { l.mu.Lock(); defer l.mu.Unlock(); return l.b.Write(p) }
func (l *fix2Log) String() string              { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

// captureLog redirects slog for the duration of the test.
func captureLog(t *testing.T) *fix2Log {
	l := &fix2Log{}
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(l, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return l
}

// fix2Session returns the only live ingress session.
func fix2Session(t *testing.T, m *IngressManager) *ingressSession {
	t.Helper()
	var s *ingressSession
	m.activeConns.Range(func(_, v any) bool { s = v.(*activeConnEntry).session; return s == nil })
	if s == nil {
		t.Fatal("no ingress session")
	}
	return s
}

// X-DP-2 / ING-3: a normal close must not be mistaken for a carrier failure:
// no recovery, no carrier cooldown, so the next connection is served at once.
func TestFix2NormalCloseDoesNotDelayNextSession(t *testing.T) {
	for _, mode := range []string{"ws", "http-sse"} {
		t.Run(mode, func(t *testing.T) {
			logs := captureLog(t)
			e, _ := fix2Setup(t, mode, false, nil, nil, nil)
			if err := e.m.Start(); err != nil {
				t.Fatal(err)
			}
			const n = 6
			for i := 0; i < n; i++ {
				c, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
				if err != nil {
					t.Fatal(err)
				}
				start := time.Now()
				c.SetDeadline(time.Now().Add(15 * time.Second))
				if err := fixEcho2(t, c, "hello"); err != nil {
					t.Fatal(err)
				}
				if d := time.Since(start); d > 2*time.Second {
					t.Fatalf("connection %d waited %v for its first echo after a normal close", i, d)
				}
				c.(*net.TCPConn).CloseWrite()
				if _, err := io.Copy(io.Discard, c); err != nil {
					t.Fatal(err)
				}
				c.Close()
			}
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) && strings.Count(logs.String(), "session terminated") < n {
				time.Sleep(20 * time.Millisecond)
			}
			if strings.Contains(logs.String(), "session recovered") {
				t.Fatalf("a normal close entered recovery:\n%s", logs.String())
			}
			ep := e.m.linkManagers["B"].GetEndpoints()[0]
			if !ep.TransportAvailable(time.Now(), mode) {
				t.Fatalf("normal closes cooled the %s carrier down", mode)
			}
		})
	}
}

// X-DP-3: small request/response round trips must not wait for the
// ack_max_delay timer (set to 2s here so a stall is unmistakable).
func TestFix2SmallFramesDoNotWaitForAckTimer(t *testing.T) {
	slow := func(c *config.Config) { c.Transport.AckMaxDelayRaw = "2s" }
	for _, mode := range []string{"ws", "http-sse"} {
		t.Run(mode, func(t *testing.T) {
			e, _ := fix2Setup(t, mode, true, slow, nil, nil)
			buf := make([]byte, 1)
			var worst time.Duration
			for i := 0; i < 300; i++ {
				t0 := time.Now()
				e.conn.SetDeadline(time.Now().Add(10 * time.Second))
				if _, err := e.conn.Write([]byte{byte(i)}); err != nil {
					t.Fatal(err)
				}
				if _, err := io.ReadFull(e.conn, buf); err != nil {
					t.Fatal(i, err)
				}
				worst = max(worst, time.Since(t0))
			}
			if worst > time.Second {
				t.Fatalf("%s round trip stalled %v waiting for the ACK timer", mode, worst)
			}
		})
	}
}

// X-DP-3 downstream: many small frames from the backend must be acknowledged
// by frame count, not only by bytes or the timer.
func TestFix2DownstreamSmallFramesAcked(t *testing.T) {
	slow := func(c *config.Config) { c.Transport.AckMaxDelayRaw = "2s" }
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				for i := 0; i < 400; i++ {
					c.Write([]byte{byte(i)})
					time.Sleep(100 * time.Microsecond)
				}
				io.Copy(io.Discard, c)
			}()
		}
	}()
	target := func(b *config.Config) {
		b.Targets = map[string]config.TargetConfig{"db": {Address: ln.Addr().String()}}
	}
	e, _ := fix2Setup(t, "ws", true, slow, target, nil)
	buf := make([]byte, 64)
	got := 0
	last := time.Now()
	var worst time.Duration
	for got < 400 {
		e.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		n, err := e.conn.Read(buf)
		if err != nil {
			t.Fatal(got, err)
		}
		if got > 0 {
			worst = max(worst, time.Since(last))
		}
		last = time.Now()
		got += n
	}
	if worst > time.Second {
		t.Fatalf("downstream small frames stalled %v waiting for the ACK timer", worst)
	}
}

// X-DP-4: a recovery that outlasts backpressure_timeout (but not the grace)
// must not be killed by the local backpressure clock. Egress keeps its own
// default backpressure_timeout (its side is SES-2, fixed separately).
func TestFix2RecoveryOutlastsBackpressureTimeout(t *testing.T) {
	var failUntil atomic.Int64
	hook := func(w http.ResponseWriter, r *http.Request, _ http.Handler) bool {
		if time.Now().UnixNano() < failUntil.Load() && (strings.HasSuffix(r.URL.Path, "/ack") || strings.HasSuffix(r.URL.Path, "/resume")) {
			w.WriteHeader(503)
			w.Write([]byte(`{"code":"DBB_X","message":"x","retryable":false,"session_closed":false}`))
			return true
		}
		return false
	}
	grace := func(c *config.Config) {
		c.Transport.Recovery.GraceRaw = "6s"
		c.Transport.Recovery.BarrierTimeoutRaw = "1s"
	}
	tweak := func(c *config.Config) { grace(c); c.Transport.BackpressureTimeoutRaw = "1s" }
	for _, mode := range []string{"ws", "http-sse"} {
		t.Run(mode, func(t *testing.T) {
			e, _ := fix2Setup(t, mode, true, tweak, grace, hook)
			e.conn.SetDeadline(time.Now().Add(15 * time.Second))
			if err := fixEcho2(t, e.conn, "a"); err != nil {
				t.Fatal(err)
			}
			failUntil.Store(time.Now().Add(2 * time.Second).UnixNano())
			time.Sleep(3 * time.Second)
			if err := fixEcho2(t, e.conn, "b"); err != nil {
				t.Fatalf("session did not survive a 2s outage (grace 6s, backpressure_timeout 1s): %v", err)
			}
		})
	}
}

// ING-5: a trusted, non-retryable refusal of a resume ends the session at
// once instead of retrying until the recovery grace (30s) expires. With
// forced ws and one endpoint, the dropped WS carrier first waits out its
// failure cooldown (DBB-AUD-06), so "at once" is measured from the refused
// resume request.
func TestFix2TrustedResumeRefusalStopsAtOnce(t *testing.T) {
	var resumedAt atomic.Int64
	hook := func(w http.ResponseWriter, r *http.Request, base http.Handler) bool {
		if !strings.HasSuffix(r.URL.Path, "/resume") {
			return false
		}
		resumedAt.CompareAndSwap(0, time.Now().UnixNano())
		rec := httptest.NewRecorder()
		base.ServeHTTP(rec, r)
		w.Header().Set(protocol.HeaderInstanceID, rec.Header().Get(protocol.HeaderInstanceID))
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: protocol.ErrTransportUnsupported})
		return true
	}
	e, _ := fix2Setup(t, "ws", true, nil, nil, hook)
	e.conn.SetDeadline(time.Now().Add(15 * time.Second))
	if err := fixEcho2(t, e.conn, "x"); err != nil {
		t.Fatal(err)
	}
	s := fix2Session(t, e.m)
	s.mu.Lock()
	g := s.g
	s.mu.Unlock()
	start := time.Now()
	g.ws.Close()
	if _, err := e.conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the local connection to close")
	}
	closed := time.Now()
	at := resumedAt.Load()
	if at == 0 {
		t.Fatal("no resume was sent")
	}
	if d := time.Since(start); d < 4*time.Second {
		t.Fatalf("the dropped WS carrier was reused after %v, inside its 5s cooldown", d)
	}
	if d := closed.Sub(time.Unix(0, at)); d > 2*time.Second {
		t.Fatalf("trusted resume refusal closed the client after %v", d)
	}
}

// ING-1: with a black-holed primary, concurrent sessions share one probe
// bounded by healthcheck_timeout, and the next session goes straight to the
// healthy backup.
func TestFix2BlackholedPrimaryDoesNotStallCreate(t *testing.T) {
	t.Setenv("FIX_TOKEN", "fix-token")
	backup, _ := fixEgress(t, fixEcho(t).Addr().String())
	bh, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var held sync.Map
	go func() {
		for {
			c, e := bh.Accept()
			if e != nil {
				return
			}
			held.Store(c, true)
		}
	}()
	t.Cleanup(func() {
		bh.Close()
		held.Range(func(k, _ any) bool { k.(net.Conn).Close(); return true })
	})
	a := &config.Config{Node: config.NodeConfig{ID: "A"}, Peers: []config.PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "FIX_TOKEN", Failover: config.FailoverConfig{HealthcheckTimeoutRaw: "300ms"}, Endpoints: []config.EndpointConfig{{ID: "primary", URL: "http://" + bh.Addr().String(), Priority: 1}, {ID: "backup", URL: backup.URL, Priority: 2}}}}, Listeners: []config.ListenerConfig{{ID: "local", Listen: "127.0.0.1:0", Peer: "B", Target: "db"}}, Transport: config.TransportConfig{Protocol: "http-sse"}}
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
	echo := func() (time.Duration, error) {
		c, err := net.Dial("tcp", m.listeners[0].Addr().String())
		if err != nil {
			return 0, err
		}
		defer c.Close()
		start := time.Now()
		c.SetDeadline(time.Now().Add(15 * time.Second))
		err = fixEcho2(t, c, "hi")
		return time.Since(start), err
	}
	var wg sync.WaitGroup
	results := make(chan time.Duration, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := echo()
			if err != nil {
				t.Error(err)
			}
			results <- d
		}()
	}
	wg.Wait()
	close(results)
	for d := range results {
		if d > 2*time.Second {
			t.Fatalf("session behind a black-holed primary waited %v (want about healthcheck_timeout)", d)
		}
	}
	d, err := echo()
	if err != nil || d > 250*time.Millisecond {
		t.Fatalf("next session after a failed primary probe: %v err=%v (should go straight to the backup)", d, err)
	}
}

// DBB-AUD-06 (replaces ING-4's "cooldown ignored without alternative"): a
// cooled-down carrier is not reselected while its cooldown runs. With no
// alternative the selection waits boundedly: it fails at the caller's
// deadline when that comes first, and picks the carrier once the cooldown
// has ended otherwise (requirements 3.2(4,8), 3.8, 10.2.1).
func TestFix2TransportCooldownWaitsWithoutAlternative(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/probe" {
			auditProbeResponse(w, r, true)
			return
		}
		w.WriteHeader(404)
	}))
	defer ts.Close()
	m, p := auditChooser(t, ts.URL)
	ep := m.linkManagers[p.ID].GetEndpoints()[0]
	m.linkManagers[p.ID].RecordTransportFailure(ep.ID, "http-sse")
	if ep.TransportAvailable(time.Now(), "http-sse") {
		t.Fatal("fixture: SSE should be cooling down")
	}
	// Deadline before the (default 5s) cooldown ends: no selection, and the
	// call returns at the deadline instead of hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	start := time.Now()
	_, _, mode, err := m.choose(ctx, p, "", "http-sse", true)
	cancel()
	if err == nil || mode != "" {
		t.Fatalf("cooled SSE carrier was reselected during its cooldown: mode=%q", mode)
	}
	if d := time.Since(start); d < 250*time.Millisecond || d > 2*time.Second {
		t.Fatalf("selection should wait until the deadline, returned after %v", d)
	}
	// Cooldown about to end: the selection waits for it and then succeeds.
	m.linkManagers[p.ID].GetEndpoints()[0].SSECooldownUntil = time.Now().Add(400 * time.Millisecond)
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start = time.Now()
	_, _, mode, err = m.choose(ctx, p, "", "http-sse", true)
	if err != nil || mode != "http-sse" {
		t.Fatalf("selection after the cooldown ended: mode=%q err=%v", mode, err)
	}
	if d := time.Since(start); d < 350*time.Millisecond || d > 2*time.Second {
		t.Fatalf("selection did not wait for the cooldown to end: %v", d)
	}
}
