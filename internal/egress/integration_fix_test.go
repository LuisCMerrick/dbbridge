package egress

import (
	"net"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"dbbridge/internal/session"
)

// intReaderSession builds a recovery-enabled, attached session whose global
// payload budget is exhausted, so the backend reader's read reservation stalls.
func intReaderSession(t *testing.T) (*session.Session, net.Conn, *resource.Budget, int64) {
	t.Helper()
	reg := session.NewRegistry(1<<20, 1<<22)
	payload := resource.New(64 << 10)
	reg.SetResources(payload, nil, 0)
	a, b := net.Pipe()
	t.Cleanup(func() { b.Close() })
	sess := session.NewSession("A", "db", "bi", a, 4096, 2048, 8, 64, true)
	sess.Configure(10*time.Second, time.Second, "ai", nil)
	id, err := sess.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if err = sess.ActivateDataPlane(id); err != nil {
		t.Fatal(err)
	}
	reg.Register(sess)
	t.Cleanup(reg.CloseAll)
	held := payload.Available()
	if !payload.Try(held) {
		t.Fatal("could not exhaust payload")
	}
	return sess, b, payload, held
}

func intReaderServer() *Server {
	return &Server{cfg: &config.Config{Transport: config.TransportConfig{BackpressureTimeout: 60 * time.Millisecond, FrameSize: 1024}}}
}

// SES-2 (egress half): while the session is suspended, the backend reader's
// reservation stall timer re-arms instead of closing the session with
// BUFFER_EXHAUSTED before the recovery grace ends.
func TestIntegrationSES2ReaderStallWaitsDuringRecovery(t *testing.T) {
	sess, backend, payload, held := intReaderSession(t)
	sess.DropTransport(1)
	if !sess.Recovering() {
		t.Fatal("session not recovering after DropTransport")
	}
	s := intReaderServer()
	done := make(chan struct{})
	go func() { defer close(done); s.runBackendReader(sess, make(chan struct{}, 1)) }()
	time.Sleep(400 * time.Millisecond)
	if sess.GetState() == session.StateClosed {
		t.Fatalf("suspended session closed by the reader stall timer: %s", sess.TerminalCode())
	}
	payload.Put(held)
	go func() { _, _ = backend.Write([]byte("hello")) }()
	deadline := time.Now().Add(3 * time.Second)
	for sess.DownProducedOffset() < 5 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := sess.DownProducedOffset(); got < 5 {
		t.Fatalf("reader did not resume after the budget freed: produced %d", got)
	}
	sess.Close()
	backend.Close()
	<-done
}

// SES-2 counterpart: an attached session still ends with BUFFER_EXHAUSTED
// once the reservation stall exceeds backpressure_timeout.
func TestIntegrationSES2ReaderStallClosesAttached(t *testing.T) {
	sess, _, _, _ := intReaderSession(t)
	s := intReaderServer()
	done := make(chan struct{})
	go func() { defer close(done); s.runBackendReader(sess, make(chan struct{}, 1)) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("attached session not closed by the stall timer")
	}
	if code := sess.TerminalCode(); code != protocol.ErrBufferExhausted {
		t.Fatalf("terminal code %q", code)
	}
}

// X-SEC-1: accepted sockets are charged to the Manager's dedicated
// ConnMetadata budget, never to the session Metadata pool, and the charge is
// returned when the socket closes.
func TestIntegrationXSEC1SocketsUseConnMetadata(t *testing.T) {
	cfg := fixCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	s, _ := egrServer(t, cfg)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	meta := cfg.Resources.Metadata.Used()
	c, err := net.Dial("tcp", s.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for cfg.Resources.ConnMetadata.Used() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := cfg.Resources.ConnMetadata.Used(); got != resource.ConnMetadataPerConn {
		t.Fatalf("ConnMetadata used %d, want %d", got, resource.ConnMetadataPerConn)
	}
	if got := cfg.Resources.Metadata.Used(); got != meta {
		t.Fatalf("socket admission charged session Metadata: %d -> %d", meta, got)
	}
	c.Close()
	for cfg.Resources.ConnMetadata.Used() != 0 && time.Now().Before(deadline.Add(3*time.Second)) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := cfg.Resources.ConnMetadata.Used(); got != 0 {
		t.Fatalf("ConnMetadata not released: %d", got)
	}
}

// X-SEC-5: the egress server applies the shared ingress reserve on a
// bidirectional node (one mechanism, idempotent with config) and none on an
// egress-only node.
func TestIntegrationXSEC5ReserveIsResourceManagers(t *testing.T) {
	cfg := fixCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 8}})
	egrServer(t, cfg)
	if s, m := cfg.Resources.IngressReserve(); s != 0 || m != 0 {
		t.Fatalf("egress-only node reserved %d sessions / %d metadata", s, m)
	}
	bi := fixCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 8}})
	bi.Listeners = []config.ListenerConfig{{Peer: "P", Target: "db"}}
	bi.Resources.SetIngressReserve(3, 4096) // set earlier by config: kept
	egrServer(t, bi)
	if s, m := bi.Resources.IngressReserve(); s != 3 || m != 4096 {
		t.Fatalf("egress replaced the configured reserve: %d/%d", s, m)
	}
	bi.Resources.SetIngressReserve(0, 0)
	bi.ApplyIngressReserve()
	limit := bi.Resources.Sessions.Limit()
	if s, m := bi.Resources.IngressReserve(); s != max(1, limit/4) || m != bi.Resources.Metadata.Limit()/8 {
		t.Fatalf("default reserve %d/%d", s, m)
	}
}
