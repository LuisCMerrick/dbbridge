package egress

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"dbbridge/internal/auth"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
)

func TestTokenRotationMustPreserveIdentitySessions(t *testing.T) {
	cfg := testCfg(t, nil)
	dir := t.TempDir()
	cfg.ConfigFileDir = dir
	cfg.Auth.CredentialsFile = filepath.Join(dir, "auth.yaml")
	cfg.Auth.ReloadInterval = 10 * time.Millisecond
	for name, value := range map[string]string{"old.token": "old-test-token-123456789", "new.token": "new-test-token-123456789"} {
		if e := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); e != nil {
			t.Fatal(e)
		}
	}
	write := func(token string) {
		t.Helper()
		b := []byte("version: 1\ninbound_peers:\n  - id: A\n    enabled: true\n    token_files: [" + token + "]\n    allowed_targets: [db]\n    max_sessions: 2\n")
		if e := os.WriteFile(cfg.Auth.CredentialsFile+".tmp", b, 0600); e != nil {
			t.Fatal(e)
		}
		if e := os.Rename(cfg.Auth.CredentialsFile+".tmp", cfg.Auth.CredentialsFile); e != nil {
			t.Fatal(e)
		}
	}
	write("old.token")
	a := auth.NewAuthenticator(cfg)
	defer a.Close()
	if e := a.Err(); e != nil {
		t.Fatal(e)
	}
	r := session.NewRegistry(1<<20, 4<<20)
	srv := NewServer(cfg, a, r)
	defer srv.Close()
	c, b := net.Pipe()
	defer b.Close()
	s := session.NewSession("A", "db", "bi", c, 4096, 2048, 8, 64, true)
	r.Register(s)
	old := a.Revision()
	write("new.token")
	until := time.Now().Add(time.Second)
	for a.Revision() == old && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if a.Revision() == old {
		t.Fatal("fixture: reload did not happen")
	}
	// The callback runs just after publication of the new snapshot.
	time.Sleep(20 * time.Millisecond)
	if e := a.AuthorizeTarget("A", "db"); e != nil {
		t.Fatalf("fixture: identity/ACL changed: %v", e)
	}
	if s.GetState() == session.StateClosed {
		t.Fatalf("contract violation: rotating only the Peer Token closed the still-authorized identity's session; code=%s", s.TerminalCode())
	}
}

// A controlled backend returns n>0 together with a non-EOF error. This tests
// the production reader's error branch, without claiming a packet-level RST.
type testReadErrorConn struct {
	net.Conn
	once   sync.Once
	failed chan struct{}
}

func (c *testReadErrorConn) Read(p []byte) (int, error) {
	n := copy(p, []byte("tail"))
	c.once.Do(func() { close(c.failed) })
	return n, errors.New("injected fatal backend read error")
}

func TestFatalBackendReadMustFenceRecovery(t *testing.T) {
	cfg := testCfg(t, nil)
	cfg.Transport.FrameSize = 512
	cfg.Transport.SocketWriteTimeout = 2 * time.Second
	srv := &Server{cfg: cfg, ctx: context.Background()}
	c, b := net.Pipe()
	defer b.Close()
	backend := &testReadErrorConn{Conn: c, failed: make(chan struct{})}
	s := session.NewSession("A", "db", "bi", backend, 4096, 2048, 8, 64, true)
	s.Configure(2*time.Second, 2*time.Second, "ai", nil)
	s.SetTransportMode("ws")
	id, e := s.ReserveStream(1, "ws")
	if e != nil {
		t.Fatal(e)
	}
	if e = s.ActivateDataPlane(id); e != nil {
		t.Fatal(e)
	}
	done := make(chan struct{})
	go func() { defer close(done); srv.runBackendReader(s, make(chan struct{}, 1)) }()
	defer func() { s.Close(); <-done }()
	select {
	case <-backend.failed:
	case <-time.After(time.Second):
		t.Fatal("fixture: read did not fail")
	}
	until := time.Now().Add(time.Second)
	for s.DownProducedOffset() != 4 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if s.DownProducedOffset() != 4 {
		t.Fatal("fixture: tail bytes not recorded")
	}
	time.Sleep(20 * time.Millisecond)
	q := &protocol.ResumeRequest{ResumeRequestID: "test-credential", ExpectedEpoch: "1", IngressInstanceID: "ai", Transport: protocol.TransportMode{Mode: "http-sse"}, KnownUpAckOffset: "0", UpNextOffset: "0", DownDeliveredOffset: "0"}
	r, err := s.Resume(q, time.Second)
	if err == nil || s.GetState() != session.StateClosed {
		t.Fatalf("contract violation: after fatal backend error resume result=%+v error=%v state=%s", r, err, s.GetState())
	}
}
