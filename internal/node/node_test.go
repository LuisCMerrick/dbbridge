package node

import (
	"context"
	"dbbridge/internal/config"
	"dbbridge/internal/session"
	"errors"
	"net"
	"testing"
	"time"
)

func TestStopDrainsBeforeForceClose(t *testing.T) {
	cfg := &config.Config{Node: config.NodeConfig{ID: "drain-test"}, HTTP: config.HTTPConfig{Enabled: true, Listen: "127.0.0.1:0"}, Transport: config.TransportConfig{ShutdownTimeoutRaw: "100ms"}}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	n := NewNode(cfg)
	if err := n.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	backend, peer := net.Pipe()
	defer peer.Close()
	s := session.NewSession("a", "db", cfg.Node.InstanceID, backend, 1024, 1024, 8, 64, false)
	n.registry.Register(s)
	done := make(chan struct{})
	start := time.Now()
	go func() { n.Stop(); close(done) }()
	until := time.Now().Add(time.Second)
	for n.registry.Accepting() && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if n.registry.Accepting() {
		t.Fatal("new session admission did not stop")
	}
	if s.GetState() == session.StateClosed {
		t.Fatal("session closed before drain budget")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop exceeded bounded drain")
	}
	if time.Since(start) < 90*time.Millisecond {
		t.Fatal("active session did not receive drain interval")
	}
	if s.GetState() != session.StateClosed {
		t.Fatal("force close did not finalize session")
	}
	peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("backend socket stayed open")
	}
}

// A stopped node refuses to start again instead of half-starting with a
// closed authenticator and a cancelled egress server.
func TestStartAfterStopFails(t *testing.T) {
	cfg := &config.Config{Node: config.NodeConfig{ID: "restart-test"}, HTTP: config.HTTPConfig{Enabled: true, Listen: "127.0.0.1:0"}, Transport: config.TransportConfig{ShutdownTimeoutRaw: "50ms"}}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	n := NewNode(cfg)
	if err := n.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	n.Stop()
	if err := n.Start(context.Background()); !errors.Is(err, ErrNodeSpent) {
		t.Fatalf("restart after Stop: %v", err)
	}
}
