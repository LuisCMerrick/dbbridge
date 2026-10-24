package egress

import (
	"bytes"
	"context"
	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMaximumValidTokenCannotUseProbe(t *testing.T) {
	token := strings.Repeat("x", 8<<10)
	t.Setenv("DBB_TEST_MAX_TOKEN", token)
	cfg := &config.Config{Node: config.NodeConfig{ID: "B"}, Targets: map[string]config.TargetConfig{"db": {Address: "127.0.0.1:3306"}}, Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{{ID: "A", TokenEnvs: []string{"DBB_TEST_MAX_TOKEN"}, AllowedTargets: []string{"db"}}}}}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatalf("configuration refused token: %v", err)
	}
	a := auth.NewAuthenticator(cfg)
	defer a.Close()
	r := session.NewRegistry(cfg.Transport.SuspendedBufferLimit, cfg.Transport.GlobalBufferLimit)
	srv := NewServer(cfg, a, r)
	defer srv.Close()
	request := httptest.NewRequest("POST", "/v1/peer/probe", bytes.NewBufferString(`{"protocol_version":"1.5","expected_node_id":"B","nonce":"nonce"}`))
	request.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, request)
	t.Logf("check-config accepted token_bytes=%d; probe HTTP=%d body=%s", len(token), w.Code, w.Body.String())
	if w.Code != 200 {
		t.Error("configured valid maximum credential cannot pass the HTTP header budget")
	}
	for _, size := range []int{8<<10 + 1, 16 << 10} {
		t.Setenv("DBB_TEST_MAX_TOKEN", strings.Repeat("x", size))
		if err := cfg.ApplyDefaultsAndValidate(); err == nil {
			t.Errorf("accepted oversized token_bytes=%d", size)
		}
	}
}

// Idle source readers must leave enough codec capacity
// for another session's data to make progress.
func TestIdleReadersStarveCodec(t *testing.T) {
	cfg := &config.Config{Node: config.NodeConfig{ID: "B"}, Transport: config.TransportConfig{
		FrameSizeRaw: 32768, MaxFrameSizeRaw: 32768, CodecBufferLimitRaw: "256KiB",
		HeartbeatRaw: "20ms", FailureDetectTimeoutRaw: "3s",
	}}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	r := session.NewRegistry(cfg.Transport.SuspendedBufferLimit, cfg.Transport.GlobalBufferLimit)
	r.SetResources(cfg.Resources.Payload, cfg.Resources.Metadata, cfg.Transport.IdempotencyTombstoneTTL)
	srv := &Server{cfg: cfg, registry: r}
	var sessions []*session.Session
	var peers []net.Conn
	var readers sync.WaitGroup
	defer func() {
		for _, s := range sessions {
			s.Close()
		}
		for _, p := range peers {
			p.Close()
		}
		readers.Wait()
		r.CloseAll()
	}()
	for i := 0; i < 8; i++ {
		local, peer := net.Pipe()
		s := session.NewSession("A", "db", "bi", local, cfg.Transport.BufferPerDirection, cfg.Transport.UpWindowBytes, 8, 64, false)
		s.SetTransportMode("ws")
		s.SetNegotiatedLimits(protocol.SessionLimits{MaxFrameSize: 32768, UpWindowBytes: 2 << 20, UpMaxInflightFrames: 8})
		stream, err := s.ReserveStream(1, "ws")
		if err != nil {
			t.Fatal(err)
		}
		if err = s.ActivateDataPlane(stream); err != nil {
			t.Fatal(err)
		}
		r.Register(s)
		sessions = append(sessions, s)
		peers = append(peers, peer)
		readers.Add(1)
		go func() { defer readers.Done(); srv.runBackendReader(s, make(chan struct{}, 1)) }()
	}
	until := time.Now().Add(time.Second)
	for cfg.Resources.Payload.Used() != 8*32768 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if cfg.Resources.Payload.Used() != 8*32768 {
		t.Fatal("fixture did not start all eight reserved backend reads")
	}
	if _, err := peers[0].Write([]byte("database-hello")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.streamWriter(ctx, sessions[0], 1, sessions[0].CurrentStreamID(), func(kind string, data []byte, _ any, _ uint64) error {
			if kind == "data" {
				got <- string(data)
				cancel()
			}
			return nil
		})
	}()
	defer func() { cancel(); <-done }()
	select {
	case <-got:
		return
	case <-time.After(150 * time.Millisecond):
		t.Logf("8 idle readers: codec=%d/%d, produced=%d, data not emitted", cfg.Resources.Codec.Used(), cfg.Resources.Codec.Limit(), sessions[0].DownProducedOffset())
	}
	// Free an unrelated idle reader: the same data should immediately progress.
	sessions[7].Close()
	select {
	case b := <-got:
		t.Errorf("valid codec configuration deadlocked data until unrelated session closed; then delivered %q", b)
	case <-time.After(time.Second):
		t.Fatal("data still blocked after unrelated session released")
	}
}
