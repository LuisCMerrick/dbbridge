package egress

import (
	"context"
	"crypto/sha256"
	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestContinuousDownstreamMustYieldToControls(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	go func() { _, _ = io.Copy(io.Discard, remote) }()
	sess := session.NewSession("A", "db", "bi", local, 1<<20, 1<<20, 8, 64, true)
	defer sess.Close()
	sess.SetTransportMode("ws")
	stream, err := sess.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if err = sess.ActivateDataPlane(stream); err != nil {
		t.Fatal(err)
	}
	if _, err = sess.AppendDownBytes([]byte("x")); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Node: config.NodeConfig{ID: "B"}}
	if err = cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	cfg.Transport.Heartbeat = 10 * time.Millisecond
	cfg.Transport.UpAckMaxDelay = time.Millisecond
	srv := &Server{cfg: cfg}
	frames, pings := 0, 0
	ackOne := false
	start := time.Now()
	err = srv.streamWriter(context.Background(), sess, 1, stream, func(kind string, data []byte, v any, end uint64) error {
		if kind == "ping" {
			pings++
		}
		if kind == "ack_up" && v.(protocol.WSTextControlMessage).AckUpOffset == "1" {
			ackOne = true
		}
		if kind != "data" {
			return nil
		}
		frames++
		if frames == 1 {
			p := []byte("u")
			sum := sha256.Sum256(p)
			if _, _, _, e := sess.IngestUpFrame(1, 0, 1, hex.EncodeToString(sum[:]), p); e != nil {
				return e
			}
			until := time.Now().Add(time.Second)
			for sess.UpAckOffset() != 1 && time.Now().Before(until) {
				time.Sleep(time.Millisecond)
			}
			if sess.UpAckOffset() != 1 {
				return errors.New("fixture backend did not acknowledge")
			}
		}
		if err := sess.AdvanceDownAck(end, false); err != nil {
			return err
		}
		time.Sleep(10 * time.Millisecond)
		if frames >= 20 {
			return io.ErrClosedPipe
		}
		_, err := sess.AppendDownBytes([]byte("x"))
		return err
	})
	t.Logf("continuous_down_frames=%d elapsed=%s ping_count=%d ack_up_1_emitted=%t exit=%v", frames, time.Since(start), pings, ackOne, err)
	if !ackOne || pings < 2 {
		t.Fatal("continuous downstream loop starved frame-boundary ACK/ping scheduling")
	}
}

func TestSinglePeerWSPrechecksMustBeBounded(t *testing.T) {
	t.Setenv("DBB_TEST_PRECHECK_TOKEN", "isolated-precheck-token")
	cfg := &config.Config{Node: config.NodeConfig{ID: "B"}, Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{{ID: "A", TokenEnvs: []string{"DBB_TEST_PRECHECK_TOKEN"}, AllowedTargets: []string{"db"}}}}, Targets: map[string]config.TargetConfig{"db": {Address: "127.0.0.1:3306"}}}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	a := auth.NewAuthenticator(cfg)
	defer a.Close()
	r := session.NewRegistry(1<<20, 4<<20)
	srv := NewServer(cfg, a, r)
	defer srv.Close()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	var sockets []*websocket.Conn
	defer func() {
		for _, c := range sockets {
			c.Close()
		}
	}()
	for i := 0; i < 3; i++ {
		h := http.Header{"Authorization": []string{"Bearer isolated-precheck-token"}, protocol.HeaderInstanceID: []string{cfg.Node.InstanceID}, protocol.HeaderWSPurpose: []string{"check"}, protocol.HeaderWSNonce: []string{"nonce-for-precheck"}}
		d := websocket.Dialer{Subprotocols: []string{protocol.SecWebSocketProtocol}, HandshakeTimeout: time.Second}
		c, response, err := d.Dial("ws"+ts.URL[len("http"):]+"/v1/transport/ws", h)
		if err != nil {
			if i != 2 || response == nil || response.StatusCode != http.StatusTooManyRequests {
				t.Fatalf("unexpected rejection for precheck %d: %v", i+1, err)
			}
			defer response.Body.Close()
			var refusal protocol.ErrorResponse
			if err = json.NewDecoder(response.Body).Decode(&refusal); err != nil || refusal.Code != protocol.ErrQuotaExceeded || !refusal.Retryable {
				t.Fatalf("unsafe quota refusal: %+v error=%v", refusal, err)
			}
			if response.Header.Get("Retry-After") != "1" || cfg.Resources.Probes.Used() > 2 {
				t.Fatal("quota rejection leaked admission or omitted retry budget")
			}
			return
		}
		sockets = append(sockets, c)
	}
	t.Fatalf("same inbound identity owns %d concurrent WS prechecks; default per-peer max_concurrent_probes is 2; global used=%d", len(sockets), cfg.Resources.Probes.Used())
}
