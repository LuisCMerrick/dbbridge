package tunnel

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"

	"github.com/xtaci/smux"
)

func TestAuditReverseSharedTokenUnambiguous(t *testing.T) {
	cfg := hubConfig(t)
	cfg.Peers = append(cfg.Peers, config.PeerConfig{ID: "zplant", Mode: config.PeerModeReverse, ExpectedNodeID: "plant-z", CredentialRef: "zplant", TokenEnv: "DBB_HUB_TEST_TOKEN"})
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	h := NewHub(cfg, nil)
	defer h.Close()
	r := httptest.NewRequest(http.MethodGet, protocol.ReverseTunnelPath, nil)
	r.Header.Set("Authorization", "Bearer "+hubToken)
	r.Header.Set(protocol.HeaderNodeID, "plant-a")
	r.Header.Set(protocol.HeaderExpectedNodeID, "hq-b")
	r.Header.Set(protocol.HeaderInstanceID, "pi_audit")
	r.Header.Set(protocol.HeaderTunnelID, newTunnelID())
	r.Header.Set(protocol.HeaderTunnelPriority, "1")
	q, err := h.verifyIdentity(r)
	if err != nil {
		t.Fatalf("validated multi-peer config rejects plant-a's valid handshake; token selected peer %s: %v", q.peer.id, err)
	}
}

func TestAuditReverseRotationFencesPendingRegistration(t *testing.T) {
	cfg := hubConfig(t)
	tokens := &tokenTable{m: map[string]string{"plant": hubToken}}
	h := NewHub(cfg, tokens.get)
	defer h.Close()
	r := httptest.NewRequest(http.MethodGet, protocol.ReverseTunnelPath, nil)
	r.Header.Set("Authorization", "Bearer "+hubToken)
	r.Header.Set(protocol.HeaderNodeID, "plant-a")
	r.Header.Set(protocol.HeaderExpectedNodeID, "hq-b")
	r.Header.Set(protocol.HeaderInstanceID, "pi_audit")
	r.Header.Set(protocol.HeaderTunnelID, newTunnelID())
	r.Header.Set(protocol.HeaderTunnelPriority, "1")
	q, herr := h.verifyIdentity(r)
	if herr != nil {
		t.Fatal(herr)
	}
	// Force the authorized handshake/credential-reload interleaving.
	tokens.set("plant", "rotated-audit-token-0123456789abcdef")
	h.Revalidate()
	a, b := net.Pipe()
	ss, err := smux.Client(a, smuxConfig(h.tcfg))
	if err != nil {
		t.Fatal(err)
	}
	other, err := smux.Server(b, smuxConfig(h.tcfg))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	tn := &Tunnel{ID: q.id, Peer: q.peer.id, Instance: q.instance, digest: q.digest, sess: ss, conn: a}
	if err := h.register(q, tn); err == nil {
		t.Fatalf("a handshake authenticated before rotation registered afterward with the revoked digest; live=%d", h.Tunnels("plant"))
	}
}

func TestAuditReverseTunnelLeavesCodecForSessions(t *testing.T) {
	cfg := hubConfig(t)
	cfg.Transport.CodecBufferLimitRaw = "2MiB"
	cfg.Listeners = []config.ListenerConfig{{ID: "local", Listen: "127.0.0.1:0", Peer: "plant", Target: "db"}}
	// Rebuild resources after changing the validated configuration.
	cfg.Resources = nil
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	// Model an explicitly protected local ingress allocation.
	cfg.Resources.SetIngressCodecReserve(512 << 10)
	h := NewHub(cfg, nil)
	defer h.Close()
	r := httptest.NewRequest(http.MethodPost, protocol.ReverseTunnelPath+"/"+newTunnelID()+protocol.ReverseOpenSuffix, nil)
	id := r.URL.Path[len(protocol.ReverseTunnelPath)+1 : len(r.URL.Path)-len(protocol.ReverseOpenSuffix)]
	r.Header.Set("Authorization", "Bearer "+hubToken)
	r.Header.Set(protocol.HeaderNodeID, "plant-a")
	r.Header.Set(protocol.HeaderExpectedNodeID, "hq-b")
	r.Header.Set(protocol.HeaderInstanceID, "pi_audit")
	r.Header.Set(protocol.HeaderTunnelID, id)
	r.Header.Set(protocol.HeaderTunnelPriority, "1")
	r.Header.Set(protocol.HeaderReverseProtocol, protocol.ReverseSubprotocol)
	r.Header.Set(protocol.HeaderTunnelTransport, modePoll)
	w := httptest.NewRecorder()
	h.Serve(w, r, nil)
	if w.Code != 200 {
		t.Fatalf("open: %d %s", w.Code, w.Body.String())
	}
	need := httpTunnelBudget()
	if cfg.Resources.Codec.Available() < cfg.Resources.IngressCodecReserve() {
		t.Fatalf("one idle HTTP tunnel consumed the local data-plane reserve: need=%d used=%d available=%d ingress_reserve=%d", need, h.codec.Used(), h.codec.Available(), cfg.Resources.IngressCodecReserve())
	}
}

func TestAuditPipeRejectsOffsetOverflow(t *testing.T) {
	p := newPipe(32, Addr("a"), Addr("b"))
	_, err := p.receive(context.Background(), int64(1<<63-1), []byte("x"))
	if err == nil {
		t.Fatal("overflowing positive offset accepted as an empty retransmission")
	}
}

func TestAuditReverseControlProgressWithFullDataStreams(t *testing.T) {
	cfg := hubConfig(t)
	cfg.Transport.CodecBufferLimit = 2 << 20
	sc := smuxConfig(&cfg.Transport)
	sc.KeepAliveDisabled = true
	a, b := net.Pipe()
	tx, err := smux.Client(a, sc)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	rx, err := smux.Server(b, sc)
	if err != nil {
		t.Fatal(err)
	}
	defer rx.Close()
	ctrlOut, err := tx.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	ctrlIn, err := rx.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ctrlOut.Write([]byte("a")); err != nil {
		t.Fatal(err)
	}
	var marker [1]byte
	if _, err = io.ReadFull(ctrlIn, marker[:]); err != nil {
		t.Fatal(err)
	}
	// Four initially unconsumed data streams fill the shared 1MiB receive bucket.
	var stalled []*smux.Stream
	defer func() {
		for _, st := range stalled {
			_ = st.Close()
		}
	}()
	for i := 0; i < 4; i++ {
		s, err := tx.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		in, err := rx.AcceptStream()
		if err != nil {
			t.Fatal(err)
		}
		stalled = append(stalled, in)
		if _, err := s.Write(make([]byte, 256<<10)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	go func() { _, _ = ctrlOut.Write([]byte("b")) }()
	_ = ctrlIn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if _, err := io.ReadFull(ctrlIn, marker[:]); err != nil {
		t.Fatalf("separate control stream stalled behind unread data streams on the same healthy tunnel: %v", err)
	}
}
