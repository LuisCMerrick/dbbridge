package egress

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"dbbridge/internal/tunnel"
)

func TestAuditReverseRequestsRespectAdmission(t *testing.T) {
	const tok = "audit-reverse-token-0123456789abcdef"
	t.Setenv("DBB_AUDIT_TOKEN", tok)
	cfg := &config.Config{Node: config.NodeConfig{ID: "hq"}, HTTP: config.HTTPConfig{Enabled: true}, Peers: []config.PeerConfig{{ID: "plant", Mode: config.PeerModeReverse, ExpectedNodeID: "plant", CredentialRef: "plant", TokenEnv: "DBB_AUDIT_TOKEN"}}}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	a := auth.NewAuthenticator(cfg)
	defer a.Close()
	h := tunnel.NewHub(cfg, a.GetOutboundToken)
	defer h.Close()
	s := NewServer(cfg, a, session.NewRegistry(cfg.Transport.SuspendedBufferLimit, cfg.Transport.GlobalBufferLimit))
	s.SetReverseHub(h)
	defer s.Close()
	id := "tn_00000000000000000000000000000001"
	r := httptest.NewRequest(http.MethodPost, protocol.ReverseTunnelPath+"/"+id+protocol.ReverseOpenSuffix, nil)
	r.Header.Set("Authorization", "Bearer "+tok)
	r.Header.Set(protocol.HeaderNodeID, "plant")
	r.Header.Set(protocol.HeaderExpectedNodeID, "hq")
	r.Header.Set(protocol.HeaderInstanceID, "pi_audit")
	r.Header.Set(protocol.HeaderTunnelID, id)
	r.Header.Set(protocol.HeaderTunnelPriority, "1")
	r.Header.Set(protocol.HeaderReverseProtocol, protocol.ReverseSubprotocol)
	r.Header.Set(protocol.HeaderTunnelTransport, "long-poll")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("open: %d %s", w.Code, w.Body.String())
	}
	// Hold every configured data request slot before another upload arrives.
	b := cfg.Resources.DataRequests
	if !b.Try(b.Limit()) {
		t.Fatal("cannot occupy request budget")
	}
	held := true
	defer func() {
		if held {
			b.Put(b.Limit())
		}
	}()
	upload := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, protocol.ReverseTunnelPath+"/"+id+protocol.ReverseUpSuffix, nil)
		r.Header.Set("Authorization", "Bearer "+tok)
		r.Header.Set(protocol.HeaderInstanceID, "pi_audit")
		r.Header.Set(protocol.HeaderOffset, "0")
		sum := sha256.Sum256(nil)
		r.Header.Set(protocol.HeaderSHA256, hex.EncodeToString(sum[:]))
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w
	}
	defer func() {
		if t.Failed() {
			return
		}
		// Once the slots are free again the upload is admitted, and every
		// budget it took is back afterwards.
		b.Put(b.Limit())
		held = false
		meta, codec := cfg.Resources.Metadata.Used(), cfg.Resources.Codec.Used()
		if w := upload(); w.Code != 200 {
			t.Fatalf("upload after release: %d %s", w.Code, w.Body.String())
		}
		if b.Used() != 0 || cfg.Resources.Metadata.Used() != meta || cfg.Resources.Codec.Used() != codec {
			t.Fatalf("budgets not restored: requests=%d metadata %d->%d codec %d->%d", b.Used(), meta, cfg.Resources.Metadata.Used(), codec, cfg.Resources.Codec.Used())
		}
	}()
	r = httptest.NewRequest(http.MethodPost, protocol.ReverseTunnelPath+"/"+id+protocol.ReverseUpSuffix, nil)
	r.Header.Set("Authorization", "Bearer "+tok)
	r.Header.Set(protocol.HeaderInstanceID, "pi_audit")
	r.Header.Set(protocol.HeaderOffset, "0")
	sum := sha256.Sum256(nil)
	r.Header.Set(protocol.HeaderSHA256, hex.EncodeToString(sum[:]))
	w = httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code == 200 {
		t.Fatalf("reverse upload admitted while all data request slots are held: HTTP %d, used=%d limit=%d", w.Code, b.Used(), b.Limit())
	}
}
