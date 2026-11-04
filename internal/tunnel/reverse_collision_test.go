package tunnel

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
)

// openHTTPTunnel registers an HTTP-carried tunnel through the normal open
// handler and fails the test unless it is accepted.
func openHTTPTunnel(t *testing.T, h *Hub, token, node, id, instance string) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, protocol.ReverseTunnelPath+"/"+id+"/open", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set(protocol.HeaderNodeID, node)
	r.Header.Set(protocol.HeaderExpectedNodeID, "hq-b")
	r.Header.Set(protocol.HeaderTunnelID, id)
	r.Header.Set(protocol.HeaderInstanceID, instance)
	r.Header.Set(protocol.HeaderTunnelPriority, "1")
	r.Header.Set(protocol.HeaderReverseProtocol, protocol.ReverseSubprotocol)
	r.Header.Set(protocol.HeaderTunnelTransport, "http-sse")
	w := httptest.NewRecorder()
	h.Serve(w, r, nil)
	if w.Code/100 != 2 {
		t.Fatalf("open %s for %s: %d %s", id, node, w.Code, w.Body.String())
	}
}

func lookupAs(h *Hub, token, id, instance string) *handshakeError {
	r := httptest.NewRequest(http.MethodGet, protocol.ReverseTunnelPath+"/"+id+"/events", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set(protocol.HeaderInstanceID, instance)
	_, herr := h.lookup(r, id, nil)
	return herr
}

// A-01: a second identity registering the same tunnel ID must not make the
// owner's HTTP tunnel requests fail, whatever order the peers are visited in.
func TestReverseTunnelIDCollisionAcrossIdentities(t *testing.T) {
	const evilToken = "evil-test-token-0123456789abcdef"
	cfg := hubConfig(t)
	t.Setenv("DBB_HUB_TEST_EVIL", evilToken)
	cfg.Peers = append(cfg.Peers, config.PeerConfig{ID: "evil", Mode: config.PeerModeReverse, ExpectedNodeID: "evil-a", CredentialRef: "evil", TokenEnv: "DBB_HUB_TEST_EVIL"})
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	tokens := &tokenTable{m: map[string]string{"plant": hubToken, "evil": evilToken}}
	h := NewHub(cfg, tokens.get)
	defer h.Close()

	id := newTunnelID()
	openHTTPTunnel(t, h, hubToken, "plant-a", id, "pi_plant")
	openHTTPTunnel(t, h, evilToken, "evil-a", id, "pi_evil")

	for i := 0; i < 512; i++ {
		if herr := lookupAs(h, hubToken, id, "pi_plant"); herr != nil {
			t.Fatalf("lookup %d: owner refused (%d %s)", i, herr.status, herr.code)
		}
		if herr := lookupAs(h, evilToken, id, "pi_evil"); herr != nil {
			t.Fatalf("lookup %d: second identity refused (%d %s)", i, herr.status, herr.code)
		}
	}
	// Neither identity can reach the other's tunnel with its own instance.
	if herr := lookupAs(h, evilToken, id, "pi_plant"); herr == nil || herr.status != 404 {
		t.Fatalf("cross-identity instance: %+v", herr)
	}
	if herr := lookupAs(h, "wrong-token-0123456789abcdef0000", id, "pi_plant"); herr == nil || herr.status != 401 {
		t.Fatalf("unknown token: %+v", herr)
	}
	if herr := lookupAs(h, hubToken, newTunnelID(), "pi_plant"); herr == nil || herr.status != 404 {
		t.Fatalf("unknown id: %+v", herr)
	}
}

// Peers sharing a token are told apart by the instance that opened the
// tunnel.
func TestReverseTunnelIDCollisionSharedToken(t *testing.T) {
	cfg := hubConfig(t)
	cfg.Peers = append(cfg.Peers, config.PeerConfig{ID: "zplant", Mode: config.PeerModeReverse, ExpectedNodeID: "plant-z", CredentialRef: "zplant", TokenEnv: "DBB_HUB_TEST_TOKEN"})
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	tokens := &tokenTable{m: map[string]string{"plant": hubToken, "zplant": hubToken}}
	h := NewHub(cfg, tokens.get)
	defer h.Close()

	id := newTunnelID()
	openHTTPTunnel(t, h, hubToken, "plant-a", id, "pi_a")
	openHTTPTunnel(t, h, hubToken, "plant-z", id, "pi_z")
	for i := 0; i < 512; i++ {
		for _, inst := range []string{"pi_a", "pi_z"} {
			if herr := lookupAs(h, hubToken, id, inst); herr != nil {
				t.Fatalf("lookup %d %s: %d %s", i, inst, herr.status, herr.code)
			}
		}
	}
}
