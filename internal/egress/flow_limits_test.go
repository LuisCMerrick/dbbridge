package egress

import (
	"bytes"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func flowCreate(t *testing.T, url, id, target string, offer bool) (int, []byte) {
	t.Helper()
	b, _ := json.Marshal(protocol.CreateSessionRequest{Limits: protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 2048, DownBufferBytes: 4096}, ProtocolVersion: "1.4", IngressInstanceID: "ai", CreateRequestID: id, TargetID: target, Transport: protocol.TransportMode{Mode: "http-sse"}})
	r, _ := http.NewRequest("POST", url+"/v1/sessions", bytes.NewReader(b))
	r.Header.Set("Authorization", "Bearer tokA")
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	if offer {
		r.Header.Set(protocol.HeaderFeatures, protocol.FeatureFlowLimits)
	}
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// X-DP-3 protocol: flow limits are advertised only to an ingress that offered
// the extension; an older ingress (strict decoder) gets plain 1.4 JSON.
func TestFlowLimitsAdvertisedOnlyWhenOffered(t *testing.T) {
	cfg := fixCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 4}})
	cfg.Transport.ReceiptLimitPerSession = 40
	s, ts := egrServer(t, cfg)

	st, raw := flowCreate(t, ts.URL, "plain", "db", false)
	if st != 201 {
		t.Fatalf("plain create: %d %s", st, raw)
	}
	if strings.Contains(string(raw), "up_receipt_limit") || strings.Contains(string(raw), "down_max_frames") {
		t.Fatalf("extension fields sent to a peer that did not offer them: %s", raw)
	}
	var plain protocol.CreateSessionResponse
	_ = json.Unmarshal(raw, &plain)
	if sess := s.registry.Get(plain.SessionID); sess == nil || sess.FlowHints() {
		t.Fatal("plain session must not send ack_requested")
	}

	st, raw = flowCreate(t, ts.URL, "flow", "db", true)
	if st != 201 {
		t.Fatalf("flow create: %d %s", st, raw)
	}
	var cr protocol.CreateSessionResponse
	if err := protocol.DecodeJSON(bytes.NewReader(raw), 32<<10, &cr); err != nil {
		t.Fatal(err)
	}
	if cr.Limits.UpReceiptLimit != cfg.Transport.ReceiptLimitPerSession || cr.Limits.DownMaxFrames != 512 {
		t.Fatalf("advertised limits %+v, want receipt=%d frames=512", cr.Limits, cfg.Transport.ReceiptLimitPerSession)
	}
	if sess := s.registry.Get(cr.SessionID); sess == nil || !sess.FlowHints() {
		t.Fatal("flow session must enable ack_requested hints")
	}
	// An idempotent replay returns the same advertised limits.
	st, raw2 := flowCreate(t, ts.URL, "flow", "db", true)
	if st != 200 || !bytes.Equal(bytes.TrimSpace(raw), bytes.TrimSpace(raw2)) {
		t.Fatalf("replay: %d %s", st, raw2)
	}
}
