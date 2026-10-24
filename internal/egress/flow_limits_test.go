package egress

import (
	"bytes"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

func flowCreate(t *testing.T, url, id, target string) (int, []byte) {
	t.Helper()
	b, _ := json.Marshal(protocol.CreateSessionRequest{Limits: protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 2048, DownBufferBytes: 4096}, ProtocolVersion: "1.5", IngressInstanceID: "ai", CreateRequestID: id, TargetID: target, Transport: protocol.TransportMode{Mode: "http-sse"}})
	r, _ := http.NewRequest("POST", url+"/v1/sessions", bytes.NewReader(b))
	r.Header.Set("Authorization", "Bearer tokA")
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// Every create response advertises the egress flow limits, and an
// idempotent replay returns the same values.
func TestFlowLimitsAdvertised(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 4}})
	cfg.Transport.ReceiptLimitPerSession = 40
	_, ts := egrServer(t, cfg)

	st, raw := flowCreate(t, ts.URL, "flow", "db")
	if st != 201 {
		t.Fatalf("create: %d %s", st, raw)
	}
	var cr protocol.CreateSessionResponse
	if err := protocol.DecodeJSON(bytes.NewReader(raw), 32<<10, &cr); err != nil {
		t.Fatal(err)
	}
	if cr.Limits.UpReceiptLimit != cfg.Transport.ReceiptLimitPerSession || cr.Limits.DownMaxFrames != 512 {
		t.Fatalf("advertised limits %+v, want receipt=%d frames=512", cr.Limits, cfg.Transport.ReceiptLimitPerSession)
	}
	st, raw2 := flowCreate(t, ts.URL, "flow", "db")
	if st != 200 || !bytes.Equal(bytes.TrimSpace(raw), bytes.TrimSpace(raw2)) {
		t.Fatalf("replay: %d %s", st, raw2)
	}
}
