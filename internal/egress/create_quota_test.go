package egress

import (
	"bytes"
	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// egrServer serves NewServer(cfg) through httptest (admission middleware
// included, no listener guard).
func egrServer(t *testing.T, cfg *config.Config) (*Server, *httptest.Server) {
	t.Helper()
	a := auth.NewAuthenticator(cfg)
	t.Cleanup(a.Close)
	reg := session.NewRegistry(1<<20, 1<<22)
	s := NewServer(cfg, a, reg)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(func() { ts.Close(); s.Close() })
	return s, ts
}

func egrCreate(t *testing.T, url, token, id, target string) (int, protocol.ErrorResponse, protocol.CreateSessionResponse) {
	t.Helper()
	b, _ := json.Marshal(protocol.CreateSessionRequest{Limits: protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 2048, DownBufferBytes: 4096}, ProtocolVersion: "1.5", IngressInstanceID: "ai", CreateRequestID: id, TargetID: target, Transport: protocol.TransportMode{Mode: "http-sse"}})
	r, _ := http.NewRequest("POST", url+"/v1/sessions", bytes.NewReader(b))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	var raw json.RawMessage
	_ = json.NewDecoder(resp.Body).Decode(&raw)
	var er protocol.ErrorResponse
	var cr protocol.CreateSessionResponse
	_ = json.Unmarshal(raw, &er)
	_ = json.Unmarshal(raw, &cr)
	return resp.StatusCode, er, cr
}

func egrDelete(t *testing.T, url, token string, cr protocol.CreateSessionResponse) int {
	t.Helper()
	r, _ := http.NewRequest("DELETE", url+"/v1/sessions/"+cr.SessionID+"?epoch=1", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	r.Header.Set(protocol.HeaderSessionToken, cr.SessionToken)
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// deadAddr is a loopback address no socket can listen on: TCP port 0 is
// never bound by a listener, so the dial is refused at once. A freed
// ephemeral port could be reused by another listener in the meantime.
func deadAddr(t *testing.T) string {
	t.Helper()
	return "127.0.0.1:0"
}

// Creates for unknown or forbidden targets are refused before the
// idempotency table is touched, so they retain no metadata and other peers
// keep creating.
func TestRefusedCreatesKeepNoRecord(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{
		{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 4},
		{ID: "X", Tokens: []string{"tokX"}, AllowedTargets: []string{"db"}, MaxSessions: 1},
	})
	_, ts := egrServer(t, cfg)
	for i := 0; i < 300; i++ {
		target := "forbidden"
		if i%2 == 1 {
			target = "missing"
		}
		if st, _, _ := egrCreate(t, ts.URL, "tokX", fmt.Sprintf("spam-%d", i), target); st != 403 {
			t.Fatalf("refused create %d: %d", i, st)
		}
	}
	if used := cfg.Resources.Metadata.Used(); used != 0 {
		t.Fatalf("refused creates retained %d bytes of metadata", used)
	}
	if st, er, _ := egrCreate(t, ts.URL, "tokA", "victim", "db"); st != 201 {
		t.Fatalf("victim create: %d %+v", st, er)
	}
}

// Dial failures on a permitted target are cached (502), but one peer
// may keep only a bounded number of such records; beyond it new keys get a
// retryable 429 without a record, replays still see their 502, and other
// peers are unaffected.
func TestPeerIdempotencyQuota(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{
		{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 4},
		{ID: "X", Tokens: []string{"tokX"}, AllowedTargets: []string{"dead"}, MaxSessions: 1},
	})
	cfg.Targets["dead"] = config.TargetConfig{Network: "tcp", Address: deadAddr(t)}
	s, ts := egrServer(t, cfg)
	limit := peerIdemLimit(1)
	for i := 0; i < limit; i++ {
		if st, er, _ := egrCreate(t, ts.URL, "tokX", fmt.Sprintf("d-%d", i), "dead"); st != 502 {
			t.Fatalf("dial failure %d: %d %+v", i, st, er)
		}
	}
	if n := s.idem.count("X"); n != limit {
		t.Fatalf("tracked %d records, want %d", n, limit)
	}
	used := cfg.Resources.Metadata.Used()
	st, er, _ := egrCreate(t, ts.URL, "tokX", "d-over", "dead")
	if st != 429 || !er.Retryable || er.Message != "peer idempotency quota" {
		t.Fatalf("over quota: %d %+v", st, er)
	}
	if cfg.Resources.Metadata.Used() != used {
		t.Fatal("refused create charged metadata")
	}
	if st, _, _ := egrCreate(t, ts.URL, "tokX", "d-0", "dead"); st != 502 {
		t.Fatalf("replay of a cached failure: %d, want 502", st)
	}
	if st, er, _ := egrCreate(t, ts.URL, "tokA", "a1", "db"); st != 201 {
		t.Fatalf("other peer: %d %+v", st, er)
	}
}

// A transient refusal (peer quota) is not cached; the same
// create_request_id succeeds once a slot frees.
func TestRetryableCreateNotCached(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 1}})
	_, ts := egrServer(t, cfg)
	st, _, first := egrCreate(t, ts.URL, "tokA", "c1", "db")
	if st != 201 {
		t.Fatalf("first create %d", st)
	}
	st, er, _ := egrCreate(t, ts.URL, "tokA", "victim", "db")
	if st != 429 || !er.Retryable {
		t.Fatalf("over peer quota: %d %+v", st, er)
	}
	if st := egrDelete(t, ts.URL, "tokA", first); st != 200 {
		t.Fatalf("delete %d", st)
	}
	if st, er, _ := egrCreate(t, ts.URL, "tokA", "victim", "db"); st != 201 {
		t.Fatalf("retry with the same create_request_id after the slot freed: %d %+v", st, er)
	}
	// A completed create is still replayed from the table.
	if st, _, _ := egrCreate(t, ts.URL, "tokA", "victim", "db"); st != 200 {
		t.Fatalf("replay of a completed create: %d", st)
	}
}

// On a node that also runs local ingress listeners,
// remote peers cannot take every shared session slot.
func TestLocalIngressReserve(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 8}})
	cfg.Listeners = []config.ListenerConfig{{Peer: "P", Target: "db"}}
	_, ts := egrServer(t, cfg)
	limit := cfg.Resources.Sessions.Limit()
	allowed := limit - max(1, limit/4)
	for i := int64(0); i < allowed; i++ {
		if st, er, _ := egrCreate(t, ts.URL, "tokA", fmt.Sprintf("s%d", i), "db"); st != 201 {
			t.Fatalf("create %d: %d %+v", i, st, er)
		}
	}
	if st, _, _ := egrCreate(t, ts.URL, "tokA", "over", "db"); st != 429 {
		t.Fatalf("egress took the local ingress reserve: %d", st)
	}
	if free := limit - cfg.Resources.Sessions.Used(); free < 1 || !cfg.Resources.Sessions.Try(1) {
		t.Fatalf("no session slot left for local ingress (free %d)", free)
	}
	cfg.Resources.Sessions.Put(1)
}
