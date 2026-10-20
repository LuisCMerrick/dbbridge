package egress

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
)

type rotationRequest struct {
	method, path, body string
	hdr                map[string]string
}

// rotationRequests lists one request of each kind a live session issues:
// control (ack), POST data (up), resume and WS attach.
func rotationRequests(url string, c protocol.CreateSessionResponse) []rotationRequest {
	base := url + "/v1/sessions/" + c.SessionID
	st := map[string]string{protocol.HeaderSessionToken: c.SessionToken, protocol.HeaderInstanceID: c.InstanceID}
	ws := map[string]string{protocol.HeaderSessionToken: c.SessionToken, protocol.HeaderInstanceID: c.InstanceID, protocol.HeaderWSSessionID: c.SessionID}
	return []rotationRequest{
		{"POST", base + "/ack?epoch=1", `{"down_delivered_offset":"0","up_confirmed_offset":"0","backpressure_reason":"none"}`, st},
		{"POST", base + "/up?epoch=1", ``, st},
		{"POST", base + "/resume", `{}`, st},
		{"GET", url + "/v1/transport/ws", ``, ws},
	}
}

func doAuth(t *testing.T, q rotationRequest, token string) int {
	t.Helper()
	r, _ := http.NewRequest(q.method, q.path, strings.NewReader(q.body))
	if q.body == "" {
		r.Body, r.ContentLength = http.NoBody, 0
	}
	r.Header.Set("Authorization", "Bearer "+token)
	for k, v := range q.hdr {
		r.Header.Set(k, v)
	}
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func waitReload(t *testing.T, a *auth.Authenticator, rev uint64) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for a.Revision() == rev && time.Now().Before(until) {
		time.Sleep(5 * time.Millisecond)
	}
	if a.Revision() == rev {
		t.Fatal("reload did not happen")
	}
}

// Disabling or deleting the identity, or removing the Target from its ACL,
// still terminates its live sessions on reload.
func TestAuthReloadRevocationClosesSessions(t *testing.T) {
	for name, peer := range map[string]string{
		"disabled":    "- id: A\n  enabled: false\n  token_envs: [AUTHZ_TOK]\n  allowed_targets: [db]\n  max_sessions: 5\n",
		"removed":     "- id: Z\n  token_envs: [AUTHZ_TOK_Z]\n  allowed_targets: [db]\n  max_sessions: 5\n",
		"acl_tighten": "- id: A\n  token_envs: [AUTHZ_TOK]\n  allowed_targets: [other]\n  max_sessions: 5\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("AUTHZ_TOK", "authz-token-00000001")
			t.Setenv("AUTHZ_TOK_Z", "authz-token-zzzzzzz1")
			p := filepath.Join(t.TempDir(), "auth.yaml")
			write := func(peers string) {
				t.Helper()
				if e := os.WriteFile(p+".tmp", []byte("version: 1\ninbound_peers:\n"+peers), 0600); e != nil {
					t.Fatal(e)
				}
				if e := os.Rename(p+".tmp", p); e != nil {
					t.Fatal(e)
				}
			}
			write("- id: A\n  token_envs: [AUTHZ_TOK]\n  allowed_targets: [db]\n  max_sessions: 5\n")
			cfg := testCfg(t, nil)
			cfg.Targets["other"] = cfg.Targets["db"]
			cfg.Auth = config.AuthConfig{CredentialsFile: p, ReloadInterval: 20 * time.Millisecond}
			a := auth.NewAuthenticator(cfg)
			defer a.Close()
			if e := a.Err(); e != nil {
				t.Fatal(e)
			}
			reg := session.NewRegistry(1<<20, 1<<22)
			defer reg.CloseAll()
			srv := NewServer(cfg, a, reg)
			defer srv.Close()
			ts := httptest.NewServer(srv.Handler())
			defer ts.Close()
			r, c := testCreate(t, ts.URL, "authz-token-00000001", "c1", "ws")
			if r.StatusCode != 201 {
				t.Fatalf("create %d", r.StatusCode)
			}
			rev := a.Revision()
			write(peer)
			waitReload(t, a, rev)
			until := time.Now().Add(2 * time.Second)
			for time.Now().Before(until) {
				if s := reg.GetAny(c.SessionID); s == nil || s.GetState() == session.StateClosed {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Fatalf("%s: session still live after reload", name)
		})
	}
}
