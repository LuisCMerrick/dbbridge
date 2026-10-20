package auth

import (
	"dbbridge/internal/config"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAuditV2ReloadMissingRequiredOutboundRetainsSnapshot(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "auth.yaml")
	t.Setenv("AUDIT_V2_OUTBOUND", "valid-outbound-token")
	os.WriteFile(p, []byte("version: 1\noutbound_peers:\n- id: cred-b\n  token_env: AUDIT_V2_OUTBOUND\n"), 0600)
	cfg := &config.Config{Auth: config.AuthConfig{CredentialsFile: p, ReloadInterval: time.Hour}, Peers: []config.PeerConfig{{ID: "B", CredentialRef: "cred-b"}}}
	a := NewAuthenticator(cfg)
	defer a.Close()
	if got := a.GetOutboundToken("B"); got != "valid-outbound-token" {
		t.Fatalf("setup: %q", got)
	}
	os.WriteFile(p, []byte("version: 1\noutbound_peers: []\n"), 0600)
	a.loadCredentialsFile()
	if got := a.GetOutboundToken("B"); got != "valid-outbound-token" {
		t.Fatalf("incomplete candidate replaced last valid outbound snapshot: token length=%d", len(got))
	}
}

func TestV2ReloadUnknownACLRetainsSnapshot(t *testing.T) {
	p := filepath.Join(t.TempDir(), "auth.yaml")
	t.Setenv("DBB_V2_ACL_TOKEN", "acl-test-secret")
	write := func(target string) {
		t.Helper()
		if err := os.WriteFile(p, []byte("version: 1\ninbound_peers:\n- id: A\n  token_envs: [DBB_V2_ACL_TOKEN]\n  allowed_targets: ["+target+"]\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("db")
	a := NewAuthenticator(&config.Config{Auth: config.AuthConfig{CredentialsFile: p, ReloadInterval: time.Hour}, Targets: map[string]config.TargetConfig{"db": {Network: "tcp", Address: "127.0.0.1:3306"}}})
	defer a.Close()
	if a.Err() != nil {
		t.Fatal(a.Err())
	}
	write("missing-target")
	a.loadCredentialsFile()
	if err := a.AuthorizeTarget("A", "db"); err != nil {
		t.Fatal("invalid ACL candidate replaced last valid snapshot")
	}
}
