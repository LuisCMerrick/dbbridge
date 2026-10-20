package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAudit20261017ConfigRejectsShortTombstoneTTL(t *testing.T) {
	c := &Config{Node: NodeConfig{ID: "A"}, Transport: TransportConfig{IdempotencyTombstoneTTLRaw: "1ms"}}
	err := c.ApplyDefaultsAndValidate()
	t.Logf("configured tombstone TTL=%s error=%v", c.Transport.IdempotencyTombstoneTTL, err)
	if err == nil && c.Transport.IdempotencyTombstoneTTL < 10*time.Minute {
		t.Fatal("configuration accepted a tombstone guarantee below 10 minutes")
	}
}

// The specification does not define the empty-list case explicitly. Record the
// permissive default as a policy question, without treating it as a hard failure.
func TestAudit20261017ObserveMissingTargetCIDRs(t *testing.T) {
	c := &Config{Node: NodeConfig{ID: "B"}, Targets: map[string]TargetConfig{"db": {Address: "127.0.0.1:3306"}}}
	if err := c.ApplyDefaultsAndValidate(); err == nil {
		t.Log("confirmed: target without an address allowlist is accepted; runtime empty policy allows any resolved address")
	} else {
		t.Logf("configuration rejected: %v", err)
	}
}
func TestAudit20261017CredentialsRequirePositiveQuota(t *testing.T) {
	t.Setenv("AUDIT_20261017_AUTH", "audit-credential")
	p := filepath.Join(t.TempDir(), "auth.yaml")
	os.WriteFile(p, []byte("version: 1\ninbound_peers:\n  - id: A\n    token_envs: [AUDIT_20261017_AUTH]\n    allowed_targets: [db]\n    max_sessions: 0\n"), 0600)
	f, _, err := ReadCredentials(p)
	if err == nil {
		t.Fatalf("credentials file accepted quota=%d (runtime interprets it as unlimited)", f.InboundPeers[0].MaxSessions)
	}
}
func TestAudit20261017CheckConfigValidatesTLSFiles(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.yaml")
	os.WriteFile(p, []byte("node: {id: B}\nhttp:\n  enabled: true\n  tls: {enabled: true, cert_file: missing.crt, key_file: missing.key}\n"), 0600)
	if _, err := LoadConfig(p); err == nil {
		t.Fatal("check-config accepted nonexistent TLS certificate and private key")
	}
}

func TestTombstoneTTLBoundary(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{{"9m59s", false}, {"10m", true}, {"11m", true}} {
		c := &Config{Node: NodeConfig{ID: "A"}, Transport: TransportConfig{IdempotencyTombstoneTTLRaw: tc.value}}
		if err := c.ApplyDefaultsAndValidate(); (err == nil) != tc.valid {
			t.Fatalf("ttl=%s error=%v", tc.value, err)
		}
	}
}
func TestOmittedCredentialQuotaIsBounded(t *testing.T) {
	t.Setenv("PATCH_QUOTA_TOKEN", "quota-test-token")
	p := filepath.Join(t.TempDir(), "auth.yaml")
	if err := os.WriteFile(p, []byte("version: 1\ninbound_peers:\n- id: A\n  token_envs: [PATCH_QUOTA_TOKEN]\n  allowed_targets: [db]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	f, _, err := ReadCredentials(p)
	if err != nil || f.InboundPeers[0].MaxSessions != 200 {
		t.Fatalf("omitted quota: %+v error=%v", f, err)
	}
}
