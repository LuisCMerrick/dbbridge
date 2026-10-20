package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigRejectsShortTombstoneTTL(t *testing.T) {
	c := &Config{Node: NodeConfig{ID: "A"}, Transport: TransportConfig{IdempotencyTombstoneTTLRaw: "1ms"}}
	err := c.ApplyDefaultsAndValidate()
	t.Logf("configured tombstone TTL=%s error=%v", c.Transport.IdempotencyTombstoneTTL, err)
	if err == nil && c.Transport.IdempotencyTombstoneTTL < 10*time.Minute {
		t.Fatal("configuration accepted a tombstone guarantee below 10 minutes")
	}
}

// A target given as an IP literal needs no allowed_cidrs: it can only ever
// resolve to itself.
func TestIPTargetWithoutCIDRsAccepted(t *testing.T) {
	c := &Config{Node: NodeConfig{ID: "B"}, Targets: map[string]TargetConfig{"db": {Address: "127.0.0.1:3306"}}}
	if err := c.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
}
func TestCredentialsRequirePositiveQuota(t *testing.T) {
	t.Setenv("DBB_TEST_AUTH", "test-credential")
	p := filepath.Join(t.TempDir(), "auth.yaml")
	os.WriteFile(p, []byte("version: 1\ninbound_peers:\n  - id: A\n    token_envs: [DBB_TEST_AUTH]\n    allowed_targets: [db]\n    max_sessions: 0\n"), 0600)
	f, _, err := ReadCredentials(p)
	if err == nil {
		t.Fatalf("credentials file accepted quota=%d (runtime interprets it as unlimited)", f.InboundPeers[0].MaxSessions)
	}
}
func TestCheckConfigValidatesTLSFiles(t *testing.T) {
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
	t.Setenv("DBB_TEST_QUOTA_TOKEN", "quota-test-token")
	p := filepath.Join(t.TempDir(), "auth.yaml")
	if err := os.WriteFile(p, []byte("version: 1\ninbound_peers:\n- id: A\n  token_envs: [DBB_TEST_QUOTA_TOKEN]\n  allowed_targets: [db]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	f, _, err := ReadCredentials(p)
	if err != nil || f.InboundPeers[0].MaxSessions != 200 {
		t.Fatalf("omitted quota: %+v error=%v", f, err)
	}
}
