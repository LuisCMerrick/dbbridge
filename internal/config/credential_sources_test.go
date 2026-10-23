package config

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

func TestMultiDocumentConfigRejected(t *testing.T) {
	p := filepath.Join(t.TempDir(), "multi.yaml")
	os.WriteFile(p, []byte("node:\n  id: first\n---\nunknown_security_option: true\n"), 0600)
	if _, e := LoadConfig(p); e == nil {
		t.Fatal("ignored second YAML document with unknown security field")
	}
}
func TestMixedCredentialSourcesRejected(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "auth.yaml")
	os.WriteFile(p, []byte("version: 1\n"), 0600)
	t.Setenv("DBB_TEST_MIXED", "outbound-token")
	c := &Config{Node: NodeConfig{ID: "A"}, Auth: AuthConfig{CredentialsFile: p}, Peers: []PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "DBB_TEST_MIXED", Endpoints: []EndpointConfig{{ID: "e", URL: "https://b.invalid"}}}}}
	if e := c.ApplyDefaultsAndValidate(); e == nil {
		t.Fatal("credentials_file plus direct peer token_env accepted; runtime file snapshot discards direct token")
	}
}
func TestIPv6DefaultPortNormalization(t *testing.T) {
	t.Setenv("DBB_TEST_IPV6", "outbound-token")
	c := &Config{Node: NodeConfig{ID: "A"}, Peers: []PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "DBB_TEST_IPV6", Endpoints: []EndpointConfig{{ID: "e", URL: "http://[::1]:80"}}}}}
	if e := c.ApplyDefaultsAndValidate(); e != nil {
		t.Fatal(e)
	}
	got := c.Peers[0].Endpoints[0].URL
	u, e := url.Parse(got)
	if e != nil {
		t.Fatalf("IPv6 origin corrupted: %q parse error=%v", got, e)
	}
	if u.Hostname() != "::1" {
		t.Fatalf("IPv6 origin corrupted: %q hostname=%q", got, u.Hostname())
	}
}
func TestZeroHealthcheckTimeoutRejected(t *testing.T) {
	t.Setenv("DBB_TEST_HC", "outbound-token")
	c := &Config{Node: NodeConfig{ID: "A"}, Peers: []PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "DBB_TEST_HC", Endpoints: []EndpointConfig{{ID: "e", URL: "https://b.invalid"}}, Failover: FailoverConfig{HealthcheckTimeoutRaw: "0s"}}}}
	if e := c.ApplyDefaultsAndValidate(); e == nil {
		t.Fatal("accepted zero healthcheck timeout; all health probes immediately cancel")
	}
}

func TestInlineReloadRejectsAddedOrDuplicatePeer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "peer.yaml")
	t.Setenv("DBB_TEST_INLINE_TOK", "inline-token-0123456789abcdef")
	base := "node:\n  id: A\npeers:\n  - id: B\n    expected_node_id: B\n    credential_ref: B\n    token_env: DBB_TEST_INLINE_TOK\n    endpoints:\n      - id: e1\n        url: http://127.0.0.1:18080\n"
	if err := os.WriteFile(path, []byte(base), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	src := cfg.CredentialSource()
	if _, _, err := src.Read(); err != nil {
		t.Fatal(err)
	}
	added := base + "  - id: C\n    expected_node_id: C\n    credential_ref: C\n    token_env: DBB_TEST_INLINE_TOK\n    endpoints:\n      - id: e1\n        url: http://127.0.0.1:18081\n"
	if err := os.WriteFile(path, []byte(added), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := src.Read(); err == nil {
		t.Fatal("inline reload accepted a newly added peer without requiring a restart")
	}
	dup := base + "  - id: B\n    expected_node_id: B\n    credential_ref: B\n    token_env: DBB_TEST_INLINE_TOK\n    endpoints:\n      - id: e1\n        url: http://127.0.0.1:18081\n"
	if err := os.WriteFile(path, []byte(dup), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := src.Read(); err == nil {
		t.Fatal("inline reload accepted duplicate peer IDs")
	}
}
