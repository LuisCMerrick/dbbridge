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
