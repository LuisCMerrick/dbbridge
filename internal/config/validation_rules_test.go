package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// egressConfig is a minimal egress node with one IP target.
const egressConfig = `node: {id: B}
http: {enabled: true, listen: '127.0.0.1:18080'}
targets: {db: {address: '127.0.0.1:3306', allowed_cidrs: ['127.0.0.1/32']}}
`

func hasWarning(c *Config, fragment string) bool {
	for _, w := range c.Warnings {
		if strings.Contains(w, fragment) {
			return true
		}
	}
	return false
}

// The quota partition rule is part of the shared snapshot
// validation, so a hot reload candidate is rejected like a startup config.
func TestSnapshotValidationSharedWithReload(t *testing.T) {
	cfg, err := testLoad(t, egressConfig+"auth:\n  inbound_peers:\n    - {id: A, token_envs: [DBB_TEST_TOKEN], allowed_targets: [db]}\n")
	if err != nil {
		t.Fatal(err)
	}
	cc := cfg.CredentialConstraints()
	ok := &AuthConfigFile{Version: 1, InboundPeers: []InboundPeerConfig{{ID: "A", Tokens: []string{testToken}, AllowedTargets: []string{"db"}, MaxSessions: 200}}}
	if _, err := ValidateCredentialSnapshot(ok, cc); err != nil {
		t.Fatalf("valid snapshot rejected: %v", err)
	}
	// Adding a second peer with an omitted quota (counted as 200) at runtime
	// must be rejected exactly as it would be at startup.
	bad := &AuthConfigFile{Version: 1, InboundPeers: append(ok.InboundPeers, InboundPeerConfig{ID: "C", Tokens: []string{"another-token-0123456789"}, AllowedTargets: []string{"db"}, MaxSessions: DefaultInboundMaxSessions})}
	if _, err := ValidateCredentialSnapshot(bad, cc); err == nil || !strings.Contains(err.Error(), "exceeds limits.max_sessions") {
		t.Fatalf("over-quota reload accepted: %v", err)
	}
	unknown := &AuthConfigFile{Version: 1, InboundPeers: []InboundPeerConfig{{ID: "A", Tokens: []string{testToken}, AllowedTargets: []string{"nope"}, MaxSessions: 1}}}
	if _, err := ValidateCredentialSnapshot(unknown, cc); err == nil {
		t.Fatal("unknown ACL target accepted")
	}
	weak := &AuthConfigFile{Version: 1, InboundPeers: []InboundPeerConfig{{ID: "A", Tokens: []string{"short"}, AllowedTargets: []string{"db"}, MaxSessions: 1}}}
	if w, err := ValidateCredentialSnapshot(weak, cc); err != nil || len(w) == 0 {
		t.Fatalf("weak token warning missing on reload: %v %v", w, err)
	}
	refs := CredentialConstraints{Refs: map[string]string{"B": "cred-b"}}
	if _, err := ValidateCredentialSnapshot(&AuthConfigFile{Version: 1}, refs); err == nil || !strings.Contains(err.Error(), "cred-b") {
		t.Fatalf("missing credential_ref accepted: %v", err)
	}
}

// The digest covers every token explicitly.
func TestCredentialDigestTracksTokens(t *testing.T) {
	f := &AuthConfigFile{Version: 1, InboundPeers: []InboundPeerConfig{{ID: "A", Tokens: []string{"token-one-0123456789"}, AllowedTargets: []string{"db"}, MaxSessions: 1}}, OutboundPeers: []OutboundPeerCredential{{ID: "B", Token: "out-0123456789abcdef"}}}
	d1 := CredentialDigest(f)
	f.InboundPeers[0].Tokens[0] = "token-two-0123456789"
	d2 := CredentialDigest(f)
	f.OutboundPeers[0].Token = "out-changed-0123456789"
	d3 := CredentialDigest(f)
	if d1 == d2 || d2 == d3 || d1 == d3 {
		t.Fatal("token rotation not reflected in the credential digest")
	}
	if CredentialDigest(f) != d3 {
		t.Fatal("digest not deterministic")
	}
}

// Localhost is 127.0.0.1 + ::1; 0.0.0.0 covers all IPv4.
func TestListenOverlap(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{"0.0.0.0", "localhost", true}, {"localhost", "0.0.0.0", true},
		{"localhost", "127.0.0.2", false}, {"localhost", "127.0.0.1", true}, {"localhost", "::1", true},
		{"0.0.0.0", "::1", false}, {"::", "127.0.0.1", true}, {"", "10.0.0.1", true},
		{"10.0.0.1", "10.0.0.2", false}, {"[::1]", "::1", true}, {"db.local", "db.local", true},
		{"db.local", "127.0.0.1", false}, {"db.local", "0.0.0.0", true},
	} {
		if got := listenHostsOverlap(tc.a, tc.b); got != tc.want {
			t.Errorf("overlap(%q,%q)=%v want %v", tc.a, tc.b, got, tc.want)
		}
	}
	body := strings.Replace(egressConfig, "listen: '127.0.0.1:18080'", "listen: '0.0.0.0:18080', allow_insecure_listen: true", 1) + "admin: {enabled: true, listen: 'localhost:18080'}\n"
	if _, err := testLoad(t, body); err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("0.0.0.0 vs localhost conflict missed: %v", err)
	}
	body = egressConfig + "admin: {enabled: true, listen: 'localhost:18081'}\n"
	body = strings.Replace(body, "127.0.0.1:18080", "127.0.0.2:18081", 1)
	if _, err := testLoad(t, body); err != nil {
		t.Fatalf("localhost vs 127.0.0.2 reported as conflict: %v", err)
	}
}

// Hostname targets without allowed_cidrs and non-loopback listeners warn.
func TestHostnameTargetRemoteListenerWarn(t *testing.T) {
	cfg, err := testLoad(t, strings.Replace(egressConfig, "address: '127.0.0.1:3306', allowed_cidrs: ['127.0.0.1/32']", "address: 'db.internal:3306'", 1))
	if err != nil || !hasWarning(cfg, "DNS rebinding") {
		t.Fatalf("hostname target without allowed_cidrs: %v %v", err, cfg)
	}
	cfg, err = testLoad(t, testPeerConfig("https://b.example.invalid", "", "listeners: [{id: l, listen: '0.0.0.0:13306', peer: B, target: db}]\n"))
	if err != nil || !hasWarning(cfg, "not a loopback address") {
		t.Fatalf("non-loopback listener: %v %v", err, cfg)
	}
	cfg, err = testLoad(t, testPeerConfig("https://b.example.invalid", "", "listeners: [{id: l, listen: '127.0.0.1:13306', peer: B, target: db}]\n"))
	if err != nil || hasWarning(cfg, "not a loopback address") {
		t.Fatalf("loopback listener warned: %v %v", err, cfg)
	}
}

// Credential errors name the source but never the token.
func TestCredentialErrorsHaveContext(t *testing.T) {
	body := strings.Replace(testPeerConfig("https://b.example.invalid", "", ""), "DBB_TEST_TOKEN", "DBB_TEST_UNSET_VAR", 1)
	_, err := testLoad(t, body)
	if err == nil || !strings.Contains(err.Error(), "DBB_TEST_UNSET_VAR") || !strings.Contains(err.Error(), "peer[B]") {
		t.Fatalf("error lacks context: %v", err)
	}
	_, err = testLoad(t, egressConfig+"auth:\n  inbound_peers:\n    - {id: plant-a, token_envs: [DBB_TEST_UNSET_VAR], allowed_targets: [db]}\n")
	if err == nil || !strings.Contains(err.Error(), "inbound_peers[plant-a]") || !strings.Contains(err.Error(), "DBB_TEST_UNSET_VAR") {
		t.Fatalf("inbound error lacks context: %v", err)
	}
	t.Setenv("DBB_TEST_BAD", "bad token "+testToken)
	_, err = testLoad(t, strings.Replace(testPeerConfig("https://b.example.invalid", "", ""), "DBB_TEST_TOKEN", "DBB_TEST_BAD", 1))
	if err == nil || strings.Contains(err.Error(), testToken) {
		t.Fatalf("malformed token error leaked or missing: %v", err)
	}
}

// World-readable token files are reported.
func TestTokenPermissionWarning(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mode bits are not used on Windows")
	}
	dir := t.TempDir()
	tok := filepath.Join(dir, "t.tok")
	if err := os.WriteFile(tok, []byte(testToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	body := strings.Replace(testPeerConfig("https://b.example.invalid", "", ""), "token_env: DBB_TEST_TOKEN", "token_file: "+tok, 1)
	cfg, err := testLoad(t, body)
	if err != nil || hasWarning(cfg, "secret file") {
		t.Fatalf("0600 token: %v %v", err, cfg.Warnings)
	}
	if err := os.Chmod(tok, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err = testLoad(t, body)
	if err != nil || !hasWarning(cfg, "secret file") {
		t.Fatalf("0644 token not reported: %v %v", err, cfg)
	}
}

// Changing one value derives its dependent defaults.
func TestDerivedDefaults(t *testing.T) {
	cfg, err := testLoad(t, testPeerConfig("https://b.example.invalid", "", "transport: {heartbeat: 20s}\n"))
	if err != nil || cfg.Transport.FailureDetectTimeout != 60*time.Second {
		t.Fatalf("heartbeat only: %v %v", err, cfg)
	}
	cfg, err = testLoad(t, testPeerConfig("https://b.example.invalid", "", "transport: {buffer_per_direction: 1MiB}\n"))
	if err != nil || cfg.Transport.UpWindowBytes != 1<<20 || cfg.Transport.AckBatchBytes != 256<<10 {
		t.Fatalf("buffer_per_direction only: %v %+v", err, cfg)
	}
	// Explicit values are still checked.
	if _, err = testLoad(t, testPeerConfig("https://b.example.invalid", "", "transport: {heartbeat: 20s, failure_detect_timeout: 30s}\n")); err == nil {
		t.Fatal("explicit too-short failure_detect_timeout accepted")
	}
}

// Idle-node and unused-target warnings; empty target names are rejected.
func TestIdleAndUnusedWarnings(t *testing.T) {
	cfg, err := testLoad(t, "node: {id: A}\n")
	if err != nil || !hasWarning(cfg, "nothing to do") {
		t.Fatalf("idle node: %v %v", err, cfg)
	}
	cfg, err = testLoad(t, strings.Replace(egressConfig, "enabled: true", "enabled: false", 1))
	if err != nil || !hasWarning(cfg, "targets are configured") {
		t.Fatalf("disabled http with targets: %v %v", err, cfg)
	}
	cfg, err = testLoad(t, strings.Replace(egressConfig, "node: {id: B}", "node: {id: 'bad idé x'}", 1))
	if err != nil || !hasWarning(cfg, "node id") {
		t.Fatalf("odd node id: %v %v", err, cfg)
	}
	if _, err = testLoad(t, egressConfig+"auth:\n  inbound_peers:\n    - {id: A, token_envs: [DBB_TEST_TOKEN], allowed_targets: [db]}\n"+"limits: {max_sessions: 10}\n"); err != nil {
		t.Fatal(err)
	}
	if _, err = testLoad(t, strings.Replace(egressConfig, "targets: {db:", "targets: {'':", 1)); err == nil {
		t.Fatal("empty target name accepted")
	}
	cfg, err = testLoad(t, egressConfig+"admin: {enabled: true, listen: '0.0.0.0:18090', allow_remote: true}\n")
	if err != nil || !hasWarning(cfg, "without TLS or authentication") {
		t.Fatalf("remote admin: %v %v", err, cfg)
	}
}

// Separate opt-ins for plaintext listen and plaintext peer URLs.
func TestSplitInsecureSwitches(t *testing.T) {
	remote := strings.Replace(egressConfig, "127.0.0.1:18080", "0.0.0.0:18080", 1)
	if _, err := testLoad(t, remote); err == nil {
		t.Fatal("plaintext non-loopback listen accepted without opt-in")
	}
	cfg, err := testLoad(t, strings.Replace(remote, "listen: '0.0.0.0:18080'", "listen: '0.0.0.0:18080', allow_insecure_listen: true", 1))
	if err != nil || !hasWarning(cfg, "plaintext HTTP") {
		t.Fatalf("http.allow_insecure_listen: %v %v", err, cfg)
	}
	if _, err = testLoad(t, testPeerConfig("http://10.0.0.5:18080", "", "")); err == nil {
		t.Fatal("plaintext remote peer URL accepted without opt-in")
	}
	body := strings.Replace(testPeerConfig("http://10.0.0.5:18080", "", ""), "    credential_ref: B\n", "    credential_ref: B\n    allow_insecure_http: true\n", 1)
	if _, err = testLoad(t, body); err != nil {
		t.Fatalf("peers[].allow_insecure_http: %v", err)
	}
}

// The generator uses the loader's token limit.
func TestGeneratorTokenLimit(t *testing.T) {
	o := generateOptions(t, "egress")
	o.TokenFile = filepath.Join(filepath.Dir(o.Output), "big.token")
	if err := os.WriteFile(o.TokenFile, []byte(strings.Repeat("a", 9<<10)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Generate(o); err == nil || !strings.Contains(err.Error(), "larger than") {
		t.Fatalf("9 KiB token accepted by the generator: %v", err)
	}
}

// reload_interval accepts numeric seconds like other durations.
func TestReloadIntervalNumericSeconds(t *testing.T) {
	cfg, err := testLoad(t, testPeerConfig("https://b.example.invalid", "", "auth: {reload_interval: 5}\n"))
	if err != nil || cfg.Auth.ReloadInterval != 5*time.Second {
		t.Fatalf("numeric reload_interval: %v %v", err, cfg)
	}
	if _, err = testLoad(t, testPeerConfig("https://b.example.invalid", "", "auth: {reload_interval: -1}\n")); err == nil {
		t.Fatal("negative reload_interval accepted")
	}
}

// max_sessions must fit the metadata budget.
func TestSessionsFitMetadataBudget(t *testing.T) {
	_, err := testLoad(t, egressConfig+"limits: {max_sessions: 1000}\n")
	if err == nil || !strings.Contains(err.Error(), "metadata_limit") {
		t.Fatalf("max_sessions 1000 with default metadata_limit accepted: %v", err)
	}
	if _, err = testLoad(t, egressConfig+"limits: {max_sessions: 1000}\ntransport: {metadata_limit: 128MiB}\n"); err != nil {
		t.Fatalf("sized metadata_limit rejected: %v", err)
	}
	o := DefaultGeneratePairOptions()
	o.OutputDir, o.MaxSessions = t.TempDir(), 1000
	if _, err = GeneratePair(o); err != nil {
		t.Fatalf("gen-config --max-sessions 1000: %v", err)
	}
}

// Offline validation needs no secrets or TLS files.
func TestOfflineValidation(t *testing.T) {
	body := `node: {id: B}
http: {enabled: true, listen: '0.0.0.0:8443', tls: {enabled: true, cert_file: /nonexistent/b.crt, key_file: /nonexistent/b.key}}
targets: {db: {address: '127.0.0.1:3306', allowed_cidrs: ['127.0.0.1/32']}}
auth:
  inbound_peers:
    - {id: A, token_envs: [DBB_TEST_OFFLINE_UNSET], allowed_targets: [db]}
peers:
  - {id: A, expected_node_id: A, credential_ref: A, token_file: /nonexistent/t, tls: {ca_file: /nonexistent/ca.pem}, endpoints: [{id: e, url: 'https://a.example.invalid:8443'}]}
`
	path := filepath.Join(t.TempDir(), "peer.yaml")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("online validation passed without secrets")
	}
	if _, err := LoadConfigWithOptions(path, LoadOptions{Offline: true}); err != nil {
		t.Fatalf("offline validation: %v", err)
	}
	bad := strings.Replace(body, "allowed_targets: [db]", "allowed_targets: [missing]", 1)
	if err := os.WriteFile(path, []byte(bad), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfigWithOptions(path, LoadOptions{Offline: true}); err == nil {
		t.Fatal("offline validation skipped cross references")
	}
}

// The stop budget used by service managers covers drain + teardown.
func TestMaxShutdownDuration(t *testing.T) {
	cfg, err := testLoad(t, testPeerConfig("https://b.example.invalid", "", "transport: {shutdown_timeout: 20s}\n"))
	if err != nil || cfg.MaxShutdownDuration() != 20*time.Second+ShutdownTeardownBudget {
		t.Fatalf("MaxShutdownDuration: %v %v", err, cfg)
	}
}
