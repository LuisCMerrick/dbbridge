package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const egressWithInbound = `node: {id: B}
http: {enabled: true, listen: "127.0.0.1:0"}
targets: {db: {network: tcp, address: "127.0.0.1:3306", allowed_cidrs: ["127.0.0.1/32"]}}
limits: {max_sessions: 10}
auth:
  inbound_peers:
`

// A disabled identity's token sources are not read, so they may be gone.
func TestDisabledIdentityNeedsNoResolvableTokenSources(t *testing.T) {
	body := egressWithInbound + "    - {id: A, token_envs: [DBB_FIX_TOKEN], allowed_targets: [db], max_sessions: 5}\n" +
		"    - {id: C, enabled: false, token_envs: [DBB_UNSET_DISABLED_TOKEN], allowed_targets: [db]}\n" +
		"    - {id: D, enabled: false, token_files: [missing.token], allowed_targets: [db]}\n"
	if _, err := fixLoad(t, body); err != nil {
		t.Fatal(err)
	}
}

// Listing one token twice for an identity is accepted (rotation slots that
// currently hold the same token); sharing it between identities is not.
func TestDuplicateTokenRules(t *testing.T) {
	t.Setenv("DBB_FIX_TOKEN_NEXT", fixToken)
	if _, err := fixLoad(t, egressWithInbound+"    - {id: A, token_envs: [DBB_FIX_TOKEN, DBB_FIX_TOKEN_NEXT], allowed_targets: [db]}\n"); err != nil {
		t.Fatal(err)
	}
	_, err := fixLoad(t, egressWithInbound+"    - {id: A, token_envs: [DBB_FIX_TOKEN], allowed_targets: [db], max_sessions: 5}\n    - {id: C, token_envs: [DBB_FIX_TOKEN_NEXT], allowed_targets: [db], max_sessions: 5}\n")
	if err == nil || !strings.Contains(err.Error(), "inbound peers A and C use the same token") {
		t.Fatalf("shared token: %v", err)
	}
}

// Configuration failures carry stable codes.
func TestConfigurationErrorsCarryStableCodes(t *testing.T) {
	for _, tc := range []struct {
		name, body, code string
	}{
		{"schema", "node: {id: A}\nunknown_key: 1\n", CodeConfigRead},
		{"value", "node: {id: A}\ntransport: {protocol: carrier-pigeon}\n", CodeConfigInvalid},
		{"listen", egressWithInbound + "    - {id: A, token_envs: [DBB_FIX_TOKEN], allowed_targets: [db]}\nadmin: {enabled: true, listen: 'nonsense'}\n", CodeListen},
	} {
		_, err := fixLoad(t, tc.body)
		if got := ErrorCode(err); got != tc.code {
			t.Errorf("%s: code %q, want %q (err %v)", tc.name, got, tc.code, err)
		}
	}
	t.Setenv("DBB_FIX_TOKEN", "")
	path := filepath.Join(t.TempDir(), "peer.yaml")
	if err := os.WriteFile(path, []byte(fixPeerConfig("https://b.example.invalid", "", "")), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); ErrorCode(err) != CodeCredential {
		t.Errorf("unset token env: code %q (err %v)", ErrorCode(err), err)
	}
	if _, err := LoadConfig(filepath.Join(t.TempDir(), "absent.yaml")); ErrorCode(err) != CodeConfigRead {
		t.Errorf("missing file: code %q", ErrorCode(err))
	}
}

// A token file in a tree the generated unit hides is reported when the
// configuration itself is elsewhere.
func TestCredentialFileInSandboxedTreeWarns(t *testing.T) {
	c := &Config{ConfigFileDir: "/etc/dbbridge"}
	c.warnCredentialSources([]string{"/home/ops/b.token", "/etc/dbbridge/a.token"})
	if len(c.Warnings) != 1 || !strings.Contains(c.Warnings[0], "/home/ops/b.token is under /home") {
		t.Fatalf("warnings: %q", c.Warnings)
	}
	c = &Config{ConfigFileDir: "/home/ops/dbbridge"}
	c.warnCredentialSources([]string{"/home/ops/b.token"})
	if len(c.Warnings) != 0 {
		t.Fatalf("same tree warned: %q", c.Warnings)
	}
}
