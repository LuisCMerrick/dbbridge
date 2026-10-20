package auth

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"dbbridge/internal/config"
	"dbbridge/internal/observability"
)

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}

func loadTestConfig(t *testing.T, dir, body string) *config.Config {
	t.Helper()
	writeFile(t, filepath.Join(dir, "peer.yaml"), body)
	c, err := config.LoadConfig(filepath.Join(dir, "peer.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// Peer IDs that are also other peers' credential_ref values still map each
// peer to exactly its configured credential, whatever the map order.
func TestOutboundCredentialRefsThatArePeerIDsMapDeterministically(t *testing.T) {
	t.Setenv("REMAP_TX", "token-for-ref-X-0123456789")
	t.Setenv("REMAP_TY", "token-for-ref-Y-0123456789")
	for i := 0; i < 30; i++ {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "auth.yaml"), "version: 1\noutbound_peers:\n  - {id: X, token_env: REMAP_TX}\n  - {id: Y, token_env: REMAP_TY}\n")
		c := loadTestConfig(t, dir, `node: {id: A}
auth: {credentials_file: auth.yaml}
peers:
  - {id: X, expected_node_id: NX, credential_ref: Y, endpoints: [{id: e, url: "http://127.0.0.1:1", priority: 1}]}
  - {id: Y, expected_node_id: NY, credential_ref: X, endpoints: [{id: e, url: "http://127.0.0.1:2", priority: 1}]}
listeners: [{id: l1, listen: "127.0.0.1:0", peer: X, target: db}]
`)
		a := NewAuthenticator(c)
		x, y := a.GetOutboundToken("X"), a.GetOutboundToken("Y")
		a.Close()
		if x != "token-for-ref-Y-0123456789" || y != "token-for-ref-X-0123456789" {
			t.Fatalf("peer X sends %q, peer Y sends %q", x, y)
		}
	}
}

// Inline credentials (the gen-config layout) rotate without a restart: the
// token files and the auth/token settings of the main configuration are
// re-read on reload, so the add-new, switch, remove-old sequence works.
func TestInlineCredentialsRotateWithoutRestart(t *testing.T) {
	dir := t.TempDir()
	defaults := config.DefaultGeneratePairOptions()
	defaults.OutputDir = dir
	if _, err := config.GeneratePair(defaults); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "peer-a.yaml")
	c, err := config.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	a := NewAuthenticator(c)
	defer a.Close()
	old := a.GetOutboundToken("B")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	newTok := "rotated-token-0123456789abcdef0123456789"
	writeFile(t, filepath.Join(dir, "new.token"), newTok+"\n")
	edit := func(from, to string) {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		next := strings.Replace(string(b), from, to, 1)
		if next == string(b) {
			t.Fatalf("%q not found", from)
		}
		writeFile(t, path, next)
		if err := a.Reload(); err != nil {
			t.Fatal(err)
		}
	}
	// 1. Accept the new token next to the old one.
	edit("      token_files:\n        - shared.token\n", "      token_files:\n        - shared.token\n        - new.token\n")
	for _, tok := range []string{old, newTok} {
		if p, err := a.AuthenticateBearer("Bearer " + tok); err != nil || p != "B" {
			t.Fatalf("during rotation: %q %v", p, err)
		}
	}
	// 2. Send the new token.
	edit("    token_file: shared.token\n", "    token_file: new.token\n")
	if got := a.GetOutboundToken("B"); got != newTok {
		t.Fatalf("outbound token not switched: %q", got)
	}
	// 3. Stop accepting the old token.
	edit("        - shared.token\n        - new.token\n", "        - new.token\n")
	if _, err := a.AuthenticateBearer("Bearer " + old); err == nil {
		t.Fatal("old token still accepted")
	}
	// Rewriting a token file in place is picked up as well.
	writeFile(t, path, string(original))
	writeFile(t, filepath.Join(dir, "shared.token"), newTok+"\n")
	if err := a.Reload(); err != nil {
		t.Fatal(err)
	}
	if a.GetOutboundToken("B") != newTok {
		t.Fatal("rewritten token file not re-read")
	}
}

// Changes that need a restart are rejected loudly and keep the snapshot.
func TestInlineReloadRejectsPeerRemoval(t *testing.T) {
	dir := t.TempDir()
	defaults := config.DefaultGeneratePairOptions()
	defaults.OutputDir = dir
	if _, err := config.GeneratePair(defaults); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "peer-a.yaml")
	c, err := config.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	a := NewAuthenticator(c)
	defer a.Close()
	b, _ := os.ReadFile(path)
	writeFile(t, path, strings.Replace(string(b), "    expected_node_id: B\n    id: B\n", "    expected_node_id: B\n    id: C\n", 1))
	if err := a.Reload(); err == nil || !strings.Contains(err.Error(), "requires a restart") {
		t.Fatalf("peer removal: %v", err)
	}
	if a.GetOutboundToken("B") == "" {
		t.Fatal("previous snapshot lost")
	}
}

// Disabling an identity revokes it even when its token file was removed in
// the same change.
func TestDisablingAnIdentityWorksAfterItsTokenFileIsRemoved(t *testing.T) {
	dir := t.TempDir()
	tok := filepath.Join(dir, "a.token")
	stolen := "stolen-token-0123456789abcdef"
	writeFile(t, tok, stolen+"\n")
	authPath := filepath.Join(dir, "auth.yaml")
	writeFile(t, authPath, "version: 1\ninbound_peers:\n  - {id: A, token_files: [a.token], allowed_targets: [db]}\n")
	c := loadTestConfig(t, dir, `node: {id: B}
http: {enabled: true, listen: "127.0.0.1:0"}
targets: {db: {network: tcp, address: "127.0.0.1:3306", allowed_cidrs: ["127.0.0.1/32"]}}
auth: {credentials_file: auth.yaml, reload_interval: 1s}
`)
	a := NewAuthenticator(c)
	defer a.Close()
	if p, err := a.AuthenticateBearer("Bearer " + stolen); err != nil || p != "A" {
		t.Fatalf("baseline: %q %v", p, err)
	}
	writeFile(t, authPath, "version: 1\ninbound_peers:\n  - {id: A, enabled: false, token_files: [a.token], allowed_targets: [db]}\n")
	if err := os.Remove(tok); err != nil {
		t.Fatal(err)
	}
	if err := a.Reload(); err != nil {
		t.Fatalf("revocation rejected: %v", err)
	}
	if p, err := a.AuthenticateBearer("Bearer " + stolen); err == nil {
		t.Fatalf("revoked token still authenticates as %s", p)
	}
}

// A rejected reload is reported with a stable code and the stale gauge; the
// previous credentials stay in effect until the source is fixed.
func TestRejectedReloadIsReportedAndRecovers(t *testing.T) {
	dir := t.TempDir()
	tok := filepath.Join(dir, "a.token")
	writeFile(t, tok, "first-token-0123456789abcdef\n")
	authPath := filepath.Join(dir, "auth.yaml")
	writeFile(t, authPath, "version: 1\ninbound_peers:\n  - {id: A, token_files: [a.token], allowed_targets: [db]}\n  - {id: C, token_files: [c.token], allowed_targets: [db], enabled: false}\n")
	c := loadTestConfig(t, dir, `node: {id: B}
http: {enabled: true, listen: "127.0.0.1:0"}
targets: {db: {network: tcp, address: "127.0.0.1:3306", allowed_cidrs: ["127.0.0.1/32"]}}
auth: {credentials_file: auth.yaml}
limits: {max_sessions: 10}
`)
	a := NewAuthenticator(c)
	defer a.Close()
	if err := a.Err(); err != nil {
		t.Fatal(err)
	}
	// Enabling C whose token file does not exist is rejected as a whole.
	writeFile(t, authPath, "version: 1\ninbound_peers:\n  - {id: A, token_files: [a.token], allowed_targets: [db], max_sessions: 5}\n  - {id: C, token_files: [c.token], allowed_targets: [db], max_sessions: 5}\n")
	err := a.Reload()
	var coded *config.CodedError
	if err == nil || !errors.As(err, &coded) || coded.Code != config.CodeReloadRejected {
		t.Fatalf("reload error %v lacks code %s", err, config.CodeReloadRejected)
	}
	if atomic.LoadInt64(&observability.DefaultMetrics.AuthCredentialsStale) != 1 {
		t.Fatal("stale gauge not set")
	}
	if _, err := a.AuthenticateBearer("Bearer first-token-0123456789abcdef"); err != nil {
		t.Fatalf("previous snapshot lost: %v", err)
	}
	writeFile(t, filepath.Join(dir, "c.token"), "second-token-0123456789abcdef\n")
	if err := a.Reload(); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt64(&observability.DefaultMetrics.AuthCredentialsStale) != 0 {
		t.Fatal("stale gauge not cleared")
	}
	if p, err := a.AuthenticateBearer("Bearer second-token-0123456789abcdef"); err != nil || p != "C" {
		t.Fatalf("fixed candidate not installed: %q %v", p, err)
	}
}

// The same token listed for the current and the next slot of one identity
// is accepted; a token shared by two identities is rejected with a clear
// message.
func TestSameTokenTwiceForOneIdentity(t *testing.T) {
	t.Setenv("DUP_CURRENT", "dup-token-0123456789abcdef")
	t.Setenv("DUP_NEXT", "dup-token-0123456789abcdef")
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "auth.yaml"), "version: 1\ninbound_peers:\n  - {id: A, token_envs: [DUP_CURRENT, DUP_NEXT], allowed_targets: [db]}\n")
	base := `node: {id: B}
http: {enabled: true, listen: "127.0.0.1:0"}
targets: {db: {network: tcp, address: "127.0.0.1:3306", allowed_cidrs: ["127.0.0.1/32"]}}
auth: {credentials_file: auth.yaml}
limits: {max_sessions: 10}
`
	c := loadTestConfig(t, dir, base)
	a := NewAuthenticator(c)
	defer a.Close()
	if p, err := a.AuthenticateBearer("Bearer dup-token-0123456789abcdef"); err != nil || p != "A" {
		t.Fatalf("%q %v", p, err)
	}
	writeFile(t, filepath.Join(dir, "auth.yaml"), "version: 1\ninbound_peers:\n  - {id: A, token_envs: [DUP_CURRENT], allowed_targets: [db], max_sessions: 5}\n  - {id: D, token_envs: [DUP_NEXT], allowed_targets: [db], max_sessions: 5}\n")
	_, err := config.LoadConfig(filepath.Join(dir, "peer.yaml"))
	if err == nil || err.Error() != "credentials: inbound peers A and D use the same token; every inbound identity needs its own token" {
		t.Fatalf("shared token: %v", err)
	}
}
