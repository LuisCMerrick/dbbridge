package auth

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dbbridge/internal/config"
)

// Hot reload runs the same
// config.ValidateCredentialSnapshot as startup. The quota partition rule
// still holds, and its warnings (here a weak token) are logged once per
// changed snapshot rather than on every reload tick.
func TestReloadUsesSharedSnapshotValidation(t *testing.T) {
	p := filepath.Join(t.TempDir(), "auth.yaml")
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(p, []byte("version: 1\ninbound_peers:\n"+body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("INT_TOKEN_A", "integration-token-a-0123456789")
	t.Setenv("INT_TOKEN_B", "short")
	write("- {id: A, token_envs: [INT_TOKEN_A], allowed_targets: [db], max_sessions: 2}\n")
	cfg := &config.Config{Auth: config.AuthConfig{CredentialsFile: p, ReloadInterval: time.Hour}, Limits: config.LimitsConfig{MaxSessions: 4}, Targets: map[string]config.TargetConfig{"db": {Address: "127.0.0.1:3306"}}}
	a := NewAuthenticator(cfg)
	defer a.Close()
	if err := a.Err(); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(old)

	rev := a.Revision()
	write("- {id: A, token_envs: [INT_TOKEN_A], allowed_targets: [db], max_sessions: 3}\n- {id: B, token_envs: [INT_TOKEN_B], allowed_targets: [db], max_sessions: 2}\n")
	a.loadCredentialsFile()
	if a.Revision() != rev {
		t.Fatal("quota sum 5 > limits.max_sessions 4 was installed")
	}
	write("- {id: A, token_envs: [INT_TOKEN_A], allowed_targets: [db], max_sessions: 2}\n- {id: B, token_envs: [INT_TOKEN_B], allowed_targets: [db], max_sessions: 2}\n")
	a.loadCredentialsFile()
	a.loadCredentialsFile()
	if a.Revision() == rev {
		t.Fatal("valid snapshot not installed")
	}
	if n := strings.Count(logs.String(), "credentials reload warning"); n != 1 {
		t.Fatalf("weak-token warning logged %d times, want 1: %s", n, logs.String())
	}
	write("- {id: A, token_envs: [INT_TOKEN_A], allowed_targets: [nope], max_sessions: 2}\n")
	rev = a.Revision()
	a.loadCredentialsFile()
	if a.Revision() != rev {
		t.Fatal("unknown ACL target installed")
	}
}
