package auth

import (
	"dbbridge/internal/config"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestQuotaReloadRejectsZeroAndRetainsCounts(t *testing.T) {
	t.Setenv("DBB_TEST_RELOAD_TOKEN", "reload-test-token")
	p := filepath.Join(t.TempDir(), "auth.yaml")
	write := func(quota string) {
		t.Helper()
		if err := os.WriteFile(p, []byte("version: 1\ninbound_peers:\n- id: A\n  token_envs: [DBB_TEST_RELOAD_TOKEN]\n  allowed_targets: [db]\n  max_sessions: "+quota+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("2")
	a := NewAuthenticator(&config.Config{Auth: config.AuthConfig{CredentialsFile: p, ReloadInterval: time.Hour}, Targets: map[string]config.TargetConfig{"db": {Address: "127.0.0.1:3306"}}})
	defer a.Close()
	if err := a.Err(); err != nil {
		t.Fatal(err)
	}
	if err := a.AuthorizeAndReserveTarget("A", "db"); err != nil {
		t.Fatal(err)
	}
	if err := a.AuthorizeAndReserveTarget("A", "db"); err != nil {
		t.Fatal(err)
	}
	revision := a.Revision()
	write("0")
	a.loadCredentialsFile()
	if a.Revision() != revision || a.AuthorizeAndReserveTarget("A", "db") != ErrMaxSessions {
		t.Fatal("invalid quota replaced snapshot or erased counts")
	}
	write("1")
	a.loadCredentialsFile()
	if a.Revision() == revision || a.AuthorizeAndReserveTarget("A", "db") != ErrMaxSessions {
		t.Fatal("lower quota did not retain active counts")
	}
	a.DecrSession("A")
	if a.AuthorizeAndReserveTarget("A", "db") != ErrMaxSessions {
		t.Fatal("one active session must exhaust quota one")
	}
	a.DecrSession("A")
	if err := a.AuthorizeAndReserveTarget("A", "db"); err != nil {
		t.Fatal(err)
	}
	a.DecrSession("A")
}
