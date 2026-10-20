package auth

import (
	"crypto/sha256"
	"dbbridge/internal/config"
	"dbbridge/internal/observability"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// A reloaded credentials file must keep the startup rule that the
// enabled inbound peers' max_sessions partition limits.max_sessions.
func TestReloadKeepsQuotaPartition(t *testing.T) {
	t.Setenv("DBB_TEST_A", "auth1-token-a")
	t.Setenv("DBB_TEST_B", "auth1-token-b")
	p := filepath.Join(t.TempDir(), "auth.yaml")
	write := func(qa, qb string) {
		t.Helper()
		body := "version: 1\ninbound_peers:\n- id: A\n  token_envs: [DBB_TEST_A]\n  allowed_targets: [db]\n  max_sessions: " + qa + "\n- id: B\n  token_envs: [DBB_TEST_B]\n  allowed_targets: [db]\n  max_sessions: " + qb + "\n"
		if err := os.WriteFile(p, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("5", "5")
	cfg := &config.Config{Auth: config.AuthConfig{CredentialsFile: p, ReloadInterval: time.Hour}, Targets: map[string]config.TargetConfig{"db": {Address: "127.0.0.1:3306"}}}
	cfg.Limits.MaxSessions = 10
	a := NewAuthenticator(cfg)
	defer a.Close()
	if err := a.Err(); err != nil {
		t.Fatal(err)
	}
	revision := a.Revision()
	failed := atomic.LoadInt64(&observability.DefaultMetrics.AuthReloadFailed)
	write("10", "5")
	a.loadCredentialsFile()
	if a.Revision() != revision || a.PeerMaxSessions("A") != 5 {
		t.Fatal("reload exceeding limits.max_sessions was installed")
	}
	if atomic.LoadInt64(&observability.DefaultMetrics.AuthReloadFailed) != failed+1 {
		t.Fatal("rejected reload not counted")
	}
	write("6", "4")
	a.loadCredentialsFile()
	if a.Revision() == revision || a.PeerMaxSessions("A") != 6 {
		t.Fatal("valid repartition rejected")
	}
}

// With three peers, two greedy peers at their share still leave the
// third peer its floor.
func TestThirdPeerKeepsFloor(t *testing.T) {
	a := NewAuthenticator(&config.Config{Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{
		{ID: "A", Tokens: []string{"token-a"}, AllowedTargets: []string{"db"}},
		{ID: "B", Tokens: []string{"token-b"}, AllowedTargets: []string{"db"}},
		{ID: "C", Tokens: []string{"token-c"}, AllowedTargets: []string{"db"}},
	}}})
	defer a.Close()
	const global = 16
	held := int64(0)
	for _, peer := range []string{"A", "B"} {
		for {
			r, ok := a.ReserveFair(peer, "create", global)
			if !ok {
				break
			}
			defer r()
			held++
		}
	}
	if free := global - held; free < 1 {
		t.Fatalf("two greedy peers hold %d of %d", held, global)
	}
	r, ok := a.ReserveFair("C", "create", global)
	if !ok {
		t.Fatal("third peer starved")
	}
	r()
	// ReserveFairN counts amounts against the same share.
	if _, ok := a.ReserveFairN("C", "bytes", 1<<20, PeerShare(1<<20, 3)+1); ok {
		t.Fatal("amount above the share admitted")
	}
}

// A listener removed with the function OnReload returned is not
// called again; AuthenticateBearerDigest returns the accepted digest.
func TestReloadListenerRemovable(t *testing.T) {
	t.Setenv("DBB_TEST_RELOAD", "reload-token")
	p := filepath.Join(t.TempDir(), "auth.yaml")
	write := func(target string) {
		if err := os.WriteFile(p, []byte("version: 1\ninbound_peers:\n- id: A\n  token_envs: [DBB_TEST_RELOAD]\n  allowed_targets: ["+target+"]\n  max_sessions: 2\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("db")
	a := NewAuthenticator(&config.Config{Auth: config.AuthConfig{CredentialsFile: p, ReloadInterval: time.Hour}, Targets: map[string]config.TargetConfig{"db": {Address: "127.0.0.1:1"}, "db2": {Address: "127.0.0.1:2"}}})
	defer a.Close()
	calls := 0
	remove := a.OnReload(func(*Authenticator) { calls++ })
	write("db2")
	a.loadCredentialsFile()
	remove()
	write("db")
	a.loadCredentialsFile()
	if calls != 1 {
		t.Fatalf("listener called %d times, want 1", calls)
	}
	peer, d, err := a.AuthenticateBearerDigest("Bearer reload-token")
	if err != nil || peer != "A" || d != sha256.Sum256([]byte("reload-token")) || d != TokenDigest("Bearer reload-token") {
		t.Fatalf("digest auth: %q %v", peer, err)
	}
}
