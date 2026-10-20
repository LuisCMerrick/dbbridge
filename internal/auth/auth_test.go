package auth_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"dbbridge/internal/auth"
	"dbbridge/internal/config"
)

func TestDualTokenRotation(t *testing.T) {
	os.Setenv("TOKEN_CURRENT", "token-version-2")
	os.Setenv("TOKEN_PREV", "token-version-1")

	cfg := &config.Config{
		Auth: config.AuthConfig{
			InboundPeers: []config.InboundPeerConfig{
				{
					ID:             "peer-alpha",
					Enabled:        config.BoolPtr(true),
					TokenEnvs:      []string{"TOKEN_CURRENT", "TOKEN_PREV"},
					Tokens:         []string{"token-version-2", "token-version-1"},
					AllowedTargets: []string{"target-mysql"},
					MaxSessions:    10,
				},
			},
		},
	}

	authenticator := auth.NewAuthenticator(cfg)

	// Test 1: Verify current token
	peerID, err := authenticator.AuthenticateBearer("Bearer token-version-2")
	if err != nil || peerID != "peer-alpha" {
		t.Fatalf("expected peer-alpha for current token, got err=%v, peerID=%s", err, peerID)
	}

	// Test 2: Verify prev token during rotation
	peerID, err = authenticator.AuthenticateBearer("Bearer token-version-1")
	if err != nil || peerID != "peer-alpha" {
		t.Fatalf("expected peer-alpha for prev token, got err=%v, peerID=%s", err, peerID)
	}

	// Test 3: Verify invalid token rejected
	_, err = authenticator.AuthenticateBearer("Bearer invalid-token")
	if err == nil {
		t.Fatalf("expected error for invalid token, got nil")
	}

	// Test 4: Target authorization
	if err := authenticator.AuthorizeTarget("peer-alpha", "target-mysql"); err != nil {
		t.Errorf("expected target authorized, got %v", err)
	}
	if err := authenticator.AuthorizeTarget("peer-alpha", "unauthorized-target"); err == nil {
		t.Errorf("expected unauthorized target error, got nil")
	}
}

func TestAuditDisabledInboundPeer(t *testing.T) {
	cfg := &config.Config{
		Auth: config.AuthConfig{
			InboundPeers: []config.InboundPeerConfig{
				{
					ID:             "peer-disabled",
					Enabled:        config.BoolPtr(false),
					Tokens:         []string{"token-disabled"},
					AllowedTargets: []string{"target-mysql"},
				},
			},
		},
	}
	authenticator := auth.NewAuthenticator(cfg)
	_, err := authenticator.AuthenticateBearer("Bearer token-disabled")
	if err == nil {
		t.Fatalf("expected error for disabled peer, got nil")
	}
}

func TestAuditHotReloadDeletesRemovedPeer(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("RELOAD_TOKEN_1", "token-1")
	t.Setenv("RELOAD_TOKEN_2", "token-2")
	credPath := filepath.Join(tmpDir, "credentials.yaml")

	initialYAML := `
version: 1
inbound_peers:
  - id: peer-1
    enabled: true
    token_envs: [RELOAD_TOKEN_1]
    allowed_targets: ["target-1"]
  - id: peer-2
    enabled: true
    token_envs: [RELOAD_TOKEN_2]
    allowed_targets: ["target-1"]
`
	if err := os.WriteFile(credPath, []byte(initialYAML), 0600); err != nil {
		t.Fatalf("failed to write initial credentials: %v", err)
	}

	cfg := &config.Config{
		ConfigFileDir: tmpDir,
		Auth: config.AuthConfig{
			CredentialsFile: credPath,
			ReloadInterval:  50 * time.Millisecond,
		},
	}
	authenticator := auth.NewAuthenticator(cfg)
	defer authenticator.Close()

	// Both peer-1 and peer-2 authenticate
	p1, err := authenticator.AuthenticateBearer("Bearer token-1")
	if err != nil || p1 != "peer-1" {
		t.Fatalf("expected peer-1, got %v", err)
	}
	p2, err := authenticator.AuthenticateBearer("Bearer token-2")
	if err != nil || p2 != "peer-2" {
		t.Fatalf("expected peer-2, got %v", err)
	}

	time.Sleep(60 * time.Millisecond)

	// Remove peer-2 from credentials file
	updatedYAML := `
version: 1
inbound_peers:
  - id: peer-1
    enabled: true
    token_envs: [RELOAD_TOKEN_1]
    allowed_targets: ["target-1"]
`
	if err := os.WriteFile(credPath, []byte(updatedYAML), 0600); err != nil {
		t.Fatalf("failed to write updated credentials: %v", err)
	}

	// Wait for reload
	time.Sleep(120 * time.Millisecond)

	// peer-1 still works, peer-2 MUST be rejected
	p1, err = authenticator.AuthenticateBearer("Bearer token-1")
	if err != nil || p1 != "peer-1" {
		t.Fatalf("expected peer-1 to remain valid, got %v", err)
	}
	_, err = authenticator.AuthenticateBearer("Bearer token-2")
	if err == nil {
		t.Fatalf("expected peer-2 to be deleted and revoked after hot reload, but was accepted")
	}
}
