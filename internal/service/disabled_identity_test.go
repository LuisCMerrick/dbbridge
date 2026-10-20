package service

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"dbbridge/internal/config"
)

// The token files of a disabled inline identity are never read, so the
// service does not need them: they may already be deleted.
func TestDisabledIdentityTokenFileNotRequired(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "peer.yaml")
	if err := os.WriteFile(filepath.Join(dir, "current.token"), []byte("current-token-0123456789abcdef0123\n"), 0600); err != nil {
		t.Fatal(err)
	}
	data := "node:\n  id: B\nhttp:\n  enabled: true\ntargets:\n  db:\n    address: 127.0.0.1:3306\nauth:\n  inbound_peers:\n    - id: current\n      token_files: [current.token]\n      allowed_targets: [db]\n    - id: retired\n      enabled: false\n      token_files: [removed.token]\n      allowed_targets: [db]\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	files, err := serviceFiles(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(files, filepath.Join(dir, "removed.token")) {
		t.Fatalf("service requires the disabled identity's token file: %v", files)
	}
	if !slices.Contains(files, filepath.Join(dir, "current.token")) {
		t.Fatalf("service does not require the enabled identity's token file: %v", files)
	}
}
