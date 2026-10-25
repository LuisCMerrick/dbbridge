package service

import (
	"path/filepath"
	"testing"

	"dbbridge/internal/config"
)

func TestAuditReverseEnvironmentIncluded(t *testing.T) {
	const name = "DBB_AUDIT_REVERSE_ONLY"
	t.Setenv(name, "audit-reverse-token-0123456789abcdef")
	cfg := &config.Config{Reverse: config.ReverseConfig{Enabled: true, TokenEnv: name}}
	values, err := credentialEnvironment(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if values[name] == "" {
		t.Fatalf("reverse-only token environment omitted from installed service snapshot: got %d entries", len(values))
	}
}

func TestAuditReverseServiceFilesIncluded(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{ConfigFileDir: dir, Reverse: config.ReverseConfig{Enabled: true, TokenFile: "reverse.token", TLS: config.TLSClientConfig{CAFile: filepath.Join(dir, "reverse-ca.pem")}}}
	paths, err := serviceFiles(cfg, filepath.Join(dir, "node.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, p := range paths {
		seen[p] = true
	}
	for _, want := range []string{filepath.Join(dir, "reverse.token"), filepath.Join(dir, "reverse-ca.pem")} {
		if !seen[want] {
			t.Errorf("reverse credential/TLS dependency omitted from service access preflight: %s", filepath.Base(want))
		}
	}
}
