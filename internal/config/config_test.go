package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"dbbridge/internal/config"
)

func TestConfigValidation(t *testing.T) {
	t.Setenv("DBB_TOKEN_TO_HQ", "mock-token-xyz")

	cfgPath := "configs/peer-a.yaml"
	if _, err := os.Stat(cfgPath); err != nil {
		cfgPath = "../../configs/peer-a.yaml"
		if _, err := os.Stat(cfgPath); err != nil {
			cfgPath = "/opt/dbbridge/configs/peer-a.yaml"
		}
	}
	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Node.ID != "plant-a" {
		t.Errorf("expected node.id plant-a, got %s", cfg.Node.ID)
	}

	if cfg.Transport.FrameSize != 32*1024 {
		t.Errorf("expected frame_size 32KiB, got %d", cfg.Transport.FrameSize)
	}

	if cfg.Transport.MaxFrameSize != 256*1024 {
		t.Errorf("expected max_frame_size 256KiB, got %d", cfg.Transport.MaxFrameSize)
	}
}

func TestConfigAuditValidationRules(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Invalid CIDR must fail config validation (DBB-20)
	invalidCIDRYAML := `
node:
  id: node-1
targets:
  t1:
    address: "127.0.0.1:3306"
    allowed_cidrs: ["not-a-valid-cidr"]
peers:
  - id: p1
    endpoints:
      - id: ep1
        url: "http://127.0.0.1:18080"
transport:
  allow_insecure_remote_http: true
`
	p1 := filepath.Join(tmpDir, "invalid_cidr.yaml")
	_ = os.WriteFile(p1, []byte(invalidCIDRYAML), 0600)
	_, err := config.LoadConfig(p1)
	if err == nil {
		t.Errorf("expected config load to fail on invalid CIDR, got nil")
	}

	// 2. Plaintext remote HTTP rejected when allow_insecure_remote_http is false (DBB-16)
	remoteHTTPYAML := `
node:
  id: node-1
peers:
  - id: p1
    endpoints:
      - id: ep1
        url: "http://example.com:18080"
transport:
  allow_insecure_remote_http: false
`
	p2 := filepath.Join(tmpDir, "remote_http.yaml")
	_ = os.WriteFile(p2, []byte(remoteHTTPYAML), 0600)
	_, err2 := config.LoadConfig(p2)
	if err2 == nil {
		t.Errorf("expected config load to reject remote plaintext http, got nil")
	}

	// 3. Zero or negative heartbeat must fail (DBB-24)
	zeroHeartbeatYAML := `
node:
  id: node-1
peers:
  - id: p1
    endpoints:
      - id: ep1
        url: "http://127.0.0.1:18080"
transport:
  heartbeat: 0s
  allow_insecure_remote_http: true
`
	p3 := filepath.Join(tmpDir, "zero_heartbeat.yaml")
	_ = os.WriteFile(p3, []byte(zeroHeartbeatYAML), 0600)
	_, err3 := config.LoadConfig(p3)
	if err3 == nil {
		t.Errorf("expected config load to reject 0s heartbeat, got nil")
	}
}
