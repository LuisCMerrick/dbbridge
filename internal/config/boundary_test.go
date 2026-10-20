package config

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBoundaryCIDRValidationMatchesRuntime(t *testing.T) {
	c := &Config{Node: NodeConfig{ID: "A"}, Targets: map[string]TargetConfig{"db": {Address: "127.0.0.1:3306", AllowedCIDRs: []string{" 127.0.0.0/8 "}}}}
	if err := c.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	_, _, err := net.ParseCIDR(c.Targets["db"].AllowedCIDRs[0])
	if err != nil {
		t.Fatalf("check-config accepted but runtime ParseCIDR rejects unnormalized value %q", c.Targets["db"].AllowedCIDRs[0])
	}
}
func TestBoundaryConfigStrictScalarTypes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		transport TransportConfig
	}{
		{"fractional_bytes", TransportConfig{FrameSizeRaw: float64(1024.9)}},
		{"string_boolean", TransportConfig{Recovery: RecoveryConfig{EnabledRaw: "false"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{Node: NodeConfig{ID: "A"}, Transport: tc.transport}
			if err := c.ApplyDefaultsAndValidate(); err == nil {
				t.Fatal("invalid scalar type accepted")
			}
		})
	}
}

func TestBoundaryRemovedConfigFieldsAreRejected(t *testing.T) {
	for _, fields := range []string{
		"transport:\n  resume_enabled: true\n",
		"transport:\n  resume_grace: 30s\n",
		"transport:\n  resume_barrier_timeout: 5s\n",
		"peers:\n  - id: B\n    url: https://b.invalid\n",
		"peers:\n  - id: B\n    failover:\n      mode: priority\n",
		"listeners:\n  - id: db\n    resume_enabled: true\n",
		"auth:\n  inbound_peers:\n    - id: B\n      tokens: [test-token]\n",
	} {
		p := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(p, []byte("node:\n  id: A\n"+fields), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(p); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("removed field must fail schema validation: %s error=%v", fields, err)
		}
	}
}
