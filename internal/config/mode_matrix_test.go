package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestV2StrictConfigurationBounds(t *testing.T) {
	for _, tc := range []struct{ name, yaml string }{
		{"network", "targets:\n  db: {network: udp, address: '127.0.0.1:3306'}\n"},
		{"target_port", "targets:\n  db: {network: tcp, address: '127.0.0.1:0'}\n"},
		{"target_host", "targets:\n  db: {network: tcp, address: ':3306'}\n"},
		{"overlap", "http: {enabled: true, listen: '0.0.0.0:8888'}\nadmin: {enabled: true, listen: '127.0.0.1:8888'}\n"},
		{"receipt_capacity", "transport: {up_max_inflight_frames: 32, receipt_limit_per_session: 64}\n"},
		{"coalesce_limit", "transport: {max_coalesce_delay: 2ms}\n"},
		{"removed_noop_knob", "transport: {resume_max_retries: 5}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(p, []byte("node: {id: A}\n"+tc.yaml), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadConfig(p); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestV2ExampleModeMatrix(t *testing.T) {
	t.Setenv("DBB_TOKEN_A_TO_B", "matrix-a-to-b")
	t.Setenv("DBB_TOKEN_B_TO_A", "matrix-b-to-a")
	for _, node := range []string{"a", "b"} {
		for n := 1; n <= 3; n++ {
			for _, failover := range []bool{false, true} {
				for _, recovery := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/endpoints=%d/failover=%t/recovery=%t", node, n, failover, recovery), func(t *testing.T) {
						c := testProductionExample(t, node)
						if len(c.Peers[0].Endpoints) < 3 {
							c.Peers[0].Endpoints = append(c.Peers[0].Endpoints, EndpointConfig{ID: "tertiary", URL: "https://tertiary.example.invalid", Priority: 3})
						}
						c.Peers[0].Endpoints = c.Peers[0].Endpoints[:n]
						c.Peers[0].Failover.Enabled = BoolPtr(failover)
						c.Transport.Recovery.EnabledRaw = recovery
						if err := c.ApplyDefaultsAndValidate(); err != nil {
							t.Fatal(err)
						}
					})
				}
			}
		}
	}
}
