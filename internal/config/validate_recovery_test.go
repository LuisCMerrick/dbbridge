package config

import (
	"strings"
	"testing"
)

func recoveryWarningConfig(t *testing.T, protocol, grace string) []string {
	t.Helper()
	t.Setenv("RECOVERY_WARN_TOKEN", strings.Repeat("ab", 32))
	c := &Config{
		Node:      NodeConfig{ID: "A"},
		Transport: TransportConfig{Protocol: protocol, Recovery: RecoveryConfig{EnabledRaw: true, GraceRaw: grace}},
		Peers:     []PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "RECOVERY_WARN_TOKEN", Endpoints: []EndpointConfig{{ID: "e", URL: "https://b.example:8443"}}}},
	}
	if err := c.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, w := range c.Warnings {
		if strings.Contains(w, "failover.cooldown") {
			out = append(out, w)
		}
	}
	return out
}

func TestWarnsWhenGraceCannotOutlastCooldownOfOnlyCarrier(t *testing.T) {
	if w := recoveryWarningConfig(t, "ws", "8s"); len(w) != 1 {
		t.Fatalf("forced ws, one endpoint, grace 8s <= cooldown 5s + barrier 5s: warnings %q", w)
	}
	if w := recoveryWarningConfig(t, "auto", "8s"); len(w) != 0 {
		t.Fatalf("auto has a second carrier: warnings %q", w)
	}
	if w := recoveryWarningConfig(t, "http-sse", "30s"); len(w) != 0 {
		t.Fatalf("default grace: warnings %q", w)
	}
}
