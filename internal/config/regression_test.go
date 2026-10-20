package config

import (
	"testing"
	"time"
)

func TestRegressionConfigContracts(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"missing_outbound_credential", func(c *Config) {
			c.Peers = []PeerConfig{{ID: "b", ExpectedNodeID: "b", Endpoints: []EndpointConfig{{ID: "e", URL: "https://b.invalid"}}}}
		}},
		{"endpoint_with_credentials_path_query_fragment", func(c *Config) {
			c.Peers = []PeerConfig{{ID: "b", Endpoints: []EndpointConfig{{ID: "e", URL: "https://user:pass@b.invalid/path?q=x#frag"}}}}
		}},
		{"duplicate_inbound_identity", func(c *Config) { c.Auth.InboundPeers = []InboundPeerConfig{{ID: "a"}, {ID: "a"}} }},
		{"negative_session_limit", func(c *Config) { c.Limits.MaxSessions = -1 }},
		{"peer_overrides_global_recovery_off", func(c *Config) {
			c.Transport.Recovery.EnabledRaw = false
			c.Peers = []PeerConfig{{ID: "b", Endpoints: []EndpointConfig{{ID: "e", URL: "https://b.invalid"}}}}
			c.Peers[0].Recovery = &PeerRecoveryConfig{Enabled: BoolPtr(true)}
		}},
		{"insufficient_codec_budget", func(c *Config) { c.Transport.CodecBufferLimitRaw = 1 }},
		{"missing_credentials_file", func(c *Config) { c.Auth.CredentialsFile = "/definitely/missing/credential.yaml" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{Node: NodeConfig{ID: "a"}}
			tc.mutate(c)
			if e := c.ApplyDefaultsAndValidate(); e == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}
func TestRegressionDurationIntegerOverflow(t *testing.T) {
	d, e := parseDuration(int64(9223372036854775807), time.Second)
	if e == nil {
		t.Fatalf("overflow accepted: %v", d)
	}
}
