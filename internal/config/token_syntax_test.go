package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestYAMLAliasCannotBypassProtocolType(t *testing.T) {
	p := filepath.Join(t.TempDir(), "peer.yaml")
	data := "node:\n  id: &numeric 1.4\ntransport:\n  protocol_version: *numeric\n"
	if err := os.WriteFile(p, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := LoadConfig(p)
	if err == nil {
		t.Fatalf("numeric YAML alias silently accepted as protocol string: protocol_version=%q node.id=%q", c.Transport.ProtocolVersion, c.Node.ID)
	}
}

func TestTokenValidationAcrossCredentialSources(t *testing.T) {
	for _, tc := range []struct {
		name, token string
		valid       bool
	}{
		{"ordinary", "a-safe-token_0123", true},
		{"empty", "", false},
		{"space", "has space", false},
		{"tab", "has\ttab", false},
		{"unicode-space", "has\u00a0space", false},
		{"control", "has\x01control", false},
		{"oversized", strings.Repeat("x", maxTokenBytes+1), false},
		{"maximum", strings.Repeat("x", maxTokenBytes), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DBB_TEST_SOURCE_TOKEN", tc.token)
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "token"), []byte(tc.token+"\r\n"), 0600); err != nil {
				t.Fatal(err)
			}
			for _, source := range []string{"env", "file"} {
				p := PeerConfig{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", Endpoints: []EndpointConfig{{ID: "one", URL: "http://127.0.0.1:18080"}}}
				in := InboundPeerConfig{ID: "A", AllowedTargets: []string{"db"}}
				out := OutboundPeerCredential{ID: "B"}
				if source == "env" {
					p.TokenEnv, out.TokenEnv = "DBB_TEST_SOURCE_TOKEN", "DBB_TEST_SOURCE_TOKEN"
					in.TokenEnvs = []string{"DBB_TEST_SOURCE_TOKEN"}
				} else {
					p.TokenFile, out.TokenFile = "token", "token"
					in.TokenFiles = []string{"token"}
				}
				c := &Config{ConfigFileDir: dir, Node: NodeConfig{ID: "A"}, Peers: []PeerConfig{p}}
				if err := c.ApplyDefaultsAndValidate(); (err == nil) != tc.valid {
					t.Fatalf("inline %s: valid=%t error=%v", source, tc.valid, err)
				}
				for _, f := range []*AuthConfigFile{{Version: 1, InboundPeers: []InboundPeerConfig{in}}, {Version: 1, OutboundPeers: []OutboundPeerCredential{out}}} {
					refs, err := ResolveCredentials(f, dir)
					if (err == nil) != tc.valid {
						t.Fatalf("credential snapshot %s: valid=%t error=%v", source, tc.valid, err)
					}
					if err == nil && source == "file" && (len(refs) != 1 || refs[0] != filepath.Join(dir, "token")) {
						t.Fatal("relative token file was not retained for credential reload")
					}
				}
			}
		})
	}
}

func TestTokenFilesRejectExtraLines(t *testing.T) {
	dir := t.TempDir()
	for _, body := range []string{"token\n\n", "token\r\n\r\n", "token\nother"} {
		if err := os.WriteFile(filepath.Join(dir, "token"), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := resolveToken("token", "", dir); err == nil {
			t.Fatal("multiple lines normalized into a credential")
		}
	}
}

func TestREADMEStandalonePairLoads(t *testing.T) {
	t.Setenv("DBB_TOKEN_SECRET", "readme-validation-token")
	b, err := os.ReadFile(filepath.Join("..", "..", "docs", "examples.md"))
	if err != nil {
		t.Fatal(err)
	}
	content := strings.ReplaceAll(string(b), "\r\n", "\n")
	_, example, ok := strings.Cut(content, "<!-- example:standalone-pair -->")
	if !ok {
		t.Fatal("missing standalone example in docs/examples.md")
	}
	example, _, _ = strings.Cut(example, "<!-- /example:standalone-pair -->")
	count := 0
	for {
		_, rest, found := strings.Cut(example, "```yaml\n")
		if !found {
			break
		}
		body, rest, found := strings.Cut(rest, "```")
		if !found {
			t.Fatal("unclosed README YAML")
		}
		p := filepath.Join(t.TempDir(), "peer.yaml")
		if err := os.WriteFile(p, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(p); err != nil {
			t.Fatalf("README standalone config %d: %v", count+1, err)
		}
		count++
		example = rest
	}
	if count != 2 {
		t.Fatalf("validated %d configs, expected complete pair", count)
	}
}

func TestOutboundWhitespaceTokenRejectedDuringCheck(t *testing.T) {
	t.Setenv("DBB_TEST_BAD_TOKEN", "contains a space")
	c := &Config{Node: NodeConfig{ID: "A"}, Peers: []PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "DBB_TEST_BAD_TOKEN", Endpoints: []EndpointConfig{{ID: "one", URL: "http://127.0.0.1:18080"}}}}}
	if err := c.ApplyDefaultsAndValidate(); err == nil {
		t.Fatal("check-config accepted an outbound Bearer token that inbound credential validation rejects")
	}
}
