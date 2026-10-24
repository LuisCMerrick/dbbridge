package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dbbridge/internal/config"
)

// check-config reports the reverse-mode codes with exit status 78 and accepts
// a valid reverse configuration.
func TestCheckConfigReverseCodes(t *testing.T) {
	t.Setenv("DBB_CLI_REVERSE_TOKEN", "cli-reverse-token-0123456789abcdef")
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	valid := write("plant.yaml", `node: {id: plant-a}
auth:
  inbound_peers: [{id: hq, token_envs: [DBB_CLI_REVERSE_TOKEN], allowed_targets: [db]}]
targets: {db: {address: "127.0.0.1:3306"}}
reverse:
  enabled: true
  peer_node_id: hq-b
  token_env: DBB_CLI_REVERSE_TOKEN
  endpoints: [{id: primary, url: "https://hq.example.com:8443"}]
`)
	var out bytes.Buffer
	if err := execute([]string{"check-config", "-c", valid}, &out, &bytes.Buffer{}); err != nil {
		t.Fatalf("valid reverse config: %v", err)
	}
	if !strings.Contains(out.String(), "is valid") {
		t.Fatalf("output %q", out.String())
	}
	for name, tc := range map[string]struct{ body, code string }{
		"mode.yaml": {`node: {id: hq-b}
peers: [{id: plant, mode: reverse, expected_node_id: plant-a, credential_ref: plant, token_env: DBB_CLI_REVERSE_TOKEN}]
`, config.CodePeerMode},
		"reverse.yaml": {`node: {id: plant-a}
reverse: {enabled: true, peer_node_id: hq-b, token_env: DBB_CLI_REVERSE_TOKEN}
`, config.CodeReverse},
	} {
		err := execute([]string{"check-config", "-c", write(name, tc.body)}, &bytes.Buffer{}, &bytes.Buffer{})
		if exitCode(err) != 78 {
			t.Fatalf("%s: exit %d (%v)", name, exitCode(err), err)
		}
		var msg bytes.Buffer
		printError(&msg, err)
		if !strings.HasPrefix(msg.String(), "Error ["+tc.code+"]: ") {
			t.Fatalf("%s: %q", name, msg.String())
		}
	}
	for _, code := range []string{config.CodePeerMode, config.CodeReverse} {
		if !isConfigurationCode(code) {
			t.Fatalf("%s does not map to exit status 78", code)
		}
	}
}
