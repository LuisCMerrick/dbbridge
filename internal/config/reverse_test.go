package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// publicSide is a public-side (ingress) node with one reverse peer and,
// optionally, a forward peer beside it.
func publicSide(peerExtra, extra string) string {
	return fmt.Sprintf(`node: {id: hq-b}
http: {enabled: true, listen: "127.0.0.1:0"}
auth:
  inbound_peers: [{id: plant, token_envs: [DBB_TEST_TOKEN], allowed_targets: [db]}]
targets: {db: {address: "127.0.0.1:3306"}}
peers:
  - id: plant
    mode: reverse
    expected_node_id: plant-a
    credential_ref: plant
    token_env: DBB_TEST_TOKEN
%s
listeners: [{id: l, listen: "127.0.0.1:0", peer: plant, target: db}]
%s`, peerExtra, extra)
}

// dbSide is a database-side (egress) node behind NAT: no http listener, a
// reverse section and targets.
func dbSide(reverse, extra string) string {
	return fmt.Sprintf(`node: {id: plant-a}
auth:
  inbound_peers: [{id: hq, token_envs: [DBB_TEST_TOKEN], allowed_targets: [db]}]
targets: {db: {address: "127.0.0.1:3306"}}
reverse:
  enabled: true
%s
%s`, reverse, extra)
}

const dbSideBase = `  peer_node_id: hq-b
  token_env: DBB_TEST_TOKEN
  endpoints: [{id: primary, url: "https://hq.example.com:8443", priority: 1}]`

func wantCode(t *testing.T, err error, code, substr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s error containing %q, got nil", code, substr)
	}
	if got := ErrorCode(err); got != code {
		t.Fatalf("code = %q, want %q (err: %v)", got, code, err)
	}
	if !strings.Contains(err.Error(), substr) {
		t.Fatalf("error %q does not contain %q", err, substr)
	}
}

func hasReverseWarning(c *Config, substr string) bool {
	for _, w := range c.Warnings {
		if strings.Contains(w, substr) {
			return true
		}
	}
	return false
}

func TestPeerModeDefaultsToForward(t *testing.T) {
	c, err := testLoad(t, testPeerConfig("http://127.0.0.1:18080", "", ""))
	if err != nil {
		t.Fatal(err)
	}
	if c.Peers[0].Mode != PeerModeForward || c.Peers[0].IsReverse() || c.HasReversePeers() {
		t.Fatalf("mode = %q, want forward", c.Peers[0].Mode)
	}
}

func TestPeerModeValidation(t *testing.T) {
	for _, tc := range []struct {
		name, body, code, substr string
	}{
		{"explicit forward", strings.Replace(testPeerConfig("http://127.0.0.1:18080", "", ""), "expected_node_id: B", "expected_node_id: B\n    mode: forward", 1), "", ""},
		{"invalid", strings.Replace(testPeerConfig("http://127.0.0.1:18080", "", ""), "expected_node_id: B", "expected_node_id: B\n    mode: sideways", 1), CodePeerMode, `invalid peers[B].mode: "sideways"`},
		{"reverse with endpoints", publicSide(`    endpoints: [{id: e, url: "http://127.0.0.1:18081"}]`, ""), CodePeerMode, "cannot have endpoints"},
		{"reverse without http", strings.Replace(publicSide("", ""), "http: {enabled: true, listen: \"127.0.0.1:0\"}", "", 1), CodePeerMode, "needs http.enabled: true"},
		{"forward without endpoints", strings.Replace(publicSide("", ""), "mode: reverse", "mode: forward", 1), CodeConfigInvalid, "peers[plant].endpoints cannot be empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := testLoad(t, tc.body)
			if tc.code == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			wantCode(t, err, tc.code, tc.substr)
		})
	}
}

func TestReversePeerAccepted(t *testing.T) {
	c, err := testLoad(t, publicSide("", ""))
	if err != nil {
		t.Fatal(err)
	}
	p := c.Peers[0]
	if !p.IsReverse() || !c.HasReversePeers() || p.Token != testToken || len(p.Endpoints) != 1 || p.Endpoints[0].URL != ReverseEndpointURL {
		t.Fatalf("reverse peer not resolved: %+v", p)
	}
	if hasReverseWarning(c, "ignored for a reverse peer") {
		t.Fatalf("unexpected warning: %v", c.Warnings)
	}
}

func TestReversePeerTLSIgnoredWarning(t *testing.T) {
	c, err := testLoad(t, publicSide("    tls: {verify: true}", ""))
	if err != nil {
		t.Fatal(err)
	}
	if !hasReverseWarning(c, "ignored for a reverse peer") {
		t.Fatalf("missing warning: %v", c.Warnings)
	}
}

// A node may hold forward and reverse peers at the same time.
func TestMixedForwardAndReversePeers(t *testing.T) {
	body := publicSide("", "") + `
`
	body = strings.Replace(body, "listeners:", `  - id: branch
    expected_node_id: branch-c
    credential_ref: branch
    token_env: DBB_TEST_TOKEN
    endpoints: [{id: e, url: "http://127.0.0.1:18085"}]
listeners:`, 1)
	c, err := testLoad(t, body)
	if err != nil {
		t.Fatal(err)
	}
	if !c.Peers[0].IsReverse() || c.Peers[1].IsReverse() || c.Peers[1].Mode != PeerModeForward {
		t.Fatalf("modes = %q, %q", c.Peers[0].Mode, c.Peers[1].Mode)
	}
}

func TestReverseDefaults(t *testing.T) {
	c, err := testLoad(t, dbSide(dbSideBase, ""))
	if err != nil {
		t.Fatal(err)
	}
	r := c.Reverse
	if r.Transport != TunnelTransportAuto || r.TunnelsPerEndpoint != DefaultTunnelsPerEndpoint || r.Reconnect.MinBackoff != DefaultReverseMinBackoff || r.Reconnect.MaxBackoff != DefaultReverseMaxBackoff {
		t.Fatalf("defaults not applied: %+v", r)
	}
	if r.Token != testToken {
		t.Fatal("reverse token not resolved")
	}
	if r.Endpoints[0].URL != "https://hq.example.com:8443" {
		t.Fatalf("endpoint url = %q", r.Endpoints[0].URL)
	}
	if !c.EgressRole() || c.HTTP.Enabled {
		t.Fatal("reverse node must have the egress role without http.enabled")
	}
	// Targets and inbound identities serve the tunnel: no "unused" warning.
	if hasReverseWarning(c, "http.enabled is false") {
		t.Fatalf("unexpected warning: %v", c.Warnings)
	}
}

func TestReverseExplicitValues(t *testing.T) {
	c, err := testLoad(t, dbSide(dbSideBase+`
  transport: long-poll
  tunnels_per_endpoint: 4
  reconnect: {min_backoff: 2s, max_backoff: 1m}`, ""))
	if err != nil {
		t.Fatal(err)
	}
	r := c.Reverse
	if r.Transport != TunnelTransportLongPoll || r.TunnelsPerEndpoint != 4 || r.Reconnect.MinBackoff != 2*time.Second || r.Reconnect.MaxBackoff != time.Minute {
		t.Fatalf("values not kept: %+v", r)
	}
}

// An unset max_backoff follows an explicit min_backoff above its default.
func TestReverseMaxBackoffFollowsMin(t *testing.T) {
	c, err := testLoad(t, dbSide(dbSideBase+`
  reconnect: {min_backoff: 45s}`, ""))
	if err != nil {
		t.Fatal(err)
	}
	if c.Reverse.Reconnect.MaxBackoff != 45*time.Second {
		t.Fatalf("max_backoff = %v", c.Reverse.Reconnect.MaxBackoff)
	}
}

func TestReverseValidation(t *testing.T) {
	ep := `  endpoints: [{id: primary, url: "https://hq.example.com:8443", priority: 1}]`
	for _, tc := range []struct {
		name, reverse, extra, code, substr string
	}{
		{"no peer node", "  token_env: DBB_TEST_TOKEN\n" + ep, "", CodeReverse, "reverse.peer_node_id is required"},
		{"peer node is self", "  peer_node_id: plant-a\n  token_env: DBB_TEST_TOKEN\n" + ep, "", CodeReverse, "must differ"},
		{"no endpoints", "  peer_node_id: hq-b\n  token_env: DBB_TEST_TOKEN", "", CodeReverse, "reverse.endpoints cannot be empty"},
		{"no token", "  peer_node_id: hq-b\n" + ep, "", CodeReverse, "reverse needs a token"},
		{"two token sources", dbSideBase + "\n  token_file: plant.token", "", CodeReverse, "ambiguous credential source"},
		{"bad transport", dbSideBase + "\n  transport: carrier-pigeon", "", CodeReverse, `invalid reverse.transport: "carrier-pigeon"`},
		{"too many tunnels", dbSideBase + "\n  tunnels_per_endpoint: 17", "", CodeReverse, "tunnels_per_endpoint must be 1-16"},
		{"plaintext remote", "  peer_node_id: hq-b\n  token_env: DBB_TEST_TOKEN\n  endpoints: [{id: p, url: \"http://hq.example.com:8080\"}]", "", CodeReverse, "set reverse.allow_insecure_http: true"},
		{"endpoint path", "  peer_node_id: hq-b\n  token_env: DBB_TEST_TOKEN\n  endpoints: [{id: p, url: \"https://hq.example.com/x\"}]", "", CodeReverse, "reverse.endpoints[p].url must be an origin"},
		{"duplicate endpoint", "  peer_node_id: hq-b\n  token_env: DBB_TEST_TOKEN\n  endpoints: [{id: p, url: \"https://a.example.com\"}, {id: p, url: \"https://b.example.com\"}]", "", CodeReverse, `duplicate endpoint id "p" in reverse`},
		{"skip verify remote", dbSideBase + "\n  tls: {verify: false}", "", CodeReverse, "reverse.tls.verify: false for non-loopback endpoint primary"},
		{"zero min backoff", dbSideBase + "\n  reconnect: {min_backoff: 0s}", "", CodeReverse, "min_backoff must be > 0"},
		{"max below min", dbSideBase + "\n  reconnect: {min_backoff: 5s, max_backoff: 1s}", "", CodeReverse, "max_backoff (1s) must be >= min_backoff (5s)"},
		{"max too large", dbSideBase + "\n  reconnect: {max_backoff: 11m}", "", CodeReverse, "must be <= 10m0s"},
		{"bad duration", dbSideBase + "\n  reconnect: {min_backoff: soon}", "", CodeReverse, "invalid reverse.reconnect.min_backoff"},
		{"missing ca file", dbSideBase + "\n  tls: {ca_file: missing-ca.pem}", "", CodeTLS, "reverse.tls.ca_file"},
		{"missing token file", "  peer_node_id: hq-b\n  token_file: missing.token\n" + ep, "", CodeCredential, "reverse credential"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := testLoad(t, dbSide(tc.reverse, tc.extra))
			wantCode(t, err, tc.code, tc.substr)
		})
	}
}

func TestReverseInsecureOptIns(t *testing.T) {
	for _, reverse := range []string{
		"  peer_node_id: hq-b\n  token_env: DBB_TEST_TOKEN\n  allow_insecure_http: true\n  endpoints: [{id: p, url: \"http://hq.example.com:8080\"}]",
		"  peer_node_id: hq-b\n  token_env: DBB_TEST_TOKEN\n  endpoints: [{id: p, url: \"http://127.0.0.1:18080\"}]",
		dbSideBase + "\n  tls: {verify: false, allow_insecure_skip_verify: true}",
	} {
		if _, err := testLoad(t, dbSide(reverse, "")); err != nil {
			t.Fatalf("%s: %v", reverse, err)
		}
	}
}

func TestReverseSchema(t *testing.T) {
	for _, tc := range []struct{ name, reverse, substr string }{
		{"zero tunnels", dbSideBase + "\n  tunnels_per_endpoint: 0", "a positive integer"},
		{"unknown key", dbSideBase + "\n  tunnels: 2", `field "tunnels" not found in config.reverse`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := testLoad(t, dbSide(tc.reverse, ""))
			if tc.substr == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			wantCode(t, err, CodeConfigRead, tc.substr)
		})
	}
	_, err := testLoad(t, strings.Replace(dbSide(dbSideBase, ""), "enabled: true", `enabled: "yes"`, 1))
	wantCode(t, err, CodeConfigRead, "must be a boolean")
	_, err = testLoad(t, strings.Replace(publicSide("", ""), "mode: reverse", "mode: 1", 1))
	wantCode(t, err, CodeConfigRead, "must be a string")
}

func TestReverseDisabledSettingsWarn(t *testing.T) {
	body := strings.Replace(dbSide(dbSideBase, ""), "enabled: true", "enabled: false", 1)
	c, err := testLoad(t, body)
	if err != nil {
		t.Fatal(err)
	}
	if !hasReverseWarning(c, "reverse.enabled is false") || c.EgressRole() {
		t.Fatalf("warnings = %v", c.Warnings)
	}
	if c.Reverse.Token != "" {
		t.Fatal("disabled reverse section resolved its token")
	}
}

func TestReverseWithoutTargetsWarns(t *testing.T) {
	body := strings.Replace(dbSide(dbSideBase, ""), "targets: {db: {address: \"127.0.0.1:3306\"}}", "", 1)
	body = strings.Replace(body, "  inbound_peers: [{id: hq, token_envs: [DBB_TEST_TOKEN], allowed_targets: [db]}]", "  reload_interval: 5s", 1)
	c, err := testLoad(t, body)
	if err != nil {
		t.Fatal(err)
	}
	if !hasReverseWarning(c, "no targets are configured") {
		t.Fatalf("warnings = %v", c.Warnings)
	}
}

func TestReverseGraceWarning(t *testing.T) {
	short := dbSide(dbSideBase+"\n  reconnect: {min_backoff: 10s}", "transport:\n  recovery: {enabled: true, grace: 12s, barrier_timeout: 3s}\n")
	c, err := testLoad(t, short)
	if err != nil {
		t.Fatal(err)
	}
	if !hasReverseWarning(c, "a tunnel that fails its first reconnect cannot come back") {
		t.Fatalf("missing grace warning: %v", c.Warnings)
	}
	c, err = testLoad(t, dbSide(dbSideBase, "transport:\n  recovery: {enabled: true, grace: 60s}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if hasReverseWarning(c, "a tunnel that fails its first reconnect") {
		t.Fatalf("unexpected grace warning: %v", c.Warnings)
	}
}

// Offline validation checks that a token source is named but reads nothing.
func TestReverseOffline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plant.yaml")
	body := dbSide("  peer_node_id: hq-b\n  token_file: /nonexistent/plant.token\n  endpoints: [{id: p, url: \"https://hq.example.com\"}]", "")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DBB_TEST_TOKEN", testToken)
	c, err := LoadConfigWithOptions(path, LoadOptions{Offline: true})
	if err != nil {
		t.Fatal(err)
	}
	if c.Reverse.Token != "" {
		t.Fatal("offline validation read the reverse token")
	}
	if err := os.WriteFile(path, []byte(dbSide("  peer_node_id: hq-b\n  endpoints: [{id: p, url: \"https://hq.example.com\"}]", "")), 0600); err != nil {
		t.Fatal(err)
	}
	_, err = LoadConfigWithOptions(path, LoadOptions{Offline: true})
	wantCode(t, err, CodeReverse, "reverse needs a token")
}

func TestProtocolVersion15Only(t *testing.T) {
	if ProtocolVersion != "1.5" {
		t.Fatalf("ProtocolVersion = %q", ProtocolVersion)
	}
	c, err := testLoad(t, testPeerConfig("http://127.0.0.1:18080", "", ""))
	if err != nil {
		t.Fatal(err)
	}
	if c.Transport.ProtocolVersion != "1.5" {
		t.Fatalf("default protocol_version = %q", c.Transport.ProtocolVersion)
	}
	_, err = testLoad(t, testPeerConfig("http://127.0.0.1:18080", "", "transport: {protocol_version: \"1.4\"}"))
	wantCode(t, err, CodeConfigInvalid, `protocol_version must be "1.5", got "1.4"`)
}

// Every new code is distinct from the existing ones.
func TestReverseErrorCodesDistinct(t *testing.T) {
	seen := map[string]bool{}
	for _, code := range []string{CodeConfigRead, CodeConfigInvalid, CodeCredential, CodeTLS, CodeListen, CodeEnvironment, CodePeerMode, CodeReverse, CodeStartFailed, CodeFatal, CodeReloadRejected, CodeServiceInstall} {
		if seen[code] {
			t.Fatalf("duplicate code %s", code)
		}
		seen[code] = true
	}
	if CodePeerMode != "DBB-CFG-007" || CodeReverse != "DBB-CFG-008" {
		t.Fatal("reverse error codes changed")
	}
}
