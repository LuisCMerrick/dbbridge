package config

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testToken = "test-test-token-0123456789abcdef"

// testPeerConfig returns a minimal valid ingress config with one peer whose
// fields can be overridden by the snippets.
func testPeerConfig(url, tls, extra string) string {
	return fmt.Sprintf(`node: {id: A}
peers:
  - id: B
    expected_node_id: B
    credential_ref: B
    token_env: DBB_TEST_TOKEN
    endpoints: [{id: e, url: %q, priority: 1}]
%s
%s`, url, tls, extra)
}

func testLoad(t *testing.T, body string) (*Config, error) {
	t.Helper()
	t.Setenv("DBB_TEST_TOKEN", testToken)
	path := filepath.Join(t.TempDir(), "peer.yaml")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return LoadConfig(path)
}

func TestFloatDurations(t *testing.T) {
	for _, tc := range []struct {
		in   float64
		want time.Duration
	}{{0.5, 500 * time.Millisecond}, {1.5, 1500 * time.Millisecond}, {10.9, 10900 * time.Millisecond}, {0, 0}} {
		got, err := parseDuration(tc.in, time.Hour)
		if err != nil || got != tc.want {
			t.Fatalf("parseDuration(%v) = %v, %v; want %v", tc.in, got, err, tc.want)
		}
	}
	for _, bad := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -0.5, 1e10, math.MaxFloat64} {
		if got, err := parseDuration(bad, time.Hour); err == nil {
			t.Fatalf("parseDuration(%v) accepted as %v", bad, got)
		}
	}
	cfg, err := testLoad(t, testPeerConfig("https://b.example.invalid", "", "transport: {heartbeat: 1.5, failure_detect_timeout: 4.5}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Transport.Heartbeat != 1500*time.Millisecond {
		t.Fatalf("heartbeat = %v", cfg.Transport.Heartbeat)
	}
	if _, err = testLoad(t, testPeerConfig("https://b.example.invalid", "", "transport: {heartbeat: .nan}\n")); err == nil {
		t.Fatal("NaN duration accepted")
	}
}

func TestReloadIntervalBounds(t *testing.T) {
	for _, tc := range []struct {
		value string
		ok    bool
		want  time.Duration
	}{{"", true, 5 * time.Second}, {"1s", true, time.Second}, {"999ms", false, 0}, {"1ns", false, 0}} {
		extra := ""
		if tc.value != "" {
			extra = "auth: {reload_interval: " + tc.value + "}\n"
		}
		cfg, err := testLoad(t, testPeerConfig("https://b.example.invalid", "", extra))
		if (err == nil) != tc.ok {
			t.Fatalf("reload_interval %q: err=%v", tc.value, err)
		}
		if tc.ok && cfg.Auth.ReloadInterval != tc.want {
			t.Fatalf("reload_interval %q = %v", tc.value, cfg.Auth.ReloadInterval)
		}
	}
}

func TestEndpointURLRules(t *testing.T) {
	for _, tc := range []struct {
		url string
		ok  bool
	}{
		{"https://b.example.invalid", true},
		{"https://b.example.invalid:8443", true},
		{"https://b.example.invalid:65535", true},
		{"https://b.example.invalid/", false},
		{"https://b.example.invalid:99999", false},
		{"https://b.example.invalid:0", false},
		{"https://b.example.invalid:", false},
		{"https://b.example.invalid/?", false},
		// Plaintext is allowed to every loopback form, and nothing else.
		{"http://127.0.0.2:18080", true},
		{"http://localhost:18080", true},
		{"http://[::1]:18080", true},
		{"http://10.0.0.1:18080", false},
	} {
		_, err := testLoad(t, testPeerConfig(tc.url, "", ""))
		if (err == nil) != tc.ok {
			t.Errorf("url %q: err=%v", tc.url, err)
		}
	}
}

func TestExpectedNodeIDMustDifferFromSelf(t *testing.T) {
	body := strings.Replace(testPeerConfig("https://b.example.invalid", "", ""), "expected_node_id: B", "expected_node_id: A", 1)
	if _, err := testLoad(t, body); err == nil || !strings.Contains(err.Error(), "expected_node_id") {
		t.Fatalf("self expected_node_id accepted: %v", err)
	}
}

func TestHTTPListenLoopbackConsistency(t *testing.T) {
	for _, tc := range []struct {
		listen string
		ok     bool
	}{{"localhost:18080", true}, {"127.0.0.2:18080", true}, {"[::1]:18080", true}, {"0.0.0.0:18080", false}, {"10.1.2.3:18080", false}} {
		body := fmt.Sprintf("node: {id: A}\nhttp: {enabled: true, listen: %q}\n", tc.listen)
		if _, err := testLoad(t, body); (err == nil) != tc.ok {
			t.Errorf("http.listen %q: err=%v", tc.listen, err)
		}
	}
}

func TestAdminListenRequiresOptInForRemote(t *testing.T) {
	for _, tc := range []struct {
		admin string
		ok    bool
	}{
		{"{enabled: true}", true},
		{"{enabled: true, listen: '127.0.0.1:9080'}", true},
		{"{enabled: true, listen: 'localhost:9080'}", true},
		{"{enabled: true, listen: '0.0.0.0:9080'}", false},
		{"{enabled: true, listen: '10.0.0.5:9080'}", false},
		{"{enabled: true, listen: '10.0.0.5:9080', allow_remote: true}", true},
	} {
		if _, err := testLoad(t, "node: {id: A}\nadmin: "+tc.admin+"\n"); (err == nil) != tc.ok {
			t.Errorf("admin %s: err=%v", tc.admin, err)
		}
	}
	cfg, err := testLoad(t, "node: {id: A}\nadmin: {enabled: true}\n")
	if err != nil || cfg.Admin.Listen != DefaultAdminListen {
		t.Fatalf("admin default = %q, %v", cfg.Admin.Listen, err)
	}
}

func TestAdminDefaultDoesNotCollideWithGeneratedPair(t *testing.T) {
	o := DefaultGeneratePairOptions()
	for _, listen := range []string{o.A.HTTPListen, o.B.HTTPListen, o.A.Listen, o.B.Listen} {
		if listen == DefaultAdminListen {
			t.Fatalf("admin default %s collides with generated listener", listen)
		}
	}
	// Generated nodes enable admin on distinct loopback ports.
	o.OutputDir = t.TempDir()
	files, err := GeneratePair(o)
	if err != nil {
		t.Fatal(err)
	}
	a, err := LoadConfig(files.ConfigA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadConfig(files.ConfigB)
	if err != nil {
		t.Fatal(err)
	}
	if !a.Admin.Enabled || !b.Admin.Enabled || a.Admin.Listen == b.Admin.Listen || a.Admin.Listen != DefaultAdminListen {
		t.Fatalf("admin: A %+v, B %+v", a.Admin, b.Admin)
	}
}

func TestTLSSkipVerifyRequiresOptIn(t *testing.T) {
	for _, tc := range []struct {
		url, tls string
		ok       bool
	}{
		{"https://b.example.invalid", "    tls: {verify: false}", false},
		{"https://b.example.invalid", "    tls: {verify: false, allow_insecure_skip_verify: true}", true},
		{"https://127.0.0.1:8443", "    tls: {verify: false}", true},
		{"https://localhost:8443", "    tls: {verify: false}", true},
		{"https://b.example.invalid", "    tls: {verify: true}", true},
	} {
		if _, err := testLoad(t, testPeerConfig(tc.url, tc.tls, "")); (err == nil) != tc.ok {
			t.Errorf("%s %s: err=%v", tc.url, tc.tls, err)
		}
	}
}

func TestTLSCAFile(t *testing.T) {
	cert, key := testCertificatePair(t)
	cfg, err := testLoad(t, testPeerConfig("https://b.example.invalid", "    tls: {ca_file: "+cert+"}", ""))
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(cfg.Peers[0].TLS.CAFile) || cfg.Peers[0].TLS.CAFile != cert {
		t.Fatalf("ca_file = %q", cfg.Peers[0].TLS.CAFile)
	}
	for _, tls := range []string{
		"    tls: {ca_file: " + key + "}",                                                   // no certificate
		"    tls: {ca_file: /definitely/missing/ca.pem}",                                    // unreadable
		"    tls: {ca_file: " + cert + ", verify: false, allow_insecure_skip_verify: true}", // contradictory
	} {
		if _, err := testLoad(t, testPeerConfig("https://b.example.invalid", tls, "")); err == nil {
			t.Errorf("accepted %s", tls)
		}
	}
}

func TestInboundQuotaPartitionsGlobalLimit(t *testing.T) {
	base := "node: {id: B}\ntargets: {db: {address: '127.0.0.1:3306', allowed_cidrs: ['127.0.0.1/32']}}\n"
	two := func(limits, a, b string) string {
		return base + limits + fmt.Sprintf(`auth:
  inbound_peers:
    - {id: A, token_envs: [DBB_TEST_TOKEN], allowed_targets: [db]%s}
    - {id: C, token_envs: [DBB_TEST_TOKEN_C], allowed_targets: [db]%s}
`, a, b)
	}
	t.Setenv("DBB_TEST_TOKEN_C", "test-test-token-c-0123456789abcdef")
	for _, tc := range []struct {
		body string
		ok   bool
	}{
		{two("", "", ""), false}, // 200 + 200 > 200: one peer could take every slot
		{two("", ", max_sessions: 200", ", max_sessions: 1"), false},
		{two("", ", max_sessions: 100", ", max_sessions: 100"), true},
		{two("limits: {max_sessions: 400}\ntransport: {metadata_limit: 64MiB}\n", "", ""), true},
		{two("", "", ", enabled: false"), true},
	} {
		if _, err := testLoad(t, tc.body); (err == nil) != tc.ok {
			t.Errorf("%s: err=%v", tc.body, err)
		}
	}
	// A single inbound peer may use the whole global limit.
	single := base + "auth:\n  inbound_peers:\n    - {id: A, token_envs: [DBB_TEST_TOKEN], allowed_targets: [db]}\n"
	if _, err := testLoad(t, single); err != nil {
		t.Fatal(err)
	}
}

func TestShortTokenWarning(t *testing.T) {
	cfg, err := testLoad(t, testPeerConfig("https://b.example.invalid", "", ""))
	if err != nil || len(cfg.Warnings) != 0 {
		t.Fatalf("warnings=%v err=%v", cfg.Warnings, err)
	}
	t.Setenv("DBB_TEST_SHORT", "short")
	body := strings.Replace(testPeerConfig("https://b.example.invalid", "", ""), "DBB_TEST_TOKEN", "DBB_TEST_SHORT", 1)
	path := filepath.Join(t.TempDir(), "peer.yaml")
	if err = os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadConfig(path)
	if err != nil || len(cfg.Warnings) != 1 || !strings.Contains(cfg.Warnings[0], "shorter") {
		t.Fatalf("warnings=%v err=%v", cfg.Warnings, err)
	}
}

func TestAliasExpansionIsLinear(t *testing.T) {
	const k = 6000
	var b strings.Builder
	b.WriteString("version: 1\ninbound_peers:\n  - &p {id: x, allowed_targets: &t [")
	for i := 0; i < k; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "t%d", i)
	}
	b.WriteString("]}\n")
	for i := 0; i < k; i++ {
		b.WriteString("  - *p\n")
	}
	start := time.Now()
	_ = validateSchema([]byte(b.String()), AuthConfigFile{})
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("alias validation took %v", elapsed)
	}
	// Aliases are still type checked.
	if err := validateSchema([]byte("node: &n {id: A}\nhttp: *n\n"), Config{}); err == nil {
		t.Fatal("aliased node with wrong fields accepted")
	}
	if err := validateSchema([]byte("node: {id: &i A}\nadmin: {listen: *i}\n"), Config{}); err != nil {
		t.Fatalf("valid alias rejected: %v", err)
	}
}

func TestSampleConfigsAreSafe(t *testing.T) {
	paths, err := filepath.Glob("../../configs/*.yaml")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no sample configs: %v", err)
	}
	for _, name := range []string{"DBB_TOKEN_TO_HQ", "DBB_TOKEN_FROM_PLANT_CURRENT", "DBB_TOKEN_FROM_PLANT_PREV"} {
		t.Setenv(name, "configuration-check-"+name)
	}
	for _, path := range paths {
		cfg, err := LoadConfig(path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		for _, p := range cfg.Peers {
			if p.AllowInsecureHTTP {
				t.Errorf("%s enables peers[%s].allow_insecure_http", path, p.ID)
			}
		}
		if cfg.HTTP.Enabled && !cfg.HTTP.TLS.Enabled && !strings.HasPrefix(cfg.HTTP.Listen, "127.") {
			t.Errorf("%s listens in plaintext on %s", path, cfg.HTTP.Listen)
		}
		for _, p := range cfg.Peers {
			if !p.TLS.IsVerify() {
				t.Errorf("%s disables TLS verification for %s", path, p.ID)
			}
		}
	}
	if _, err := os.Stat("../../configs/token.secret"); !os.IsNotExist(err) {
		t.Error("configs/token.secret is still present")
	}
}
