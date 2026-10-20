package config

import (
	"os"
	"strings"
	"testing"
)

// gen-config opts in to plaintext per peer instead of the legacy global key.
func TestGeneratedInsecureHTTPUsesPerPeerKey(t *testing.T) {
	o := DefaultGeneratePairOptions()
	o.OutputDir = t.TempDir()
	o.AllowInsecureHTTP = true
	o.A.PeerURLs = []string{"http://b.example.invalid:18081"}
	files, err := GeneratePair(o)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(files.ConfigA)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "allow_insecure_remote_http") || !strings.Contains(string(b), "allow_insecure_http: true") {
		t.Fatalf("generated config:\n%s", b)
	}
	cfg, err := LoadConfig(files.ConfigA)
	if err != nil || !cfg.Peers[0].AllowInsecureHTTP || cfg.Transport.AllowInsecureRemoteHTTP {
		t.Fatalf("%v %+v", err, cfg)
	}
}
