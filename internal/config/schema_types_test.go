package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestAudit20261018RejectNullAndWrongScalarTypes(t *testing.T) {
	for _, body := range []string{
		"node: {id: A}\ntransport: {recovery: {enabled: null}}\n",
		"node: {id: A}\nhttp: {allowed_transports: null}\n",
		"node: {id: 123}\n",
		"node: {id: A}\nauth: {reload_interval: -1s}\n",
		"node: {id: true}\n",
		"node: {id: A}\nhttp: {enabled: 'true'}\n",
		"node: {id: A}\nhttp: []\n",
		"node: {id: A}\npeers: {}\n",
		"node: {id: A}\ntargets: {db: null}\n",
		"node: {id: A}\ntransport: {frame_size: true}\n",
		"node: {id: A}\ntransport: {heartbeat: false}\n",
		"node: {id: A}\ntransport: {ws: null}\n",
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadConfig(path)
		if err == nil {
			t.Errorf("invalid schema accepted: %q => node=%q recovery=%v transports=%v reload=%s", body, cfg.Node.ID, cfg.Transport.Recovery.Enabled, cfg.HTTP.AllowedTransports, cfg.Auth.ReloadInterval)
		}
	}
}

func TestMainConfigLimitBeforeAllocation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oversized-config.yaml")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(16 << 20); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err = LoadConfig(path)
	runtime.ReadMemStats(&after)
	if err == nil || !strings.Contains(err.Error(), "too large") || after.TotalAlloc-before.TotalAlloc > 4<<20 {
		t.Fatalf("config size guard: allocated=%d error=%v", after.TotalAlloc-before.TotalAlloc, err)
	}
}

func TestAudit20261018CredentialLimitBeforeAllocation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oversized-auth.yaml")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(16 << 20); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, _, err = ReadCredentials(path)
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("credentials ceiling=1048576 file_bytes=16777216 temporary_allocated=%d error=%v", allocated, err)
	if err == nil {
		t.Fatal("fixture must reject oversized file")
	}
	if allocated > 4<<20 {
		t.Error("oversized credential file was allocated before applying its 1 MiB limit")
	}
}
