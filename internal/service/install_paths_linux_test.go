package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A symlinked executable is installed by its link path.
func TestInstallKeepsSymlinkedExecutablePath(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "v1", "dbbridge")
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("binary"), 0755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "dbbridge")
	if err := os.Symlink(target, link); err != nil {
		t.Skip(err)
	}
	path := filepath.Join(dir, "peer.yaml")
	if err := os.WriteFile(path, []byte("node: {id: A}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	p, err := Prepare(Options{Name: "x", Config: path, Executable: link, LogLevel: "info", LogFormat: "text", User: "nobody"})
	if err != nil {
		t.Fatal(err)
	}
	if p.Executable != link {
		t.Fatalf("executable %s, want %s", p.Executable, link)
	}
	found := false
	for _, w := range p.Warnings {
		found = found || strings.Contains(w, target)
	}
	if !found {
		t.Fatalf("no symlink warning: %q", p.Warnings)
	}
}

func TestUninstallWaitFollowsUnitStopTimeout(t *testing.T) {
	if d := unitStopWait("[Service]\nTimeoutStopSec=330s\n"); d != 360*time.Second {
		t.Fatalf("wait %v", d)
	}
	if d := unitStopWait("[Service]\nTimeoutStopSec=60s\n"); d != 2*time.Minute {
		t.Fatalf("minimum wait %v", d)
	}
}
