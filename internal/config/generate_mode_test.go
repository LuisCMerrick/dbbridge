package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// Regenerating with --force keeps the permissions an administrator
// gave the existing configuration (for example 0640 for a service group).
func TestForceKeepsConfigMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mode bits are not used on Windows")
	}
	o := DefaultGeneratePairOptions()
	o.OutputDir = t.TempDir()
	if _, err := GeneratePair(o); err != nil {
		t.Fatal(err)
	}
	a := filepath.Join(o.OutputDir, "peer-a.yaml")
	if err := os.Chmod(a, 0640); err != nil {
		t.Fatal(err)
	}
	o.Force = true
	if _, err := GeneratePair(o); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(a); err != nil || info.Mode().Perm() != 0640 {
		t.Fatalf("pair --force reset the mode: %v %v", info.Mode(), err)
	}
	if info, err := os.Stat(filepath.Join(o.OutputDir, "peer-b.yaml")); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("untouched file mode changed: %v %v", info.Mode(), err)
	}

	single := generateOptions(t, "egress")
	if _, err := Generate(single); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(single.Output, 0640); err != nil {
		t.Fatal(err)
	}
	single.Force = true
	if _, err := Generate(single); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(single.Output); err != nil || info.Mode().Perm() != 0640 {
		t.Fatalf("Generate --force reset the mode: %v %v", info.Mode(), err)
	}
}
