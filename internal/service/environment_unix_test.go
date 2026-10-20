//go:build !windows

package service

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A snapshot readable by group/other is refused.
func TestLoadEnvironmentChecksMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "env.json")
	if err := os.WriteFile(path, []byte(`{"DBB_TEST_ENV":"value-0123456789"}`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if err := LoadEnvironment(path); err == nil || !strings.Contains(err.Error(), "mode") {
		t.Fatalf("0644 snapshot accepted: %v", err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DBB_TEST_ENV", "")
	os.Unsetenv("DBB_TEST_ENV")
	if err := LoadEnvironment(path); err != nil || os.Getenv("DBB_TEST_ENV") != "value-0123456789" {
		t.Fatalf("private snapshot: %v", err)
	}
}

// SIGHUP does not terminate the process; it triggers a credential reload.
func TestHangupTriggersReloadWithoutTerminating(t *testing.T) {
	stop := handleHangup()
	defer stop()
	reloaded := make(chan struct{}, 1)
	unregister := OnReload(func() { reloaded <- struct{}{} })
	defer unregister()
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reloaded:
	case <-time.After(2 * time.Second):
		t.Fatal("SIGHUP did not request a reload")
	}
}

// ProtectHome considers symlink targets.
func TestProtectHomeFollowsSymlinks(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil || !strings.HasPrefix(home+"/", "/home/") {
		t.Skip("home directory is not under /home")
	}
	target, err := os.MkdirTemp(home, ".dbbridge-audit-")
	if err != nil {
		t.Skip(err)
	}
	defer os.RemoveAll(target)
	file := filepath.Join(target, "shared.token")
	if err = os.WriteFile(file, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "shared.token")
	if err = os.Symlink(file, link); err != nil {
		t.Fatal(err)
	}
	p := Plan{Name: "dbbridge-test", Account: "dbbridge", Executable: "/opt/dbbridge/dbbridge", WorkingDirectory: "/etc/dbbridge", Arguments: []string{"run"}, readPaths: []string{"/etc/dbbridge/peer.yaml", link}}
	unit, err := systemdUnit(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(unit, "\nProtectHome=read-only\n") {
		t.Fatal("symlink into /home ignored by ProtectHome")
	}
}
