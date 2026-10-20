package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFixCheckAccountAccess(t *testing.T) {
	// t.TempDir's parent is private to the test user; use a directory
	// directly under the world-traversable temp root instead.
	dir, err := os.MkdirTemp("", "dbbridge-access-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	private := filepath.Join(dir, "private.yaml")
	public := filepath.Join(dir, "public.yaml")
	if err := os.WriteFile(private, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(public, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	self, other := os.Getuid(), 65534
	if self == other {
		t.Skip("running as nobody")
	}
	none := map[int]bool{}
	if err := checkAccountAccess(private, self, none, 4); err != nil {
		t.Fatalf("owner denied: %v", err)
	}
	if err := checkAccountAccess(public, other, none, 4); err != nil {
		t.Fatalf("world-readable denied: %v", err)
	}
	if err := checkAccountAccess(private, other, none, 4); err == nil {
		t.Fatal("0600 file reported readable by another account")
	}
	// SVC-1: the unit drops CAP_DAC_OVERRIDE, so root gets no bypass. A
	// 0600 file owned by another uid is unreadable for root as well.
	if self != 0 {
		if err := checkAccountAccess(private, 0, map[int]bool{0: true}, 4); err == nil || !strings.Contains(err.Error(), "CAP_DAC_OVERRIDE") {
			t.Fatalf("root reported able to read another user's 0600 file: %v", err)
		}
	}
	if err := checkAccountAccess(public, 0, map[int]bool{0: true}, 4); err != nil {
		t.Fatalf("world-readable file denied to root: %v", err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := checkAccountAccess(public, other, none, 4); err == nil {
		t.Fatal("untraversable directory not detected")
	}
}
