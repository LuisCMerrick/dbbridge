package fileutil

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteNewDoesNotReplaceExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret")
	if _, err := WriteNew(path, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	if created, err := WriteNew(path, []byte("replacement"), 0600); err == nil || created {
		t.Fatal("existing secret was replaced")
	}
	b, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(b, []byte("original")) {
		t.Fatal("existing secret changed")
	}
}

func TestSyncDir(t *testing.T) {
	if err := SyncDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err := SyncDir(filepath.Join(t.TempDir(), "missing")); err == nil && filepath.Separator == '/' {
		t.Fatal("missing directory synced")
	}
}
