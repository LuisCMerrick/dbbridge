//go:build !windows

package fileutil

import (
	"io/fs"
	"os"
)

func openNewFile(path string, mode fs.FileMode) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
}

// SyncDir flushes directory metadata so that a completed create or rename
// survives a crash.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
