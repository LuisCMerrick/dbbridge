package fileutil

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// WriteNew never follows or replaces an existing file, including a symlink.
// The file and its directory entry are flushed before it returns. created
// reports whether the file exists at path after the call because this call
// created it: an error with created true (a failed directory sync) leaves a
// complete file that the caller owns and must remove or keep explicitly.
func WriteNew(path string, data []byte, mode fs.FileMode) (created bool, err error) {
	f, err := openNewFile(path, mode)
	if err != nil {
		return false, err
	}
	if err = writeNewFile(f, data); err != nil {
		return false, err
	}
	return true, SyncDir(filepath.Dir(path))
}

func writeNewFile(f *os.File, data []byte) (err error) {
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
		}
	}()
	n, err := f.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	if err = f.Sync(); err != nil {
		return err
	}
	return f.Close()
}
