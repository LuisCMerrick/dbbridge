package fileutil

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// WriteNew never follows or replaces an existing file, including a symlink.
// The file and its directory entry are flushed before it returns.
func WriteNew(path string, data []byte, mode fs.FileMode) (err error) {
	f, err := openNewFile(path, mode)
	if err != nil {
		return err
	}
	if err = writeNewFile(f, data); err != nil {
		return err
	}
	return SyncDir(filepath.Dir(path))
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
