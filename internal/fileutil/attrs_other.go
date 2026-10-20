//go:build !windows

package fileutil

import (
	"os"
	"syscall"
)

// PreserveAttributes gives path the permission bits and, where allowed, the
// owner and group of the file it replaces (from), so that regenerating a
// configuration keeps the access an administrator granted to a service
// account. An ownership change the caller is not permitted to make is
// skipped; the mode is always applied.
func PreserveAttributes(from os.FileInfo, path string) error {
	if from == nil {
		return nil
	}
	if err := os.Chmod(path, from.Mode().Perm()); err != nil {
		return err
	}
	st, ok := from.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	if err := os.Lchown(path, int(st.Uid), int(st.Gid)); err != nil && !os.IsPermission(err) {
		return err
	}
	return nil
}
