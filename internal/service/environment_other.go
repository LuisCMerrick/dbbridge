//go:build !windows

package service

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"dbbridge/internal/fileutil"
)

func writeEnvironmentFile(p Plan, data []byte) error {
	return fileutil.WriteNew(p.EnvironmentFile, data, 0600)
}

func validateEnvironmentAccess(Plan) error { return nil }

// checkEnvironmentFile rejects a credential snapshot that other accounts can
// read or replace: it must be private (no group/other bits) and owned by the
// service account or root.
func checkEnvironmentFile(f *os.File) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("not a regular file")
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		return fmt.Errorf("mode %04o allows group/other access; use chmod 600", mode)
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok && int(st.Uid) != os.Geteuid() && st.Uid != 0 {
		return fmt.Errorf("owned by uid %d, not the service account (uid %d) or root", st.Uid, os.Geteuid())
	}
	return nil
}
