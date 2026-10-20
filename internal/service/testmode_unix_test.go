//go:build !windows

package service

import "os"

func runtimeFileModeCheck(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Mode().Perm() != 0600 {
		return os.ErrPermission
	}
	return nil
}
