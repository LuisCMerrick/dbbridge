//go:build !windows

package config

import (
	"fmt"
	"os"
)

// warnInsecurePermissions records a warning when a secret file (token,
// credentials file, TLS private key) is accessible to "other" users or
// writable by its group. Group read is accepted: it is the documented
// way to share files with a dedicated service account.
func (c *Config) warnInsecurePermissions(path string) {
	if w := insecurePermissionWarning(path); w != "" {
		c.Warnings = append(c.Warnings, w)
	}
}

func insecurePermissionWarning(path string) string {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return ""
	}
	if mode := info.Mode().Perm(); mode&0o027 != 0 {
		return fmt.Sprintf("secret file %s has mode %04o: other users can access it or its group can write it; use chmod 600 (or 640 with a service group)", path, mode)
	}
	return ""
}
