package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"dbbridge/internal/fileutil"
)

type systemdManager struct {
	dir     string
	command func(context.Context, ...string) error
}

func newSystemdManager() systemdManager {
	return systemdManager{dir: "/etc/systemd/system", command: func(parent context.Context, args ...string) error {
		// A caller that knows how long the command may legitimately take
		// (stopping a draining service) passes its own deadline.
		ctx, cancel := parent, context.CancelFunc(func() {})
		if _, ok := parent.Deadline(); !ok {
			ctx, cancel = context.WithTimeout(parent, 2*time.Minute)
		}
		defer cancel()
		out, err := exec.CommandContext(ctx, "systemctl", args...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("systemctl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
		}
		return nil
	}}
}
func systemdAvailable() error {
	if os.Geteuid() != 0 {
		return errors.New("systemd service installation requires root (use sudo)")
	}
	if info, err := os.Stat("/run/systemd/system"); err != nil || !info.IsDir() {
		return errors.New("systemd is not running as the system manager; use --dry-run to preview")
	}
	return nil
}
func install(ctx context.Context, p Plan) error {
	if err := systemdAvailable(); err != nil {
		return err
	}
	return newSystemdManager().install(ctx, p)
}
func (m systemdManager) install(ctx context.Context, p Plan) (err error) {
	path := filepath.Join(m.dir, p.Name+".service")
	if _, err = os.Lstat(path); err == nil {
		return fmt.Errorf("service already exists: %s; uninstall it before reinstalling", p.Name)
	} else if !os.IsNotExist(err) {
		return err
	}
	account, err := user.Lookup(p.Account)
	if _, numeric := strconv.ParseUint(p.Account, 10, 32); numeric == nil {
		account, err = user.LookupId(p.Account)
	}
	if err != nil {
		return fmt.Errorf("service account: %w", err)
	}
	uid, err := strconv.Atoi(account.Uid)
	if err != nil {
		return err
	}
	gid, err := strconv.Atoi(account.Gid)
	if err != nil {
		return err
	}
	groups := map[int]bool{gid: true}
	if ids, e := account.GroupIds(); e == nil {
		for _, id := range ids {
			if n, e := strconv.Atoi(id); e == nil {
				groups[n] = true
			}
		}
	}
	for _, path := range p.readPaths {
		if err = checkAccountAccess(path, uid, groups, 4); err != nil {
			return fmt.Errorf("service account %s: %w", p.Account, err)
		}
	}
	if err = checkAccountAccess(p.Executable, uid, groups, 1); err != nil {
		return fmt.Errorf("service account %s: %w", p.Account, err)
	}
	createdEnv, err := saveEnvironment(p)
	defer func() {
		if err != nil && createdEnv {
			_ = os.Remove(p.EnvironmentFile)
		}
	}()
	if err != nil {
		return err
	}
	if createdEnv {
		// Only a snapshot created by this installation is handed to the
		// service account; an existing one keeps its ownership.
		if err = os.Chown(p.EnvironmentFile, uid, gid); err != nil {
			return fmt.Errorf("service environment ownership: %w", err)
		}
	} else if p.EnvironmentFile != "" {
		if err = checkAccountAccess(p.EnvironmentFile, uid, groups, 4); err != nil {
			return fmt.Errorf("service account %s: %w", p.Account, err)
		}
	}
	if created, e := fileutil.WriteNew(path, []byte(p.Unit), 0644); e != nil {
		if created {
			_ = os.Remove(path)
		}
		return e
	}
	if err = m.command(ctx, "daemon-reload"); err != nil {
		_ = os.Remove(path)
		_ = m.command(context.WithoutCancel(ctx), "daemon-reload")
		return err
	}
	if err = m.command(ctx, "enable", p.Name+".service"); err != nil {
		_ = m.command(context.WithoutCancel(ctx), "disable", p.Name+".service")
		_ = os.Remove(path)
		_ = m.command(context.WithoutCancel(ctx), "daemon-reload")
		return err
	}
	// Registration succeeded. Preserve it and its credentials if an optional
	// start fails, so the administrator can inspect journalctl and retry.
	createdEnv = false
	if p.Start {
		if err = m.command(ctx, "start", p.Name+".service"); err != nil {
			return fmt.Errorf("service installed and enabled, but start failed: %w", err)
		}
	}
	return nil
}

// checkAccountAccess approximates access(2) for another account from mode
// bits: want is 4 (read) or 1 (execute) on the file, and every parent
// directory must be searchable. ACLs are not evaluated. uid 0 gets no bypass:
// the unit's empty CapabilityBoundingSet removes CAP_DAC_OVERRIDE and
// CAP_DAC_READ_SEARCH, so root is checked like any other uid.
func checkAccountAccess(path string, uid int, groups map[int]bool, want os.FileMode) error {
	allowed := func(p string, bit os.FileMode) (bool, error) {
		info, err := os.Stat(p)
		if err != nil {
			return false, err
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return true, nil
		}
		mode := info.Mode().Perm()
		switch {
		case int(st.Uid) == uid:
			return mode&(bit<<6) != 0, nil
		case groups[int(st.Gid)]:
			return mode&(bit<<3) != 0, nil
		default:
			return mode&bit != 0, nil
		}
	}
	for dir := filepath.Dir(path); ; dir = filepath.Dir(dir) {
		ok, err := allowed(dir, 1)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("cannot traverse directory %s needed for %s (the service account needs execute permission on it)", dir, path)
		}
		if dir == filepath.Dir(dir) {
			break
		}
	}
	ok, err := allowed(path, want)
	if err != nil {
		return err
	}
	if !ok {
		hint := "grant the account read access (or choose another --service-user)"
		if uid == 0 {
			hint = "the service runs as root without CAP_DAC_OVERRIDE, so root needs owner, group or other permission like any account; install with --service-user set to the files' owner or a dedicated account that can read them"
		}
		return fmt.Errorf("cannot access %s; %s", path, hint)
	}
	return nil
}

// checkUninstall performs the read-only checks of uninstall.
func checkUninstall(_ context.Context, name string) error {
	_, err := newSystemdManager().ownedUnit(name)
	return err
}

func (m systemdManager) ownedUnit(name string) (string, error) {
	path := filepath.Join(m.dir, name+".service")
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("service %s is not installed (%s not found)", name, path)
		}
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("refusing to remove a non-regular unit")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(string(b), "# "+marker+"\n") {
		return "", errors.New("refusing to remove a unit not installed by DBBridge")
	}
	return path, nil
}

func uninstall(ctx context.Context, name string) error {
	if err := systemdAvailable(); err != nil {
		return err
	}
	return newSystemdManager().uninstall(ctx, name)
}
func (m systemdManager) uninstall(ctx context.Context, name string) error {
	path, err := m.ownedUnit(name)
	if err != nil {
		return err
	}
	unit, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	stopCtx, cancel := context.WithTimeout(ctx, unitStopWait(string(unit)))
	defer cancel()
	if err = m.command(stopCtx, "disable", "--now", name+".service"); err != nil {
		return err
	}
	if err = os.Remove(path); err != nil {
		return err
	}
	return m.command(ctx, "daemon-reload")
}

// unitStopWait is how long uninstall waits for "systemctl disable --now":
// the unit's TimeoutStopSec (derived from shutdown_timeout at installation)
// plus 30 seconds, and at least two minutes.
func unitStopWait(unit string) time.Duration {
	wait := 2 * time.Minute
	for _, line := range strings.Split(unit, "\n") {
		value, ok := strings.CutPrefix(strings.TrimSpace(line), "TimeoutStopSec=")
		if !ok {
			continue
		}
		if seconds, err := strconv.ParseInt(strings.TrimSuffix(value, "s"), 10, 64); err == nil && seconds > 0 {
			if d := time.Duration(seconds)*time.Second + 30*time.Second; d > wait {
				wait = d
			}
		}
	}
	return wait
}
