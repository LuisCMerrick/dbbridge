package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
	"unsafe"

	"dbbridge/internal/fileutil"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/eventlog"
	"golang.org/x/sys/windows/svc/mgr"
)

func install(ctx context.Context, p Plan) (err error) {
	account, err := windowsAccount(p.Account)
	if err != nil {
		return err
	}
	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("connect to SCM (run as Administrator): %w", err)
	}
	defer m.Disconnect()
	if existing, e := m.OpenService(p.Name); e == nil {
		existing.Close()
		return errors.New("service already exists; uninstall it before reinstalling")
	} else if !errors.Is(e, windows.ERROR_SERVICE_DOES_NOT_EXIST) {
		return e
	}
	if err = checkWindowsReadPaths(p); err != nil {
		return err
	}
	createdEnv, err := saveEnvironment(p)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil && createdEnv {
			_ = os.Remove(p.EnvironmentFile)
		}
	}()
	s, err := m.CreateService(p.Name, p.Executable, mgr.Config{DisplayName: "DBBridge (" + p.Name + ")", Description: marker + ": peer node", StartType: mgr.StartAutomatic, ErrorControl: mgr.ErrorNormal, ServiceStartName: account, Dependencies: []string{"Tcpip"}}, p.Arguments...)
	if err != nil {
		return err
	}
	defer s.Close()
	registered := false
	defer func() {
		if !registered {
			_ = s.Delete()
		}
	}()
	if err = eventlog.InstallAsEventCreate(p.Name, eventlog.Error|eventlog.Warning|eventlog.Info); err != nil {
		// No service of this name exists (checked above), so a source of the
		// same name is a leftover of an earlier failed install.
		if eventlog.Remove(p.Name) != nil {
			return fmt.Errorf("event log source: %w", err)
		}
		if err = eventlog.InstallAsEventCreate(p.Name, eventlog.Error|eventlog.Warning|eventlog.Info); err != nil {
			return fmt.Errorf("event log source: %w", err)
		}
	}
	defer func() {
		if !registered {
			_ = eventlog.Remove(p.Name)
		}
	}()
	// The restart action applies when the process ends without reporting
	// SERVICE_STOPPED (a crash, or a runtime failure, which dbbridge turns
	// into one). A configuration error is reported as SERVICE_STOPPED with
	// service-specific exit code 78; with non-crash failure actions off it
	// is not restarted, because a restart cannot fix the configuration.
	if err = s.SetRecoveryActions([]mgr.RecoveryAction{{Type: mgr.ServiceRestart, Delay: 5 * time.Second}}, 86400); err != nil {
		return err
	}
	if err = s.SetRecoveryActionsOnNonCrashFailures(false); err != nil {
		return err
	}
	// Preshutdown lets the node drain sessions before the normal OS shutdown
	// window, which can be shorter than transport.shutdown_timeout.
	timeout := uint64((p.ShutdownTimeout + 15*time.Second) / time.Millisecond)
	if timeout > uint64(^uint32(0)) {
		timeout = uint64(^uint32(0))
	}
	info := struct{ Milliseconds uint32 }{uint32(timeout)}
	if err = windows.ChangeServiceConfig2(s.Handle, windows.SERVICE_CONFIG_PRESHUTDOWN_INFO, (*byte)(unsafe.Pointer(&info))); err != nil {
		return err
	}
	registered = true
	createdEnv = false
	if p.Start {
		if err = s.Start(); err != nil {
			return fmt.Errorf("service installed, but start failed: %w", err)
		}
		if err = waitWindowsState(ctx, s, svc.Running, 30*time.Second); err != nil {
			return fmt.Errorf("service installed, but did not start; check Windows Application event log: %w", err)
		}
	}
	return nil
}
func waitWindowsState(parent context.Context, s *mgr.Service, want svc.State, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		status, err := s.Query()
		if err != nil {
			return err
		}
		if status.State == want {
			return nil
		}
		if want == svc.Running && status.State == svc.Stopped {
			return fmt.Errorf("service stopped, win32 exit=%d service exit=%d", status.Win32ExitCode, status.ServiceSpecificExitCode)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
func uninstall(ctx context.Context, name string) error {
	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("connect to SCM (run as Administrator): %w", err)
	}
	defer m.Disconnect()
	s, err := m.OpenService(name)
	if err != nil {
		return err
	}
	defer s.Close()
	cfg, err := s.Config()
	if err != nil {
		return err
	}
	if cfg.Description != marker+": peer node" {
		return errors.New("refusing to remove a service not installed by DBBridge")
	}
	status, err := s.Query()
	if err != nil {
		return err
	}
	if status.State != svc.Stopped {
		if _, err = s.Control(svc.Stop); err != nil {
			return err
		}
		if err = waitWindowsState(ctx, s, svc.Stopped, stopWait(s)); err != nil {
			return err
		}
	}
	if err = s.Delete(); err != nil {
		return err
	}
	if err = eventlog.Remove(name); err != nil {
		return fmt.Errorf("service removed, but event log source cleanup failed: %w", err)
	}
	return nil
}

// checkWindowsReadPaths verifies from the DACLs that the service account can
// read the configuration, credential, token and TLS files.
func checkWindowsReadPaths(p Plan) error {
	sid, err := environmentReader(p.Account)
	if err != nil {
		return err
	}
	for _, path := range p.readPaths {
		if err := fileutil.CheckReadAccessForSID(path, sid); err != nil && !errors.Is(err, fileutil.ErrNullDACL) {
			return fmt.Errorf("service account %s cannot be confirmed to read %s: %w; grant it with: icacls %q /grant \"%s:R\"", p.Account, path, err, path, accountDisplay(p.Account))
		}
	}
	return nil
}

func accountDisplay(account string) string {
	name, err := windowsAccount(account)
	if err != nil || name == "" {
		return `NT AUTHORITY\SYSTEM`
	}
	return name
}

// checkUninstall performs the read-only checks of uninstall.
func checkUninstall(_ context.Context, name string) error {
	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("connect to SCM (run as Administrator): %w", err)
	}
	defer m.Disconnect()
	s, err := m.OpenService(name)
	if err != nil {
		return fmt.Errorf("service %s: %w", name, err)
	}
	defer s.Close()
	cfg, err := s.Config()
	if err != nil {
		return err
	}
	if cfg.Description != marker+": peer node" {
		return errors.New("refusing to remove a service not installed by DBBridge")
	}
	return nil
}

// stopWait is how long uninstall waits for the service to stop: the
// preshutdown timeout recorded at installation (shutdown_timeout plus a
// margin) plus 30 seconds, and at least two minutes.
func stopWait(s *mgr.Service) time.Duration {
	wait := 2 * time.Minute
	var info struct{ Milliseconds uint32 }
	var needed uint32
	if err := windows.QueryServiceConfig2(s.Handle, windows.SERVICE_CONFIG_PRESHUTDOWN_INFO, (*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)), &needed); err == nil {
		if d := time.Duration(info.Milliseconds)*time.Millisecond + 30*time.Second; d > wait {
			wait = d
		}
	}
	return wait
}
