package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/service"
)

// runnableNode is the part of *node.Node that serve uses.
type runnableNode interface {
	Start(context.Context) error
	Stop()
	Fatal() <-chan error
}

// serve runs a started node until ctx is cancelled (first SIGINT/SIGTERM or a
// service stop request) or the node reports a fatal background failure, then
// stops it. The second-signal force exit is installed before the drain starts
// and removed only after Stop returns. A fatal failure is returned so
// the process exits non-zero and the service manager restarts it.
func serve(ctx context.Context, n runnableNode, ready func() error, installForceExit func() (stop func())) error {
	if err := n.Start(ctx); err != nil {
		slog.Error("failed to start node", "code", config.CodeStartFailed, "err", err)
		n.Stop()
		return config.WithCode(config.CodeStartFailed, err)
	}
	if err := ready(); err != nil {
		n.Stop()
		return fmt.Errorf("service readiness notification: %w", err)
	}
	var runErr error
	select {
	case <-ctx.Done():
		slog.Info("received termination request, initiating graceful shutdown (send the signal again to force exit)")
	case err := <-n.Fatal():
		slog.Error("fatal background failure; stopping the node", "code", config.CodeFatal, "err", err)
		runErr = config.WithCode(config.CodeFatal, fmt.Errorf("fatal background failure: %w", err))
	}
	stopForce := installForceExit()
	service.NotifyStopping()
	n.Stop()
	stopForce()
	if runErr == nil {
		slog.Info("node exited cleanly")
	}
	return runErr
}

// forceExitOnSecondSignal lets an operator abort a graceful drain (bounded by
// transport.shutdown_timeout) with another Ctrl-C, or SIGTERM where it is a
// real termination request (not on Windows, where the runtime also maps
// CTRL_LOGOFF_EVENT to SIGTERM).
func forceExitOnSecondSignal(errOut io.Writer, exit func(int)) (stop func()) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, forceExitSignals...)
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		select {
		case <-signals:
			fmt.Fprintln(errOut, "second termination signal received; forcing exit")
			exit(130)
		case <-done:
		}
	}()
	return func() {
		signal.Stop(signals)
		close(done)
		<-finished
	}
}

// exitError carries a process exit status.
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }
func (e *exitError) Unwrap() error { return e.err }
func (e *exitError) ExitCode() int { return e.code }

// configError marks configuration and credential failures with EX_CONFIG so
// the service manager does not restart the service in a loop.
func configError(err error) error {
	if err == nil {
		return nil
	}
	return &exitError{code: service.ExitConfigError, err: err}
}

func exitCode(err error) int {
	var e *exitError
	if errors.As(err, &e) {
		return e.code
	}
	return 1
}

// resolveBuildInfo fills values not injected with -ldflags from the Go build
// information. The version never depends on the current date.
func resolveBuildInfo(version, commit, date string, read func() (*debug.BuildInfo, bool)) (string, string, string) {
	if version == "" {
		version = protocol.ProtocolVersionString + "-dev"
	}
	info, ok := read()
	if !ok || info == nil {
		return version, commit, date
	}
	modified, fromVCS := false, false
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			if commit == "unknown" && s.Value != "" {
				commit, fromVCS = s.Value[:min(12, len(s.Value))], true
			}
		case "vcs.time":
			if date == "unknown" && s.Value != "" {
				date = s.Value
			}
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}
	if modified && fromVCS {
		commit += "-dirty"
	}
	return version, commit, date
}
