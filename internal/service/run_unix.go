//go:build !windows

package service

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func Run(_ string, _ string, out io.Writer, run Runner) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	// Once the first SIGINT/SIGTERM has cancelled ctx, stop intercepting so
	// that the default action (immediate termination) is the fallback for a
	// further signal when no force-exit handler is installed.
	go func() { <-ctx.Done(); cancel() }()
	stopHUP := handleHangup()
	defer stopHUP()
	return run(ctx, func() error { return notifyReady() }, out)
}

// handleHangup turns SIGHUP into an immediate credential reload instead of
// letting it kill the node without a drain. Credentials are also re-read
// every auth.reload_interval.
func handleHangup() (stop func()) {
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-hup:
				slog.Info("SIGHUP received; re-reading credentials (use SIGTERM to stop)")
				if !requestReload() {
					slog.Info("SIGHUP ignored; the node is not running yet")
				}
			case <-done:
				return
			}
		}
	}()
	return func() {
		signal.Stop(hup)
		close(done)
	}
}
