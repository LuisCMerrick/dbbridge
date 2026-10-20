//go:build !windows

package main

import (
	"bytes"
	"syscall"
	"testing"
	"time"
)

// A second SIGTERM during the drain forces the exit.
func TestForceExitOnSecondSignal(t *testing.T) {
	codes := make(chan int, 1)
	var out bytes.Buffer
	stop := forceExitOnSecondSignal(&out, func(code int) { codes <- code })
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-codes:
		if code != 130 {
			t.Fatalf("exit code %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second signal did not force exit")
	}
	stop()
}
