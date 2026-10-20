//go:build !windows

package main

import (
	"os"
	"syscall"
)

// forceExitSignals are the signals that abort a drain.
var forceExitSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}
