package service

import (
	"errors"
	"sync"
)

// ExitCoder is implemented by errors that carry a process exit status.
type ExitCoder interface{ ExitCode() int }

// ExitStatus returns the process exit status for the result of a Runner:
// 0 for nil, the status carried by an ExitCoder, otherwise 1.
func ExitStatus(err error) int {
	if err == nil {
		return 0
	}
	var coder ExitCoder
	if errors.As(err, &coder) {
		return coder.ExitCode()
	}
	return 1
}

var reloadHandlers struct {
	sync.Mutex
	next int
	fns  map[int]func()
}

// OnReload registers fn to run when the service manager asks for a reload
// (SIGHUP on Unix, a parameter-change control on Windows) and returns a
// function that unregisters it.
func OnReload(fn func()) (stop func()) {
	reloadHandlers.Lock()
	defer reloadHandlers.Unlock()
	if reloadHandlers.fns == nil {
		reloadHandlers.fns = map[int]func(){}
	}
	reloadHandlers.next++
	id := reloadHandlers.next
	reloadHandlers.fns[id] = fn
	return func() {
		reloadHandlers.Lock()
		delete(reloadHandlers.fns, id)
		reloadHandlers.Unlock()
	}
}

// requestReload runs every registered reload handler and reports whether
// there was one.
func requestReload() bool {
	reloadHandlers.Lock()
	fns := make([]func(), 0, len(reloadHandlers.fns))
	for _, fn := range reloadHandlers.fns {
		fns = append(fns, fn)
	}
	reloadHandlers.Unlock()
	for _, fn := range fns {
		fn()
	}
	return len(fns) > 0
}
