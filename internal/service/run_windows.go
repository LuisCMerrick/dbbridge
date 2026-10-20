package service

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/eventlog"
)

type eventWriter struct{ log *eventlog.Log }

func (w eventWriter) Write(b []byte) (int, error) {
	message := strings.TrimSpace(string(b))
	if len(message) > 12000 {
		message = string([]rune(message)[:min(len([]rune(message)), 3000)]) + " [truncated]"
	}
	var err error
	if strings.Contains(message, `"level":"ERROR"`) || strings.Contains(message, "level=ERROR") {
		err = w.log.Error(1, message)
	} else if strings.Contains(message, `"level":"WARN"`) || strings.Contains(message, "level=WARN") {
		err = w.log.Warning(1, message)
	} else {
		err = w.log.Info(1, message)
	}
	if err != nil {
		return 0, err
	}
	return len(b), nil
}
func Run(name string, directory string, out io.Writer, run Runner) error {
	isService, err := svc.IsWindowsService()
	if err != nil {
		return err
	}
	if !isService {
		// Only Ctrl-C / Ctrl-Break (os.Interrupt) start a drain. Registering
		// SIGTERM would also capture CTRL_LOGOFF_EVENT, which Windows sends
		// when any interactive user logs off.
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
		defer cancel()
		// After the first Ctrl-C only the force-exit handler intercepts
		// further signals.
		go func() { <-ctx.Done(); cancel() }()
		return run(ctx, func() error { return nil }, out)
	}
	log, err := eventlog.Open(name)
	if err != nil {
		return err
	}
	defer log.Close()
	h := &windowsHandler{run: func(ctx context.Context, ready func() error, output io.Writer) error {
		if err := os.Chdir(directory); err != nil {
			return fmt.Errorf("service working directory: %w", err)
		}
		return run(ctx, ready, output)
	}, output: eventWriter{log}, crash: func(code int) {
		// Exiting without reporting SERVICE_STOPPED makes the SCM treat
		// the failure as a crash and apply the restart actions.
		os.Exit(code)
	}, events: log}
	if err = svc.Run(name, h); err != nil {
		_ = log.Error(1, err.Error())
		return err
	}
	h.mu.Lock()
	err = h.err
	h.mu.Unlock()
	if err != nil {
		_ = log.Error(1, err.Error())
	}
	return err
}

type windowsHandler struct {
	run    Runner
	output io.Writer
	// crash, when set, ends the process for a failure that the SCM should
	// restart. Configuration errors are reported as a stopped service with
	// service-specific exit code 78 instead, which the recovery settings
	// installed by DBBridge do not restart.
	crash  func(int)
	events *eventlog.Log
	mu     sync.Mutex
	err    error
}

// accepted lists the controls a running service accepts.
const accepted = svc.AcceptStop | svc.AcceptShutdown | svc.AcceptPreShutdown | svc.AcceptParamChange

func (h *windowsHandler) Execute(_ []string, requests <-chan svc.ChangeRequest, statuses chan<- svc.Status) (bool, uint32) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	var once sync.Once
	done := make(chan error, 1)
	go func() { done <- h.run(ctx, func() error { once.Do(func() { close(ready) }); return nil }, h.output) }()
	status := svc.Status{State: svc.StartPending, WaitHint: 10000, CheckPoint: 1}
	statuses <- status
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ready:
			ready = nil
			if status.State != svc.StopPending {
				status = svc.Status{State: svc.Running, Accepts: accepted}
				statuses <- status
			}
		case err := <-done:
			h.mu.Lock()
			h.err = err
			h.mu.Unlock()
			code := ExitStatus(err)
			switch {
			case err == nil:
				return false, 0
			case code == ExitConfigError:
				if h.events != nil {
					_ = h.events.Error(1, fmt.Sprintf("configuration error; the service stops with service-specific exit code %d and is not restarted. Fix the configuration and start the service again: %v", ExitConfigError, err))
				}
				return true, ExitConfigError
			default:
				if h.crash != nil {
					if h.events != nil {
						_ = h.events.Error(1, fmt.Sprintf("service failed and will be restarted by the service manager: %v", err))
					}
					h.crash(code)
				}
				return false, 1
			}
		case request, ok := <-requests:
			if !ok {
				requests = nil
				cancel()
				status = svc.Status{State: svc.StopPending, WaitHint: 10000, CheckPoint: 1}
				statuses <- status
				continue
			}
			switch request.Cmd {
			case svc.Stop, svc.Shutdown, svc.PreShutdown:
				if status.State != svc.StopPending {
					status = svc.Status{State: svc.StopPending, WaitHint: 10000, CheckPoint: 1}
					statuses <- status
					cancel()
				}
			case svc.ParamChange:
				go requestReload()
				statuses <- status
			case svc.Interrogate:
				statuses <- status
			}
		case <-tick.C:
			if status.State == svc.StartPending || status.State == svc.StopPending {
				status.CheckPoint++
				statuses <- status
			}
		}
	}
}
