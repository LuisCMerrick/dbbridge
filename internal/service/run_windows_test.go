package service

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"golang.org/x/sys/windows/svc"
)

func receiveServiceStatus(t *testing.T, statuses <-chan svc.Status, want svc.State) svc.Status {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case status := <-statuses:
			if status.State == want {
				return status
			}
		case <-timer.C:
			t.Fatalf("service did not reach state %d", want)
			return svc.Status{}
		}
	}
}
func TestWindowsHandlerReadinessAndDrain(t *testing.T) {
	start, drain := make(chan struct{}), make(chan struct{})
	requests := make(chan svc.ChangeRequest, 4)
	statuses := make(chan svc.Status, 16)
	h := &windowsHandler{output: io.Discard, run: func(ctx context.Context, ready func() error, _ io.Writer) error {
		<-start
		if err := ready(); err != nil {
			return err
		}
		<-ctx.Done()
		<-drain
		return nil
	}}
	done := make(chan uint32, 1)
	go func() { _, code := h.Execute(nil, requests, statuses); done <- code }()
	receiveServiceStatus(t, statuses, svc.StartPending)
	select {
	case state := <-statuses:
		if state.State == svc.Running {
			t.Fatal("reported running before readiness")
		}
	default:
	}
	close(start)
	running := receiveServiceStatus(t, statuses, svc.Running)
	if running.Accepts&svc.AcceptStop == 0 || running.Accepts&svc.AcceptPreShutdown == 0 {
		t.Fatal("service lacks stop/preshutdown controls")
	}
	requests <- svc.ChangeRequest{Cmd: svc.Interrogate}
	receiveServiceStatus(t, statuses, svc.Running)
	requests <- svc.ChangeRequest{Cmd: svc.PreShutdown}
	receiveServiceStatus(t, statuses, svc.StopPending)
	select {
	case <-done:
		t.Fatal("service stopped before drain completed")
	default:
	}
	close(drain)
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("normal stop code=%d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("service did not stop")
	}
}
func TestWindowsHandlerReportsStartupFailure(t *testing.T) {
	failure := errors.New("startup fixture failure")
	h := &windowsHandler{output: io.Discard, run: func(context.Context, func() error, io.Writer) error { return failure }}
	statuses := make(chan svc.Status, 16)
	_, code := h.Execute(nil, make(chan svc.ChangeRequest), statuses)
	if code == 0 {
		t.Fatal("startup failure reported success")
	}
	h.mu.Lock()
	err := h.err
	h.mu.Unlock()
	if !errors.Is(err, failure) {
		t.Fatal("startup cause not preserved")
	}
}
