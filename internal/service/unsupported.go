//go:build !windows && !linux

package service

import (
	"context"
	"errors"
)

func install(context.Context, Plan) error {
	return errors.New("service installation is supported on Windows and Linux/systemd")
}
func uninstall(context.Context, string) error {
	return errors.New("service removal is supported on Windows and Linux/systemd")
}
func notifyReady() error    { return nil }
func notifyStopping() error { return nil }
func checkUninstall(context.Context, string) error {
	return errors.New("service removal is supported on Windows and Linux/systemd")
}
