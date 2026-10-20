package service

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSystemdInstallRollbackDuplicateAndUninstall(t *testing.T) {
	for _, failed := range []string{"", "daemon-reload", "enable", "start"} {
		t.Run("failure="+failed, func(t *testing.T) {
			o := serviceFixture(t)
			o.Start = failed == "start"
			p, err := Prepare(o)
			if err != nil {
				t.Fatal(err)
			}
			p.Account = strconv.Itoa(os.Getuid())
			p.Unit, err = systemdUnit(p)
			if err != nil {
				t.Fatal(err)
			}
			p.EnvironmentFile = filepath.Join(p.WorkingDirectory, "test-env.json")
			p.environment = map[string]string{"SERVICE_TEST_TOKEN": "private-fixture"}
			var calls []string
			m := systemdManager{dir: t.TempDir(), command: func(_ context.Context, args ...string) error {
				calls = append(calls, strings.Join(args, " "))
				if args[0] == failed {
					return errors.New("injected systemctl failure")
				}
				return nil
			}}
			err = m.install(context.Background(), p)
			if (err == nil) != (failed == "") {
				t.Fatalf("failure=%s error=%v", failed, err)
			}
			unit := filepath.Join(m.dir, p.Name+".service")
			_, unitErr := os.Stat(unit)
			_, envErr := os.Stat(p.EnvironmentFile)
			if failed == "daemon-reload" || failed == "enable" {
				if !os.IsNotExist(unitErr) || !os.IsNotExist(envErr) {
					t.Fatal("failed registration left unit/credentials behind")
				}
				return
			}
			if unitErr != nil || envErr != nil {
				t.Fatal("successful registration was not retained")
			}
			before := len(calls)
			if err = m.install(context.Background(), p); err == nil || len(calls) != before {
				t.Fatal("duplicate installation mutated existing service")
			}
			m.command = func(_ context.Context, args ...string) error {
				calls = append(calls, strings.Join(args, " "))
				return nil
			}
			if err = m.uninstall(context.Background(), p.Name); err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(unit); !os.IsNotExist(err) {
				t.Fatal("unit not removed")
			}
			if _, err = os.Stat(o.Config); err != nil {
				t.Fatal("uninstall deleted config")
			}
			if _, err = os.Stat(p.EnvironmentFile); err != nil {
				t.Fatal("uninstall deleted credentials")
			}
		})
	}
}
func TestSystemdRefusesUnownedUnit(t *testing.T) {
	m := systemdManager{dir: t.TempDir(), command: func(context.Context, ...string) error { t.Fatal("unowned service was changed"); return nil }}
	if err := os.WriteFile(filepath.Join(m.dir, "foreign.service"), []byte("[Service]\nExecStart=/bin/true\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := m.uninstall(context.Background(), "foreign"); err == nil {
		t.Fatal("unowned unit was removed")
	}
}

type capturedNotification struct {
	payload  []byte
	deadline time.Time
	closed   bool
}

func (c *capturedNotification) Write(b []byte) (int, error) {
	c.payload = append(c.payload, b...)
	return len(b), nil
}
func (c *capturedNotification) Close() error                       { c.closed = true; return nil }
func (c *capturedNotification) SetWriteDeadline(d time.Time) error { c.deadline = d; return nil }
func TestReadinessNotification(t *testing.T) {
	for _, path := range []string{"/run/dbbridge-notify.sock", "@dbbridge-notify"} {
		c := &capturedNotification{}
		err := sendReady(path, func(address *net.UnixAddr) (notifyConnection, error) {
			want := path
			if strings.HasPrefix(path, "@") {
				want = "\x00" + path[1:]
			}
			if address.Net != "unixgram" || address.Name != want {
				t.Fatalf("wrong notification address: %+v", address)
			}
			return c, nil
		})
		if err != nil || string(c.payload) != "READY=1" || !c.closed || c.deadline.IsZero() {
			t.Fatalf("notification=%+v error=%v", c, err)
		}
	}
	if err := sendReady("", func(*net.UnixAddr) (notifyConnection, error) {
		t.Fatal("foreground mode tried to notify systemd")
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestUnitAcceptedBySystemdAnalyze(t *testing.T) {
	tool, err := exec.LookPath("systemd-analyze")
	if err != nil {
		t.Skip("systemd-analyze not installed")
	}
	dir := filepath.Join(t.TempDir(), "paths $literal% with space")
	if err = os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	p := Plan{Name: "dbbridge-verify", Account: strconv.Itoa(os.Getuid()), Executable: "/bin/true", WorkingDirectory: dir, Arguments: []string{"run", "--config", filepath.Join(dir, "peer.yaml")}, ShutdownTimeout: 30 * time.Second}
	unit, err := systemdUnit(p)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), p.Name+".service")
	if err = os.WriteFile(path, []byte(unit), 0600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command(tool, "verify", "--man=no", path).CombinedOutput(); err != nil {
		t.Fatalf("systemd rejected generated unit: %v\n%s", err, output)
	}
}

// STOPPING=1 is sent through the same notification path.
func TestStoppingNotification(t *testing.T) {
	c := &capturedNotification{}
	if err := sendState("/run/dbbridge-notify.sock", "STOPPING=1", func(*net.UnixAddr) (notifyConnection, error) { return c, nil }); err != nil {
		t.Fatal(err)
	}
	if string(c.payload) != "STOPPING=1" || !c.closed {
		t.Fatalf("notification=%+v", c)
	}
}
