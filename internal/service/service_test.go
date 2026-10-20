package service

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dbbridge/internal/config"
)

func serviceFixture(t *testing.T) Options {
	t.Helper()
	path := filepath.Join(t.TempDir(), "peer.yaml")
	_, err := config.Generate(config.GenerateOptions{Output: path, Role: "ingress", NodeID: "A", PeerID: "B", Listen: "127.0.0.1:0", TargetID: "mysql", Protocol: "auto", Recovery: true, MaxSessions: 10})
	if err != nil {
		t.Fatal(err)
	}
	return Options{Name: "dbbridge-test", Config: path, LogLevel: "info", LogFormat: "json", DryRun: true}
}
func TestPrepareDoesNotWriteAndHidesCredentials(t *testing.T) {
	o := serviceFixture(t)
	t.Setenv("SERVICE_TEST_SECRET", "private-captured-credential")
	b, err := os.ReadFile(o.Config)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err = json.Unmarshal([]byte(`{}`), &object); err != nil {
		t.Fatal(err)
	}
	// A file-backed generated config is converted to an existing env-backed
	// deployment without changing the normal config validator.
	text := strings.ReplaceAll(string(b), "token_file: peer.token", "token_env: SERVICE_TEST_SECRET")
	if text == string(b) {
		t.Fatal("fixture did not replace token source")
	}
	if err = os.WriteFile(o.Config, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	p, err := Prepare(o)
	if err != nil {
		t.Fatal(err)
	}
	if p.EnvironmentFile == "" || len(p.EnvironmentNames) != 1 {
		t.Fatal("credential environment was not captured")
	}
	serialized, _ := json.Marshal(p)
	if bytes.Contains(serialized, []byte("private-captured-credential")) {
		t.Fatal("preview leaked a secret")
	}
	if _, err = os.Stat(p.EnvironmentFile); !os.IsNotExist(err) {
		t.Fatal("preflight wrote credentials")
	}
	created, err := saveEnvironment(p)
	if err != nil || !created {
		t.Fatal(err)
	}
	if runtimeFileModeCheck(p.EnvironmentFile) != nil {
		t.Fatal("snapshot permissions")
	}
	t.Setenv("SERVICE_TEST_SECRET", "")
	if err = os.Unsetenv("SERVICE_TEST_SECRET"); err != nil {
		t.Fatal(err)
	}
	if err = LoadEnvironment(p.EnvironmentFile); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("SERVICE_TEST_SECRET") != "private-captured-credential" {
		t.Fatal("snapshot did not restore referenced credential")
	}
	// X-SEC-10: a value the service manager already provides wins over the
	// install-time snapshot.
	t.Setenv("SERVICE_TEST_SECRET", "different-current-value")
	if err = LoadEnvironment(p.EnvironmentFile); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("SERVICE_TEST_SECRET") != "different-current-value" {
		t.Fatal("snapshot overrode a variable already set in the process environment")
	}
	if created, err = saveEnvironment(p); err != nil || created {
		t.Fatal("same snapshot should be reused")
	}
	p.environment["SERVICE_TEST_SECRET"] = "changed"
	if _, err = saveEnvironment(p); err == nil {
		t.Fatal("different snapshot overwritten")
	}
}
func TestEnvironmentRejectsMalformedOrProcessControlVariables(t *testing.T) {
	for _, body := range []string{`{"PATH":"bad"}`, `{"NOTIFY_SOCKET":"/tmp/bad"}`, `{"TOKEN":"a","TOKEN":"b"}`, `{"TOKEN":null}`, `{"TOKEN":123}`, `{"BAD=NAME":"value"}`} {
		path := filepath.Join(t.TempDir(), "environment.json")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if err := LoadEnvironment(path); err == nil {
			t.Fatalf("accepted malformed environment: %s", body)
		}
	}
}
func TestUnitEscapesPathsAndSetsGracefulStopBudget(t *testing.T) {
	p := Plan{Name: "dbbridge-test", Account: "root", Executable: `/opt/db "bridge"/$literal%/dbbridge`, WorkingDirectory: `/etc/db bridge/%configs`, Arguments: []string{"run", "--config", `/etc/db bridge/$config%.yaml`}, ShutdownTimeout: 65 * time.Second}
	unit, err := systemdUnit(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{`Type=notify`, `NotifyAccess=main`, `$$literal%%`, `\"bridge\"`, `$$config%%.yaml`, `TimeoutStopSec=80s`, `WorkingDirectory=/etc/db bridge/%%configs`} {
		if !strings.Contains(unit, fragment) {
			t.Fatalf("missing unit escape/setting: %s", fragment)
		}
	}
	p.Arguments = append(p.Arguments, "bad\nvalue")
	if _, err = systemdUnit(p); err == nil {
		t.Fatal("newline injection accepted")
	}
	for _, name := range []string{"../other", "other/service", "space name", "x\ny", "dbbridge.service", ""} {
		if ValidName(name) {
			t.Fatalf("accepted unsafe service name %q", name)
		}
	}
}
