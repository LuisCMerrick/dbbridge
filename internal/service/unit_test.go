package service

import (
	"strings"
	"testing"
	"time"
)

// Restart policy, descriptor limit and stop budget.
func TestUnitRestartPolicyNofileAndStopBudget(t *testing.T) {
	p := Plan{Name: "dbbridge-test", Account: "dbbridge", Executable: "/opt/dbbridge/dbbridge", WorkingDirectory: "/etc/dbbridge", Arguments: []string{"run"}, ShutdownTimeout: 45 * time.Second, MaxSessions: 5000}
	unit, err := systemdUnit(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, directive := range []string{"RestartPreventExitStatus=78\n", "LimitNOFILE=21024\n", "TimeoutStopSec=60s\n", "StartLimitIntervalSec=300\n", "StartLimitBurst=10\n", "CapabilityBoundingSet=\n"} {
		if !strings.Contains(unit, "\n"+directive) {
			t.Errorf("unit lacks %q", directive)
		}
	}
	p.MaxSessions = 200
	if unit, _ = systemdUnit(p); !strings.Contains(unit, "\nLimitNOFILE=8192\n") {
		t.Error("descriptor limit below the 8192 floor")
	}
}

// The plan's stop budget is drain + teardown, so TimeoutStopSec
// exceeds the longest graceful Node.Stop.
func TestPrepareUsesMaxShutdownDuration(t *testing.T) {
	o := serviceFixture(t)
	p, err := Prepare(o)
	if err != nil {
		t.Fatal(err)
	}
	if p.ShutdownTimeout != 45*time.Second {
		t.Fatalf("plan stop budget %v, want shutdown_timeout 30s + teardown 15s", p.ShutdownTimeout)
	}
	if p.Platform == "linux" && !strings.Contains(p.Unit, "\nTimeoutStopSec=60s\n") {
		t.Fatalf("unit stop timeout:\n%s", p.Unit)
	}
}

// Runtime-affecting names are refused; the irrelevant
// CODEX_HOME entry is gone.
func TestEnvironmentNames(t *testing.T) {
	for _, name := range []string{"GOFIPS140", "GOCOVERDIR", "REQUEST_METHOD", "LISTEN_FDNAMES", "DYLD_INSERT_LIBRARIES", "TZ"} {
		if safeEnvName(name) {
			t.Errorf("%s accepted", name)
		}
	}
	for _, name := range []string{"CODEX_HOME", "DBB_TOKEN_X", "PLANT_A_SECRET"} {
		if !safeEnvName(name) {
			t.Errorf("%s rejected", name)
		}
	}
}
