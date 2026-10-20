package service

import (
	"strings"
	"testing"
	"time"
)

func TestFixUnitHardening(t *testing.T) {
	p := Plan{Name: "dbbridge-test", Account: "dbbridge", Executable: "/opt/dbbridge/dbbridge", WorkingDirectory: "/etc/dbbridge", Arguments: []string{"run"}, ShutdownTimeout: 30 * time.Second}
	unit, err := systemdUnit(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, directive := range []string{"ProtectSystem=strict\n", "ProtectHome=true\n", "PrivateTmp=true\n", "CapabilityBoundingSet=\n", "AmbientCapabilities=\n", "RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX\n", "NoNewPrivileges=true\n", "SystemCallFilter=@system-service\n"} {
		if !strings.Contains(unit, "\n"+directive) {
			t.Errorf("unit lacks %q", directive)
		}
	}
	p.lowPort = true
	p.WorkingDirectory = "/home/ops/dbbridge"
	p.Executable = "/tmp/dbbridge"
	if unit, err = systemdUnit(p); err != nil {
		t.Fatal(err)
	}
	for _, directive := range []string{"CapabilityBoundingSet=CAP_NET_BIND_SERVICE\n", "AmbientCapabilities=CAP_NET_BIND_SERVICE\n", "ProtectHome=read-only\n", "PrivateTmp=false\n"} {
		if !strings.Contains(unit, "\n"+directive) {
			t.Errorf("unit lacks %q", directive)
		}
	}
}

func TestFixEnvironmentDenyList(t *testing.T) {
	for _, name := range []string{"GODEBUG", "HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "NO_PROXY", "SSL_CERT_FILE", "SSL_CERT_DIR", "LD_PRELOAD", "ld_library_path"} {
		if safeEnvName(name) {
			t.Errorf("%s accepted in credential snapshot", name)
		}
	}
	if !safeEnvName("DBB_TOKEN_A_TO_B") {
		t.Fatal("ordinary credential name rejected")
	}
}

func TestFixPrepareWarnsAboutPrivilegedDefaultAccount(t *testing.T) {
	o := serviceFixture(t)
	p, err := Prepare(o)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.readPaths) < 2 {
		t.Fatalf("service files not collected: %v", p.readPaths)
	}
	if p.Account == "root" || p.Account == "LocalSystem" {
		found := false
		for _, w := range p.Warnings {
			found = found || strings.Contains(w, "full privileges") || strings.Contains(w, "root without capabilities")
		}
		if !found {
			t.Fatalf("no privilege warning: %v", p.Warnings)
		}
	}
}
