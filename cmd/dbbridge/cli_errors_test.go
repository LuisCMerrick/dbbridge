package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dbbridge/internal/config"
	"dbbridge/internal/service"
)

// install reports configuration errors with exit status 78 and a code, like
// check-config.
func TestInstallConfigurationErrorExitsWithConfigStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(path, []byte("node: {id: A}\nbogus: 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"install", "-c", path, "--dry-run"}, {"check-config", "-c", path}} {
		err := execute(args, &bytes.Buffer{}, &bytes.Buffer{})
		if exitCode(err) != 78 {
			t.Fatalf("%v: exit %d (%v)", args, exitCode(err), err)
		}
		var out bytes.Buffer
		printError(&out, err)
		if !strings.HasPrefix(out.String(), "Error ["+config.CodeConfigRead+"]: ") {
			t.Fatalf("%v: %q", args, out.String())
		}
	}
	var out bytes.Buffer
	printError(&out, errors.New("plain"))
	if out.String() != "Error: plain\n" {
		t.Fatalf("uncoded error: %q", out.String())
	}
}

// Each command's -h lists only its own options.
func TestCommandHelpListsOnlyItsOptions(t *testing.T) {
	var diagnostics bytes.Buffer
	if err := execute([]string{"run", "--help"}, &bytes.Buffer{}, &diagnostics); err != nil {
		t.Fatal(err)
	}
	help := diagnostics.String()
	for _, hidden := range []string{"-dry-run", "-start", "-service-user", "-instal", "-offline", "-exec-path"} {
		if strings.Contains(help, hidden+"\n") {
			t.Fatalf("run help lists %s:\n%s", hidden, help)
		}
	}
	diagnostics.Reset()
	if err := execute([]string{"install", "--help"}, &bytes.Buffer{}, &diagnostics); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(diagnostics.String(), "-exec-path\n") || strings.Contains(diagnostics.String(), "-offline\n") {
		t.Fatalf("install help:\n%s", diagnostics.String())
	}
}

// --exec-path is written to the service definition verbatim.
func TestInstallExecPathFlag(t *testing.T) {
	dir := t.TempDir()
	if err := execute([]string{"gen-config", "-o", dir}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := execute([]string{"install", "-c", filepath.Join(dir, "peer-a.yaml"), "--dry-run", "--exec-path", exe}, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var plan service.Plan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal plan: %v\nout:\n%s", err, out.String())
	}
	if plan.Executable != exe {
		t.Fatalf("executable mismatch: got %q, want %q", plan.Executable, exe)
	}
	if err := execute([]string{"run", "-c", filepath.Join(dir, "peer-a.yaml"), "--exec-path", exe}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("--exec-path accepted without installation")
	}
}
