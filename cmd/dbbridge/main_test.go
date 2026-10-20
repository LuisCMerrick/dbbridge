package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"dbbridge/internal/config"
	"dbbridge/internal/service"
)

func TestGenerateAndInstallPreview(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "peer-a.yaml")
	var output, diagnostics bytes.Buffer
	if err := execute([]string{"gen-config", "-o", dir}, &output, &diagnostics); err != nil {
		t.Fatal(err)
	}
	secret, err := os.ReadFile(filepath.Join(dir, "shared.token"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(output.Bytes(), bytes.TrimSpace(secret)) {
		t.Fatal("generator printed secret")
	}
	for _, args := range [][]string{
		{"install", "-c", path, "--dry-run"},
		{"install", "--config", path, "--dry-run"},
	} {
		output.Reset()
		diagnostics.Reset()
		if err = execute(args, &output, &diagnostics); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		var plan service.Plan
		if err = json.Unmarshal(output.Bytes(), &plan); err != nil {
			t.Fatal(err)
		}
		if plan.Name != "dbbridge" || !plan.Automatic || plan.Start || !filepath.IsAbs(plan.Executable) || !filepath.IsAbs(plan.Arguments[2]) {
			t.Fatalf("unexpected plan: %+v", plan)
		}
		if bytes.Contains(output.Bytes(), bytes.TrimSpace(secret)) {
			t.Fatal("install preview printed secret")
		}
	}
	if err = execute([]string{"check-config", "-c", path}, &output, &diagnostics); err != nil {
		t.Fatal(err)
	}
}
func TestRejectsAmbiguousOrUnsupportedArguments(t *testing.T) {
	for _, args := range [][]string{
		{"gen-config", "--mode", "invalid"}, {"gen-config", "--role", "egress"}, {"run"}, {"install"}, {"version", "extra"}, {"gen-config", "extra"},
		{"uninstall", "--service-name", "../ssh", "--dry-run"}, {"run", "-c", "unused", "--dry-run"}, {"check-config", "-c", "unused", "--install"},
		{"--config", "unused"}, {"--install", "-c", "unused", "--dry-run"}, {"run", "-c", "unused", "--start"},
		{"run", "-c", "unused", "--log-format", "invalid"}, {"unknown"},
	} {
		var out bytes.Buffer
		if err := execute(args, &out, &out); err == nil {
			t.Fatalf("accepted args %v", args)
		}
	}
	// Uninstall --dry-run checks that the service exists and is
	// managed by DBBridge instead of always reporting success.
	if runtime.GOOS == "linux" {
		var out bytes.Buffer
		if _, err := os.Stat("/etc/systemd/system/dbbridge-preview-absent.service"); os.IsNotExist(err) {
			if err := execute([]string{"uninstall", "--service-name", "dbbridge-preview-absent", "--dry-run"}, &out, &out); err == nil || !strings.Contains(err.Error(), "not installed") {
				t.Fatalf("dry-run uninstall of a missing service: %v", err)
			}
		}
	}
	for _, args := range [][]string{{"help"}, {"gen-config", "--help"}, {"run", "--help"}, {"--version"}} {
		var out bytes.Buffer
		if err := execute(args, &out, &out); err != nil || strings.TrimSpace(out.String()) == "" {
			t.Fatalf("help/preview args=%v error=%v", args, err)
		}
	}
}

func TestGenerateDefaultPairInCurrentDirectory(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	var output bytes.Buffer
	if err := execute([]string{"gen-config"}, &output, &output); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"peer-a.yaml", "peer-b.yaml"} {
		cfg, err := config.LoadConfig(filepath.Join(dir, name))
		if err != nil || !cfg.HTTP.Enabled || len(cfg.Listeners) != 1 || len(cfg.Targets) != 1 || len(cfg.Peers) != 1 {
			t.Fatalf("default %s is not bidirectional: %v", name, err)
		}
		if !strings.Contains(output.String(), name) {
			t.Fatal("CLI omitted one node from output")
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 3 || !strings.Contains(output.String(), "bidirectional") {
		t.Fatal("default generation did not produce exactly one bidirectional pair and token")
	}
}

func TestGeneratePairPerNodeCLIOptions(t *testing.T) {
	dir := t.TempDir()
	var output bytes.Buffer
	args := []string{"gen-config", "-o", dir, "--a-node-id", "factory", "--b-node-id", "cloud",
		"--a-target-id", "factory-db", "--b-target-id", "cloud-db", "--a-listen", "127.0.0.1:15432", "--b-listen", "127.0.0.1:11433",
		"--a-peer-url", "https://cloud-primary.example.com", "--a-peer-url", "https://cloud-backup.example.com", "--b-peer-url", "https://factory.example.com",
		"--a-target-address", "db.internal:5432", "--a-allow-cidr", "10.0.0.0/8", "--b-target-address", "127.0.0.1:1433", "--protocol", "http-sse", "--recovery=false"}
	if err := execute(args, &output, &output); err != nil {
		t.Fatal(err)
	}
	a, err := config.LoadConfig(filepath.Join(dir, "peer-a.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := config.LoadConfig(filepath.Join(dir, "peer-b.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if a.Node.ID != "factory" || b.Node.ID != "cloud" || a.Listeners[0].Target != "cloud-db" || b.Listeners[0].Target != "factory-db" || len(a.Peers[0].Endpoints) != 2 || a.Transport.Protocol != "http-sse" || b.Transport.Recovery.Enabled {
		t.Fatal("CLI per-node options were not applied to the matched pair")
	}
}
