package service

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"dbbridge/internal/config"
)

// The installed service loads its configuration from the install-time
// snapshot alone, including outbound credentials no peer references yet.
func TestSnapshotCoversEveryResolvedTokenVariable(t *testing.T) {
	dir := t.TempDir()
	auth := `version: 1
outbound_peers:
  - {id: B, token_env: SNAP_TOKEN_B}
  - {id: C, token_env: SNAP_TOKEN_C}
inbound_peers:
  - {id: D, enabled: false, token_envs: [SNAP_TOKEN_D_UNSET], allowed_targets: [db]}
`
	cfg := `node: {id: A}
auth: {credentials_file: auth.yaml}
peers:
  - id: B
    expected_node_id: B
    credential_ref: B
    endpoints: [{id: e1, url: "http://127.0.0.1:18080", priority: 1}]
listeners: [{id: l1, listen: "127.0.0.1:0", peer: B, target: db}]
`
	if err := os.WriteFile(filepath.Join(dir, "auth.yaml"), []byte(auth), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "peer.yaml")
	if err := os.WriteFile(path, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SNAP_TOKEN_B", "token-b-0123456789abcdef")
	t.Setenv("SNAP_TOKEN_C", "token-c-0123456789abcdef")
	exe, _ := os.Executable()
	p, err := Prepare(Options{Name: "snap", Config: path, Executable: exe, LogLevel: "info", LogFormat: "json", DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.EnvironmentNames) != 2 {
		t.Fatalf("snapshot names: %v", p.EnvironmentNames)
	}
	if _, err := saveEnvironment(p); err != nil {
		t.Fatal(err)
	}
	os.Unsetenv("SNAP_TOKEN_B")
	os.Unsetenv("SNAP_TOKEN_C")
	if err := LoadEnvironment(p.EnvironmentFile); err != nil {
		t.Fatal(err)
	}
	if _, err = config.LoadConfig(path); err != nil {
		t.Fatalf("installed service cannot load its config: %v", err)
	}
}

// Configuration problems found by the install preflight carry a
// configuration error code.
func TestInstallPreflightErrorsAreConfigurationErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peer.yaml")
	if err := os.WriteFile(path, []byte("node: {id: A}\nunknown: 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	exe, _ := os.Executable()
	_, err := Prepare(Options{Name: "x", Config: path, Executable: exe, LogLevel: "info", LogFormat: "text"})
	if config.ErrorCode(err) != config.CodeConfigRead {
		t.Fatalf("code %q for %v", config.ErrorCode(err), err)
	}
}

type codedExit struct{ code int }

func (e codedExit) Error() string { return "exit" }
func (e codedExit) ExitCode() int { return e.code }

func TestExitStatus(t *testing.T) {
	if ExitStatus(nil) != 0 || ExitStatus(errors.New("x")) != 1 || ExitStatus(codedExit{78}) != 78 {
		t.Fatal("exit status mapping")
	}
}
