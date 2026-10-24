package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	"dbbridge/internal/service"
)

type fakeNode struct {
	mu       sync.Mutex
	events   []string
	fatal    chan error
	release  chan struct{}
	startErr error
}

func (f *fakeNode) log(e string) { f.mu.Lock(); f.events = append(f.events, e); f.mu.Unlock() }
func (f *fakeNode) Start(context.Context) error {
	f.log("start")
	return f.startErr
}
func (f *fakeNode) Stop() {
	f.log("stop-begin")
	if f.release != nil {
		<-f.release
	}
	f.log("stop-end")
}
func (f *fakeNode) Fatal() <-chan error { return f.fatal }
func (f *fakeNode) get() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...)
}

// The force-exit handler is active for the whole drain: installed
// before Stop starts and removed only after Stop returns.
func TestServeForceExitCoversDrain(t *testing.T) {
	n := &fakeNode{fatal: make(chan error, 1), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	install := func() func() {
		n.log("force-on")
		return func() { n.log("force-off") }
	}
	done := make(chan error, 1)
	go func() { done <- serve(ctx, n, func() error { return nil }, install) }()
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(strings.Join(n.get(), ","), "stop-begin") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := strings.Join(n.get(), ","); got != "start,force-on,stop-begin" {
		t.Fatalf("during drain: %s", got)
	}
	close(n.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(n.get(), ","); got != "start,force-on,stop-begin,stop-end,force-off" {
		t.Fatalf("after drain: %s", got)
	}
}

// A fatal background failure stops the node and is returned so the
// process exits non-zero.
func TestServeReturnsFatalBackgroundError(t *testing.T) {
	n := &fakeNode{fatal: make(chan error, 1)}
	n.fatal <- errors.New("egress listener died")
	err := serve(context.Background(), n, func() error { return nil }, func() func() { return func() {} })
	if err == nil || !strings.Contains(err.Error(), "egress listener died") {
		t.Fatalf("fatal error not returned: %v", err)
	}
	if got := strings.Join(n.get(), ","); got != "start,stop-begin,stop-end" {
		t.Fatalf("events: %s", got)
	}
	n = &fakeNode{fatal: make(chan error), startErr: errors.New("bind failed")}
	if err = serve(context.Background(), n, func() error { return nil }, nil); err == nil {
		t.Fatal("start error swallowed")
	}
	if got := strings.Join(n.get(), ","); got != "start,stop-begin,stop-end" {
		t.Fatalf("failed start did not release the node: %s", got)
	}
}

// The default version never contains the run date.
func TestResolveBuildInfoIsStable(t *testing.T) {
	none := func() (*debug.BuildInfo, bool) { return nil, false }
	v, c, d := resolveBuildInfo("", "unknown", "unknown", none)
	if v != "1.5-dev" || c != "unknown" || d != "unknown" {
		t.Fatalf("got %s %s %s", v, c, d)
	}
	vcs := func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "0123456789abcdef0123"}, {Key: "vcs.time", Value: "2026-10-01T00:00:00Z"}, {Key: "vcs.modified", Value: "true"}}}, true
	}
	v, c, d = resolveBuildInfo("", "unknown", "unknown", vcs)
	if v != "1.5-dev" || c != "0123456789ab-dirty" || d != "2026-10-01T00:00:00Z" {
		t.Fatalf("got %s %s %s", v, c, d)
	}
	if v, c, _ = resolveBuildInfo("1.4.7", "abc", "x", vcs); v != "1.4.7" || c != "abc" {
		t.Fatal("injected values overridden")
	}
}

// Configuration errors exit with EX_CONFIG; --offline
// validates without secrets.
func TestCheckConfigExitCodeAndOffline(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "peer.yaml")
	body := "node: {id: A}\npeers:\n  - {id: B, expected_node_id: B, credential_ref: B, token_env: DBB_CLI_UNSET_TOKEN, endpoints: [{id: e, url: 'https://b.example.invalid'}]}\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	err := execute([]string{"check-config", "-c", path}, &out, &errOut)
	if err == nil || exitCode(err) != service.ExitConfigError {
		t.Fatalf("missing token: err=%v code=%d", err, exitCode(err))
	}
	if err = execute([]string{"check-config", "-c", path, "--offline"}, &out, &errOut); err != nil || !strings.Contains(out.String(), "offline") {
		t.Fatalf("offline check: %v %q", err, out.String())
	}
	if err = execute([]string{"run", "-c", path, "--offline"}, &out, &errOut); err == nil {
		t.Fatal("--offline accepted for run")
	}
	if exitCode(errors.New("x")) != 1 {
		t.Fatal("generic errors must exit 1")
	}
}
