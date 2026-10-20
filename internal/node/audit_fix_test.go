package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"dbbridge/internal/config"
)

func loadNodeConfig(t *testing.T, body string) *config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "peer.yaml")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func waitGoroutines(limit int) int {
	deadline := time.Now().Add(3 * time.Second)
	n := runtime.NumGoroutine()
	for n > limit && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		n = runtime.NumGoroutine()
	}
	return n
}

// X-LC-6: Stop on a node that never started releases the credential reload
// goroutine NewNode started.
func TestAuditXLC6StopWithoutStartReleasesReloadLoop(t *testing.T) {
	dir := t.TempDir()
	creds := filepath.Join(dir, "auth.yaml")
	if err := os.WriteFile(creds, []byte("version: 1\ninbound_peers:\n  - {id: A, token_envs: [DBB_NODE_TEST_TOKEN], allowed_targets: [db]}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DBB_NODE_TEST_TOKEN", "node-test-token-0123456789abcdef")
	cfg := loadNodeConfig(t, fmt.Sprintf("node: {id: B}\nhttp: {enabled: true, listen: '127.0.0.1:0'}\ntargets: {db: {address: '127.0.0.1:3306', allowed_cidrs: ['127.0.0.1/32']}}\nauth: {credentials_file: %q, reload_interval: 1s}\n", creds))
	before := runtime.NumGoroutine()
	n := NewNode(cfg)
	if runtime.NumGoroutine() <= before {
		t.Skip("authenticator started no reload goroutine")
	}
	n.Stop()
	if got := waitGoroutines(before); got > before {
		t.Fatalf("goroutines after Stop of an unstarted node: %d > %d", got, before)
	}
	if err := n.Start(context.Background()); !errors.Is(err, ErrNodeSpent) {
		t.Fatalf("start after release: %v", err)
	}
}

// X-LC-1: a fatal background failure is delivered on Fatal and marks the
// node not ready; further reports never block.
func TestAuditXLC1FatalChannel(t *testing.T) {
	cfg := loadNodeConfig(t, "node: {id: B}\nhttp: {enabled: true, listen: '127.0.0.1:0'}\nadmin: {enabled: true, listen: '127.0.0.1:0'}\n")
	n := NewNode(cfg)
	if err := n.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	n.reportFatal(errors.New("first"))
	n.reportFatal(errors.New("second"))
	select {
	case err := <-n.Fatal():
		if err.Error() != "first" {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fatal error not delivered")
	}
	if n.adminServer.Ready() {
		t.Fatal("node still ready after a fatal failure")
	}
}

// X-LC-3: teardown is bounded by the hard deadline.
func TestAuditXLC3RunBounded(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	start := time.Now()
	if runBounded(time.Now().Add(50*time.Millisecond), func() {}, func() { <-block }) {
		t.Fatal("blocked teardown reported complete")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("runBounded waited %v", d)
	}
	if !runBounded(time.Now().Add(time.Second), func() {}) {
		t.Fatal("fast teardown reported incomplete")
	}
}

// ING-13 (node part): a failed ingress start leaves no listener behind and
// spends the node.
func TestAuditING13FailedIngressStartCleansUp(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	freeAddr := free.Addr().String()
	free.Close()
	t.Setenv("DBB_NODE_TEST_TOKEN", "node-test-token-0123456789abcdef")
	cfg := loadNodeConfig(t, fmt.Sprintf(`node: {id: A}
peers:
  - {id: B, expected_node_id: B, credential_ref: B, token_env: DBB_NODE_TEST_TOKEN, endpoints: [{id: e, url: 'http://127.0.0.1:9'}]}
listeners:
  - {id: one, listen: %q, peer: B, target: db}
  - {id: two, listen: %q, peer: B, target: db}
`, freeAddr, busy.Addr().String()))
	n := NewNode(cfg)
	if err = n.Start(context.Background()); err == nil {
		n.Stop()
		t.Fatal("start succeeded with a busy port")
	}
	if c, e := net.DialTimeout("tcp", freeAddr, time.Second); e == nil {
		c.Close()
		t.Fatal("listener of a failed start still accepts connections")
	}
	if err = n.Start(context.Background()); !errors.Is(err, ErrNodeSpent) {
		t.Fatalf("failed node restarted: %v", err)
	}
}
