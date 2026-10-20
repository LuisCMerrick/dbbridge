package node

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ING-10/X-LC-3 (node part): the ingress manager's CloseContext gets the
// hard shutdown deadline minus its abort grace, never a time in the past.
func TestIntegrationIngressCloseDeadline(t *testing.T) {
	hard := time.Now().Add(30 * time.Second)
	if d := ingressCloseDeadline(hard); !d.Equal(hard.Add(-time.Second)) {
		t.Fatalf("deadline %v, want %v", d, hard.Add(-time.Second))
	}
	if d := ingressCloseDeadline(time.Now().Add(-time.Minute)); d.Before(time.Now().Add(-time.Second)) {
		t.Fatalf("deadline in the past: %v", d)
	}
}

// Node.Start honours an already cancelled context and releases what NewNode
// created (ING-13 follow-up: the ctx parameter was unused).
func TestIntegrationStartCancelledContext(t *testing.T) {
	cfg := loadNodeConfig(t, "node: {id: B}\nhttp: {enabled: true, listen: '127.0.0.1:0'}\n")
	n := NewNode(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := n.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("start with cancelled ctx: %v", err)
	}
	if err := n.Start(context.Background()); !errors.Is(err, ErrNodeSpent) {
		t.Fatalf("node not released: %v", err)
	}
}

// Node metrics: every family has a TYPE line; *_total ingress snapshot values
// are counters and the ConnMetadata budget is exported.
func TestIntegrationMetricsTypes(t *testing.T) {
	t.Setenv("DBB_NODE_TEST_TOKEN", "node-test-token-0123456789abcdef")
	cfg := loadNodeConfig(t, `node: {id: A}
peers:
  - {id: B, expected_node_id: B, credential_ref: B, token_env: DBB_NODE_TEST_TOKEN, endpoints: [{id: e, url: 'http://127.0.0.1:9'}]}
listeners:
  - {id: one, listen: '127.0.0.1:0', peer: B, target: db}
`)
	n := NewNode(cfg)
	defer n.Stop()
	var b bytes.Buffer
	n.writeMetrics(&b)
	out := b.String()
	for _, want := range []string{"# TYPE dbb_ingress_accept_rejected_total counter", "# TYPE dbb_ingress_payload_bytes gauge", `dbb_resource_limit{resource="conn_metadata"}`} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q", want)
		}
	}
	typed := map[string]bool{}
	for _, line := range strings.Split(out, "\n") {
		if f := strings.Fields(line); len(f) >= 4 && f[0] == "#" && f[1] == "TYPE" {
			typed[f[2]] = true
		}
	}
	for _, line := range strings.Split(out, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name := strings.FieldsFunc(line, func(r rune) bool { return r == '{' || r == ' ' })[0]
		base := strings.TrimSuffix(strings.TrimSuffix(name, "_count"), "_sum")
		if !typed[name] && !typed[base] {
			t.Fatalf("sample %q has no TYPE line", name)
		}
	}
}
