package node

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestAuditValidatedReverseCodecBudgetCanServeSession(t *testing.T) {
	t.Setenv("DBB_REVERSE_E2E_TOKEN", reverseTestToken)
	backend, greeting := mysqlLikeBackend(t)
	httpAddr, listen := freeAddr(t), freeAddr(t)
	dir := t.TempDir()
	pub := writeConfig(t, dir, "public.yaml", fmt.Sprintf(`node: {id: hq-b}
http: {enabled: true, listen: %q}
peers: [{id: plant, mode: reverse, expected_node_id: plant-a, credential_ref: plant, token_env: DBB_REVERSE_E2E_TOKEN}]
listeners: [{id: db, listen: %q, peer: plant, target: db}]
transport:
  codec_buffer_limit: 2MiB
  control_request_timeout: 200ms
  create_retry_timeout: 1s
  recovery: {enabled: false}
  shutdown_timeout: 10ms
`, httpAddr, listen))
	db := writeConfig(t, dir, "database.yaml", fmt.Sprintf(`node: {id: plant-a}
auth: {inbound_peers: [{id: hq-b, token_envs: [DBB_REVERSE_E2E_TOKEN], allowed_targets: [db]}]}
targets: {db: {address: %q}}
reverse:
  enabled: true
  peer_node_id: hq-b
  token_env: DBB_REVERSE_E2E_TOKEN
  tunnels_per_endpoint: 2
  endpoints: [{id: p, url: %q}]
transport:
  recovery: {enabled: false}
  shutdown_timeout: 10ms
`, backend, "http://"+httpAddr))
	pn, dn := NewNode(pub), NewNode(db)
	if err := pn.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer pn.Stop()
	if err := dn.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer dn.Stop()
	waitFor(t, 3*time.Second, "two WS tunnels", func() bool { return pn.hub.Tunnels("plant") == 2 })
	c, err := net.Dial("tcp", listen)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, len(greeting))
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("valid config and two live tunnels cannot carry first database greeting; codec_used=%d codec_limit=%d ingress_reserve=%d: %v", pub.Resources.Codec.Used(), pub.Resources.Codec.Limit(), pub.Resources.IngressCodecReserve(), err)
	}
	// Stream windows are charged per open stream and released when the
	// stream closes: after both nodes stop, nothing is left charged.
	_ = c.Close()
	dn.Stop()
	pn.Stop()
	if u, v := pub.Resources.Codec.Used(), db.Resources.Codec.Used(); u != 0 || v != 0 {
		t.Fatalf("codec still charged after stop: public=%d database=%d", u, v)
	}
}
