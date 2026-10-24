package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"dbbridge/internal/config"
)

const reverseTestToken = "reverse-e2e-token-0123456789abcdef"

var (
	usedAddrMu sync.Mutex
	usedAddrs  = map[string]bool{}
)

// freeAddr returns a loopback address that was free and that no other call
// in this test binary returned.
func freeAddr(t *testing.T) string {
	t.Helper()
	usedAddrMu.Lock()
	defer usedAddrMu.Unlock()
	for {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		a := l.Addr().String()
		l.Close()
		if !usedAddrs[a] {
			usedAddrs[a] = true
			return a
		}
	}
}

// mysqlLikeBackend speaks first (like a MySQL greeting) and then echoes every
// byte back, so a client can check both directions byte for byte.
func mysqlLikeBackend(t *testing.T) (addr string, greeting []byte) {
	t.Helper()
	greeting = []byte("\x4a\x00\x00\x00\x0a8.4.0-dbbridge-test\x00")
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				if _, err := c.Write(greeting); err != nil {
					return
				}
				_, _ = io.Copy(c, c)
			}(c)
		}
	}()
	return l.Addr().String(), greeting
}

type reversePair struct {
	public, db   *Node
	listen       string // public-side listener for database clients
	greeting     []byte
	publicHTTP   string
	publicConfig *config.Config
}

func writeConfig(t *testing.T, dir, name, body string) *config.Config {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := config.LoadConfig(p)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return c
}

// startReversePair starts a public-side node with one reverse peer and a
// database-side node behind "NAT" (no listening socket) that dials it.
func startReversePair(t *testing.T, recovery string) *reversePair {
	t.Helper()
	return startReversePairWith(t, recovery, "auto", nil)
}

// startReversePairWith is startReversePair with reverse.transport set to
// mode; front, when set, puts an intermediary in front of the public side
// (it gets the public side's base URL and returns the URL to dial).
func startReversePairWith(t *testing.T, recovery, mode string, front func(public string) string) *reversePair {
	t.Helper()
	t.Setenv("DBB_REVERSE_E2E_TOKEN", reverseTestToken)
	backend, greeting := mysqlLikeBackend(t)
	httpAddr, listen := freeAddr(t), freeAddr(t)
	dir := t.TempDir()
	transport := fmt.Sprintf("transport:\n  recovery: %s\n  shutdown_timeout: 200ms\n  ws: {handshake_timeout: 1s}\n", recovery)
	dialURL := "http://" + httpAddr
	if front != nil {
		dialURL = front(dialURL)
	}
	pub := writeConfig(t, dir, "hq.yaml", fmt.Sprintf(`node: {id: hq-b}
http: {enabled: true, listen: %q}
peers:
  - id: plant
    mode: reverse
    expected_node_id: plant-a
    credential_ref: plant
    token_env: DBB_REVERSE_E2E_TOKEN
    failover: {cooldown: 200ms}
listeners: [{id: plant-db, listen: %q, peer: plant, target: db}]
%s`, httpAddr, listen, transport))
	db := writeConfig(t, dir, "plant.yaml", fmt.Sprintf(`node: {id: plant-a}
auth:
  inbound_peers: [{id: hq-b, token_envs: [DBB_REVERSE_E2E_TOKEN], allowed_targets: [db]}]
targets: {db: {address: %q}}
reverse:
  enabled: true
  peer_node_id: hq-b
  token_env: DBB_REVERSE_E2E_TOKEN
  transport: %s
  tunnels_per_endpoint: 2
  endpoints: [{id: primary, url: %q, priority: 1}]
  reconnect: {min_backoff: 50ms, max_backoff: 500ms}
%s`, backend, mode, dialURL, transport))
	p := &reversePair{public: NewNode(pub), db: NewNode(db), listen: listen, greeting: greeting, publicHTTP: httpAddr, publicConfig: pub}
	if err := p.public.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.public.Stop)
	if err := p.db.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.db.Stop)
	waitFor(t, 5*time.Second, "two tunnels registered", func() bool { return p.public.hub.Tunnels("plant") == 2 && p.db.tunnelCli.Live() == 2 })
	return p
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// dialDB connects a database client through the public node and checks the
// greeting the database sent first.
func dialDB(t *testing.T, p *reversePair) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", p.listen, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	got := make([]byte, len(p.greeting))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("greeting: %v", err)
	}
	if !bytes.Equal(got, p.greeting) {
		t.Fatalf("greeting = %q", got)
	}
	return c
}

func pattern(n int, seed byte) []byte {
	b := make([]byte, n)
	x := uint32(seed) + 1
	for i := range b {
		x = x*1664525 + 1013904223
		b[i] = byte(x >> 24)
	}
	return b
}

// exchange writes data and reads the echo concurrently, checking every byte.
func exchange(t *testing.T, c net.Conn, data []byte) {
	t.Helper()
	errc := make(chan error, 1)
	go func() {
		_, err := c.Write(data)
		errc <- err
	}()
	got := make([]byte, len(data))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("write: %v", err)
	}
	if sha256.Sum256(got) != sha256.Sum256(data) {
		t.Fatal("echo differs from what was sent")
	}
}

// Application -> public node -> tunnel the database side dialed -> database:
// a transaction-like request/response sequence and a large result in both
// directions pass byte for byte.
func TestReverseEndToEnd(t *testing.T) {
	p := startReversePair(t, "{enabled: false}")
	c := dialDB(t, p)
	defer c.Close()
	for i, q := range []string{"BEGIN", "INSERT INTO t VALUES (1)", "SELECT * FROM t FOR UPDATE", "COMMIT"} {
		exchange(t, c, append([]byte{byte(len(q)), 0, 0, byte(i)}, q...))
	}
	exchange(t, c, pattern(8<<20, 7))

	// Several concurrent sessions spread over the tunnels.
	var wg sync.WaitGroup
	// More concurrent sessions (and so new streams) than the per-IP
	// pre-authentication allowance: tunnel streams are counted per tunnel.
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := dialDB(t, p)
			defer c.Close()
			exchange(t, c, pattern(256<<10, byte(i)))
		}(i)
	}
	wg.Wait()
}

// One public node holds a reverse peer and a forward peer at the same time.
func TestReverseMixedWithForwardPeer(t *testing.T) {
	p := startReversePair(t, "{enabled: false}")
	backend, greeting := mysqlLikeBackend(t)
	fwdHTTP, fwdListen := freeAddr(t), freeAddr(t)
	dir := t.TempDir()
	branch := writeConfig(t, dir, "branch.yaml", fmt.Sprintf(`node: {id: branch-c}
http: {enabled: true, listen: %q}
auth:
  inbound_peers: [{id: hq2, token_envs: [DBB_REVERSE_E2E_TOKEN], allowed_targets: [db]}]
targets: {db: {address: %q}}
transport: {shutdown_timeout: 200ms}
`, fwdHTTP, backend))
	bn := NewNode(branch)
	if err := bn.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer bn.Stop()
	mixed := writeConfig(t, dir, "hq2.yaml", fmt.Sprintf(`node: {id: hq-2}
http: {enabled: true, listen: %q}
peers:
  - {id: plant2, mode: reverse, expected_node_id: plant-z, credential_ref: plant2, token_env: DBB_REVERSE_E2E_TOKEN}
  - {id: branch, expected_node_id: branch-c, credential_ref: branch, token_env: DBB_REVERSE_E2E_TOKEN, endpoints: [{id: e, url: "http://%s"}]}
listeners:
  - {id: branch-db, listen: %q, peer: branch, target: db}
transport: {shutdown_timeout: 200ms}
`, freeAddr(t), fwdHTTP, fwdListen))
	mn := NewNode(mixed)
	if err := mn.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer mn.Stop()
	c, err := net.DialTimeout("tcp", fwdListen, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(20 * time.Second))
	got := make([]byte, len(greeting))
	if _, err := io.ReadFull(c, got); err != nil || !bytes.Equal(got, greeting) {
		t.Fatalf("forward greeting %q %v", got, err)
	}
	exchange(t, c, pattern(1<<20, 9))
	// The reverse pair keeps working beside it.
	rc := dialDB(t, p)
	defer rc.Close()
	exchange(t, rc, pattern(1<<20, 10))
}

// A database side with the wrong token never registers a tunnel, and
// sessions to the reverse peer fail fast instead of hanging.
func TestReverseWrongTokenNeverRegisters(t *testing.T) {
	t.Setenv("DBB_REVERSE_E2E_TOKEN", reverseTestToken)
	t.Setenv("DBB_REVERSE_E2E_BAD", "not-the-right-token-0123456789ab")
	httpAddr, listen := freeAddr(t), freeAddr(t)
	dir := t.TempDir()
	pub := writeConfig(t, dir, "hq.yaml", fmt.Sprintf(`node: {id: hq-b}
http: {enabled: true, listen: %q}
peers: [{id: plant, mode: reverse, expected_node_id: plant-a, credential_ref: plant, token_env: DBB_REVERSE_E2E_TOKEN}]
listeners: [{id: l, listen: %q, peer: plant, target: db}]
transport: {shutdown_timeout: 200ms}
`, httpAddr, listen))
	db := writeConfig(t, dir, "plant.yaml", fmt.Sprintf(`node: {id: plant-a}
auth:
  inbound_peers: [{id: hq-b, token_envs: [DBB_REVERSE_E2E_BAD], allowed_targets: [db]}]
targets: {db: {address: "127.0.0.1:9"}}
reverse:
  enabled: true
  peer_node_id: hq-b
  token_env: DBB_REVERSE_E2E_BAD
  endpoints: [{id: primary, url: "http://%s"}]
  reconnect: {min_backoff: 20ms, max_backoff: 100ms}
transport: {shutdown_timeout: 200ms}
`, httpAddr))
	pn, dn := NewNode(pub), NewNode(db)
	if err := pn.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer pn.Stop()
	if err := dn.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer dn.Stop()
	waitFor(t, 5*time.Second, "refused handshakes", func() bool { return pn.hub.Rejected() >= 3 })
	if pn.hub.Tunnels("plant") != 0 || dn.tunnelCli.Live() != 0 {
		t.Fatal("tunnel registered with a wrong token")
	}
	c, err := net.DialTimeout("tcp", listen, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	if n, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatalf("read %d bytes through a peer without tunnel", n)
	}
}

// The database side has no listening socket at all: reverse.enabled with
// http.enabled false serves only the tunnel.
func TestReverseDatabaseSideListensNowhere(t *testing.T) {
	p := startReversePair(t, "{enabled: false}")
	if p.db.egressSrv.Addr() != nil {
		t.Fatalf("database side listens on %v", p.db.egressSrv.Addr())
	}
}

// A forced tunnel loss is a carrier loss: with recovery enabled the session
// resumes within grace over the reconnected tunnels, with nothing lost or
// duplicated in either direction.
func TestReverseTunnelLossRecovers(t *testing.T) {
	for _, side := range []string{"public", "database"} {
		t.Run(side, func(t *testing.T) {
			p := startReversePair(t, "{enabled: true, grace: 20s, barrier_timeout: 2s}")
			tunnelLossRecovers(t, p, side)
		})
	}
}

// tunnelLossRecovers cuts every tunnel on side while a recoverable session
// streams 4 MiB, and checks that nothing is lost or duplicated.
func tunnelLossRecovers(t *testing.T, p *reversePair, side string) {
	t.Helper()
	c := dialDB(t, p)
	defer c.Close()
	exchange(t, c, pattern(256<<10, 1))

	// Keep writing while the tunnels are cut.
	data := pattern(4<<20, 2)
	errc := make(chan error, 1)
	go func() {
		for off := 0; off < len(data); off += 64 << 10 {
			if _, err := c.Write(data[off : off+64<<10]); err != nil {
				errc <- err
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
		errc <- nil
	}()
	time.Sleep(30 * time.Millisecond)
	var n int
	if side == "public" {
		n = p.public.hub.CloseTunnels("plant")
	} else {
		n = p.db.tunnelCli.CloseTunnels()
	}
	if n == 0 {
		t.Fatal("no tunnel to cut")
	}
	got := make([]byte, len(data))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("read after tunnel loss: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("write after tunnel loss: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data lost or duplicated across the tunnel loss")
	}
	waitFor(t, 5*time.Second, "tunnels back", func() bool { return p.public.hub.Tunnels("plant") == 2 })
	exchange(t, c, pattern(128<<10, 3))
}
