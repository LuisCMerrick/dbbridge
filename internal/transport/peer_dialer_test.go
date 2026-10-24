package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/resource"
)

func reversePeerClient(t *testing.T) (*TripleTransportClient, *resource.Manager) {
	t.Helper()
	cfg := &config.Config{Node: config.NodeConfig{ID: "hq-b"}, Transport: config.TransportConfig{ControlRequestTimeoutRaw: "2s"}}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	p := &config.PeerConfig{ID: "plant", Mode: config.PeerModeReverse, ExpectedNodeID: "plant-a", Token: "token"}
	c := NewTripleTransportClient(p, &cfg.Transport, "ai")
	r := resource.NewManager(256<<20, 32<<20, 32<<20, 16)
	c.SetResources(r)
	t.Cleanup(c.Close)
	return c, r
}

// A reverse peer has no endpoints: its client must not need one, and every
// dial fails fast until a tunnel is attached.
func TestReversePeerClientWithoutTunnelFailsFast(t *testing.T) {
	c, r := reversePeerClient(t)
	if c.URL() != ReverseEndpointURL || c.expectedNodeID != "plant-a" {
		t.Fatalf("url=%q expected=%q", c.URL(), c.expectedNodeID)
	}
	start := time.Now()
	_, err := c.controlClient.Get(ReverseEndpointURL + "/v1/peer/probe")
	if !errors.Is(err, ErrTunnelUnavailable) {
		t.Fatalf("err = %v, want ErrTunnelUnavailable", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("dial without tunnel did not fail fast")
	}
	// A failed dial returns every budget slot it took.
	if c.shared.controlConnections.Used() != 0 || r.ControlConnections.Used() != 0 {
		t.Fatalf("slots leaked: peer=%d global=%d", c.shared.controlConnections.Used(), r.ControlConnections.Used())
	}
}

// With a tunnel attached, HTTP requests of the reverse peer run over streams
// from the opener (the endpoint address is ignored) and are charged to the
// same per-peer and global connection budgets as TCP dials.
func TestReversePeerDialsThroughStreamOpener(t *testing.T) {
	var host atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host.Store(r.Host)
		_, _ = io.WriteString(w, "over-tunnel")
	}))
	defer srv.Close()
	c, r := reversePeerClient(t)
	var opened atomic.Int64
	c.SetPeerDialer(TunnelDialer{Timeout: time.Second, Open: func(ctx context.Context) (net.Conn, error) {
		opened.Add(1)
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Listener.Addr().String())
	}})
	resp, err := c.controlClient.Get(ReverseEndpointURL + "/hello")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "over-tunnel" || opened.Load() != 1 {
		t.Fatalf("body=%q opened=%d", body, opened.Load())
	}
	if h, _ := host.Load().(string); h != "reverse.tunnel.invalid" {
		t.Fatalf("Host = %q", h)
	}
	if c.shared.controlConnections.Used() != 1 || r.ControlConnections.Used() != 1 {
		t.Fatalf("stream not charged to budgets: peer=%d global=%d", c.shared.controlConnections.Used(), r.ControlConnections.Used())
	}
	c.Close()
	deadline := time.Now().Add(time.Second)
	for (c.shared.controlConnections.Used() != 0 || r.ControlConnections.Used() != 0) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if c.shared.controlConnections.Used() != 0 || r.ControlConnections.Used() != 0 {
		t.Fatal("closing the stream did not release its budget slots")
	}
}

func TestTunnelDialerTimeout(t *testing.T) {
	d := TunnelDialer{Timeout: 50 * time.Millisecond, Open: func(ctx context.Context) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	start := time.Now()
	if _, err := d.DialPeer(context.Background(), "tcp", "ignored:80"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("timeout not applied")
	}
}

// Forward peers keep the TCP dial bounded by peer_dial_timeout.
func TestForwardPeerUsesTCPDialer(t *testing.T) {
	c := NewTripleTransportClientForURL("http://127.0.0.1:1", "t", &config.TransportConfig{PeerDialTimeout: 750 * time.Millisecond, DialTimeout: time.Second}, "ai", true)
	d, ok := c.peerDialer().(TCPDialer)
	if !ok || d.Timeout != 750*time.Millisecond || d.KeepAlive != 30*time.Second {
		t.Fatalf("dialer = %#v", c.peerDialer())
	}
	c.SetPeerDialer(TunnelDialer{})
	if _, ok := c.WithURL("http://127.0.0.1:2").peerDialer().(TunnelDialer); !ok {
		t.Fatal("dialer is not shared by the peer's endpoint clients")
	}
	c.SetPeerDialer(nil)
	if _, ok := c.peerDialer().(TCPDialer); !ok {
		t.Fatal("nil dialer did not restore TCP")
	}
}
