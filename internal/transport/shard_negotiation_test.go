package transport

import (
	"net/http"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
)

func firstShard(c *TripleTransportClient) *http.Client {
	cl, done := c.nextSSEShard()
	done()
	return cl
}

// SSE subscriptions share one shard (one h2 connection) until it carries
// sseShardSpread of them; only then is another shard opened, never beyond
// h2_pool_size or config.MaxH2PoolSize. Ended subscriptions free their slot,
// so sequential sessions never open a second connection.
func TestSSEShardsCreatedOnDemand(t *testing.T) {
	c := NewTripleTransportClientForURL("http://127.0.0.1:1", "token", &config.TransportConfig{H2PoolSize: 4, ControlRequestTimeout: time.Second}, "instance", true)
	defer c.Close()
	for range 50 {
		firstShard(c)
	}
	if len(c.sseShards) != 1 {
		t.Fatalf("sequential subscriptions created %d shards, want 1", len(c.sseShards))
	}
	var dones []func()
	for range sseShardSpread {
		_, d := c.nextSSEShard()
		dones = append(dones, d)
	}
	if len(c.sseShards) != 1 {
		t.Fatalf("%d concurrent subscriptions created %d shards, want 1", sseShardSpread, len(c.sseShards))
	}
	cl, d := c.nextSSEShard()
	dones = append(dones, d)
	if len(c.sseShards) != 2 || cl != c.sseShards[1] {
		t.Fatalf("subscription beyond the spread did not open shard 2 (%d shards)", len(c.sseShards))
	}
	for range 4 * sseShardSpread {
		_, d := c.nextSSEShard()
		dones = append(dones, d)
	}
	if len(c.sseShards) != 4 {
		t.Fatalf("shards under load: %d, want 4", len(c.sseShards))
	}
	for _, d := range dones {
		d()
		d() // idempotent
	}
	for i, l := range c.sseLoad {
		if l != 0 {
			t.Fatalf("shard %d load %d after all subscriptions ended", i, l)
		}
	}
	// Least-loaded selection spreads new subscriptions over existing shards.
	seen := map[*http.Client]bool{}
	for range 4 {
		cl, d := c.nextSSEShard()
		defer d()
		seen[cl] = true
	}
	if len(seen) != 4 {
		t.Fatalf("4 concurrent subscriptions used %d of 4 idle shards", len(seen))
	}
	big := NewTripleTransportClientForURL("http://127.0.0.1:1", "token", &config.TransportConfig{H2PoolSize: 4096, ControlRequestTimeout: time.Second}, "instance", true)
	defer big.Close()
	for range (config.MaxH2PoolSize + 2) * sseShardSpread {
		big.nextSSEShard()
	}
	if len(big.sseShards) != config.MaxH2PoolSize {
		t.Fatalf("shards %d, want at most %d", len(big.sseShards), config.MaxH2PoolSize)
	}
}

// A create response whose frame size does not fit the negotiated window and
// buffer is a protocol error, even when every field is in range by itself.
func TestCreateResponseFrameMustFitWindow(t *testing.T) {
	c := NewTripleTransportClientForURL("http://127.0.0.1:1", "token", &config.TransportConfig{}, "ingress", true)
	defer c.Close()
	requested := protocol.SessionLimits{MaxFrameSize: 4096, UpMaxInflightFrames: 8, UpWindowBytes: 8192, DownBufferBytes: 16384}
	base := protocol.CreateSessionResponse{ProtocolVersion: "1.4", InstanceID: "owner", SessionID: "0123456789abcdef0123456789abcdef", SessionToken: "secret", Epoch: "1", Transport: protocol.TransportMode{Mode: "ws"}, UnattachedTimeoutMs: 1000,
		Limits: protocol.SessionLimits{MaxFrameSize: 4096, UpMaxInflightFrames: 8, UpWindowBytes: 4096, DownBufferBytes: 4096, UpReceiptLimit: 64, DownMaxFrames: 512}}
	ok := base
	if err := c.validateCreateResponse(&ok, requested, protocol.RecoveryNegotiation{}, "owner", "ws"); err != nil {
		t.Fatalf("frame equal to window and buffer rejected: %v", err)
	}
	for _, lim := range []protocol.SessionLimits{
		{MaxFrameSize: 4096, UpMaxInflightFrames: 8, UpWindowBytes: 1, DownBufferBytes: 1, UpReceiptLimit: 64, DownMaxFrames: 512},
		{MaxFrameSize: 4096, UpMaxInflightFrames: 8, UpWindowBytes: 4096, DownBufferBytes: 4095, UpReceiptLimit: 64, DownMaxFrames: 512},
	} {
		r := base
		r.Limits = lim
		if err := c.validateCreateResponse(&r, requested, protocol.RecoveryNegotiation{}, "owner", "ws"); err == nil {
			t.Errorf("accepted %+v", lim)
		}
	}
}
