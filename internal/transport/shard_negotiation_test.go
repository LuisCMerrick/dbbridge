package transport

import (
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
)

// SSE shards are created as the rotation reaches them, never beyond
// config.MaxH2PoolSize.
func TestSSEShardsCreatedOnDemand(t *testing.T) {
	c := NewTripleTransportClientForURL("http://127.0.0.1:1", "token", &config.TransportConfig{H2PoolSize: 4, ControlRequestTimeout: time.Second}, "instance", true)
	defer c.Close()
	c.nextSSEShard()
	if len(c.sseShards) != 1 {
		t.Fatalf("first subscription created %d shards", len(c.sseShards))
	}
	for range 7 {
		c.nextSSEShard()
	}
	if len(c.sseShards) != 4 {
		t.Fatalf("shards after 8 subscriptions: %d, want 4", len(c.sseShards))
	}
	big := NewTripleTransportClientForURL("http://127.0.0.1:1", "token", &config.TransportConfig{H2PoolSize: 4096, ControlRequestTimeout: time.Second}, "instance", true)
	defer big.Close()
	for range 200 {
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
	base := protocol.CreateSessionResponse{ProtocolVersion: "1.5", InstanceID: "owner", SessionID: "0123456789abcdef0123456789abcdef", SessionToken: "secret", Epoch: "1", Transport: protocol.TransportMode{Mode: "ws"}, UnattachedTimeoutMs: 1000,
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
