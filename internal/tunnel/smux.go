package tunnel

import (
	"dbbridge/internal/config"

	"github.com/xtaci/smux"
)

// smuxConfig derives the stream multiplexer settings from the transport
// configuration. Protocol v2 gives every stream its own window. Keepalive
// follows heartbeat and failure_detect_timeout, so a blackholed tunnel is
// dropped as fast as a blackholed carrier. The receive buffer (the bytes
// that may wait unread across all streams of a tunnel) is charged to the
// codec budget by the caller; a stream may hold at most half of it, and no
// more than buffer_per_direction.
func smuxConfig(t *config.TransportConfig) *smux.Config {
	c := smux.DefaultConfig()
	c.Version = 2
	if t.Heartbeat > 0 {
		c.KeepAliveInterval = t.Heartbeat
	}
	if t.FailureDetectTimeout >= c.KeepAliveInterval {
		c.KeepAliveTimeout = t.FailureDetectTimeout
	} else {
		c.KeepAliveTimeout = 3 * c.KeepAliveInterval
	}
	c.MaxReceiveBuffer = int(ReceiveBuffer(t))
	c.MaxStreamBuffer = int(min(max(t.BufferPerDirection, 64<<10), int64(c.MaxReceiveBuffer/2)))
	return c
}

// ReceiveBuffer is the smux receive buffer of one tunnel: a sixteenth of
// codec_buffer_limit, at least 1 MiB and at most twice buffer_per_direction
// (2 MiB with the defaults).
func ReceiveBuffer(t *config.TransportConfig) int64 {
	b := t.CodecBufferLimit / 16
	if hi := 2 * t.BufferPerDirection; hi > 0 {
		b = min(b, hi)
	}
	return min(max(b, 1<<20), 1<<30)
}
