package tunnel

import (
	"math"
	"sync"

	"dbbridge/internal/config"
	"dbbridge/internal/resource"

	"github.com/xtaci/smux"
)

// smuxConfig derives the stream multiplexer settings from the transport
// configuration. Protocol v2 gives every stream its own window
// (TunnelStreamWindow), so a stream never holds more unread bytes than that.
// The session-wide receive bucket is sized for MaxStreamsPerTunnel full
// windows: smux stops reading the whole tunnel once the bucket is empty, and
// a bucket smaller than the windows of the streams in use would let slow
// data streams stall every other stream, control requests included. Memory
// is charged per open stream instead (see chargeStream). Keepalive follows
// heartbeat and failure_detect_timeout, so a blackholed tunnel is dropped as
// fast as a blackholed carrier.
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
	w := ReceiveBuffer(t)
	c.MaxStreamBuffer = int(w)
	c.MaxReceiveBuffer = int(min(w*config.MaxStreamsPerTunnel, math.MaxInt32))
	return c
}

// ReceiveBuffer is the receive window of one tunnel stream, charged to the
// codec budget for as long as the stream is open.
func ReceiveBuffer(t *config.TransportConfig) int64 { return t.TunnelStreamWindow() }

// tryCodec takes n bytes of codec, leaving keep() free when keep is set (a
// role reserve the charge must not eat into). A nil budget always succeeds.
func tryCodec(b *resource.Budget, n int64, keep func() int64) (release func(), ok bool) {
	if b == nil {
		return func() {}, true
	}
	k := int64(0)
	if keep != nil {
		k = keep()
	}
	if !b.TryKeep(n, k) {
		return nil, false
	}
	var once sync.Once
	return func() { once.Do(func() { b.Put(n) }) }, true
}
