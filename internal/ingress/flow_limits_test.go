package ingress

import (
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
)

func TestFlowAckThresholds(t *testing.T) {
	plain := protocol.SessionLimits{UpMaxInflightFrames: 8, DownBufferBytes: 4 << 20}
	if got := upFramesAckThreshold(plain); got != 12 {
		t.Fatalf("plain up threshold %d", got)
	}
	if got := downFramesAckThreshold(plain); got != 32 {
		t.Fatalf("plain down threshold %d", got)
	}
	adv := plain
	adv.UpReceiptLimit, adv.DownMaxFrames = 64, 512
	if got := upFramesAckThreshold(adv); got != 12 {
		t.Fatalf("advertised up threshold %d", got)
	}
	adv.UpReceiptLimit, adv.DownMaxFrames = 10, 40
	if got := upFramesAckThreshold(adv); got != 5 {
		t.Fatalf("small receipt limit up threshold %d", got)
	}
	if got := downFramesAckThreshold(adv); got != 10 {
		t.Fatalf("small frame cap down threshold %d", got)
	}
	adv.DownMaxFrames = 2
	if got := downFramesAckThreshold(adv); got != 1 {
		t.Fatalf("tiny frame cap down threshold %d", got)
	}
	if got := downBytesAckThreshold(1<<20, plain); got != 1<<20 {
		t.Fatalf("byte threshold %d", got)
	}
	small := protocol.SessionLimits{DownBufferBytes: 512 << 10}
	if got := downBytesAckThreshold(1<<20, small); got != 256<<10 {
		t.Fatalf("byte threshold under a small negotiated window %d", got)
	}
}

func flowRoundTrips(t *testing.T, c net.Conn, n int) time.Duration {
	t.Helper()
	buf := make([]byte, 1)
	var worst time.Duration
	for i := 0; i < n; i++ {
		t0 := time.Now()
		c.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := c.Write([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(c, buf); err != nil {
			t.Fatal(i, err)
		}
		worst = max(worst, time.Since(t0))
	}
	return worst
}

// X-DP-3 protocol: against a current egress the session learns the egress
// flow limits; against an egress that ignores the extension (simulated by
// dropping the header) it falls back to the built-in thresholds. Either way
// small-frame round trips never wait for the ACK timer.
func TestFlowLimitsNegotiationAndFallback(t *testing.T) {
	slow := func(c *config.Config) { c.Transport.AckMaxDelayRaw = "2s" }
	strip := func(w http.ResponseWriter, r *http.Request, _ http.Handler) bool {
		r.Header.Del(protocol.HeaderFeatures)
		return false
	}
	for _, mode := range []string{"ws", "http-sse"} {
		for _, old := range []bool{false, true} {
			name := mode + "/current-egress"
			var hook fix2Hook
			if old {
				name, hook = mode+"/old-egress", strip
			}
			t.Run(name, func(t *testing.T) {
				e, _ := fix2Setup(t, mode, true, slow, nil, hook)
				flowRoundTrips(t, e.conn, 1)
				s := fix2Session(t, e.m)
				s.mu.Lock()
				lim := s.limits
				s.mu.Unlock()
				if old && (lim.UpReceiptLimit != 0 || lim.DownMaxFrames != 0) {
					t.Fatalf("old egress cannot advertise limits: %+v", lim)
				}
				if !old && (lim.UpReceiptLimit < 2*lim.UpMaxInflightFrames+8 || lim.DownMaxFrames != 512) {
					t.Fatalf("advertised limits missing: %+v", lim)
				}
				if worst := flowRoundTrips(t, e.conn, 300); worst > time.Second {
					t.Fatalf("round trip stalled %v waiting for the ACK timer", worst)
				}
			})
		}
	}
}

// A negotiated downstream window smaller than ack_batch_bytes is
// acknowledged at half the window instead of waiting for the ACK timer.
func TestFlowSmallNegotiatedWindowAcked(t *testing.T) {
	slow := func(c *config.Config) { c.Transport.AckMaxDelayRaw = "2s" }
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	const total = 8 << 20
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				chunk := make([]byte, 64<<10)
				for sent := 0; sent < total; sent += len(chunk) {
					if _, err := c.Write(chunk); err != nil {
						return
					}
				}
				io.Copy(io.Discard, c)
			}()
		}
	}()
	egressCfg := func(b *config.Config) {
		b.Targets = map[string]config.TargetConfig{"db": {Address: ln.Addr().String()}}
		b.Transport.BufferPerDirectionRaw = "512KiB"
	}
	e, _ := fix2Setup(t, "ws", true, slow, egressCfg, nil)
	if _, err := e.conn.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	e.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	if n, err := io.CopyN(io.Discard, e.conn, total); err != nil {
		t.Fatal(n, err)
	}
	if d := time.Since(t0); d > 4*time.Second {
		t.Fatalf("8 MiB through a 512 KiB window took %v (ACK timer bound)", d)
	}
}

// A cleanup may start while someone already waits for idle (a session timer
// firing during CloseContext); the waiter must see it.
func TestCleanupTrackerLateAdd(t *testing.T) {
	var c cleanupTracker
	select {
	case <-c.idle():
	default:
		t.Fatal("empty tracker must be idle")
	}
	c.add()
	w := c.idle()
	c.add()
	c.done()
	select {
	case <-w:
		t.Fatal("idle while a cleanup still runs")
	default:
	}
	c.done()
	select {
	case <-w:
	case <-time.After(time.Second):
		t.Fatal("not idle after the last cleanup")
	}
}
