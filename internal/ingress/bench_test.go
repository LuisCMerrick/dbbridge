package ingress

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

// BenchmarkForward pushes 1 MiB per op through a loopback ingress -> egress ->
// echo backend and reads it back: both directions of the data plane, codec,
// digest and ACK paths included.
func BenchmarkForward(b *testing.B) {
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	b.Cleanup(func() { slog.SetDefault(prev) })
	for _, mode := range []string{"ws", "http-sse"} {
		b.Run(mode, func(b *testing.B) {
			e, _ := dataplaneSetup(b, mode, true, nil, nil, nil)
			c := e.conn
			const op = 1 << 20
			buf := make([]byte, op)
			for i := range buf {
				buf[i] = byte(i * 7)
			}
			// Warm up the session and its adaptive frame size.
			roundTrip(b, c, buf)
			b.SetBytes(2 * op)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				roundTrip(b, c, buf)
			}
		})
	}
}

func roundTrip(b testing.TB, c net.Conn, buf []byte) {
	c.SetDeadline(time.Now().Add(30 * time.Second))
	done := make(chan error, 1)
	go func() { _, err := c.Write(buf); done <- err }()
	if _, err := io.CopyN(io.Discard, c, int64(len(buf))); err != nil {
		b.Fatal(err)
	}
	if err := <-done; err != nil {
		b.Fatal(err)
	}
}
