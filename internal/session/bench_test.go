package session

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"testing"
	"time"
)

// BenchmarkIngestUpFrame measures the egress upstream ingest of one verified
// 64 KiB frame (digest check, admission, enqueue) and its write to the backend.
func BenchmarkIngestUpFrame(b *testing.B) {
	const size = 64 << 10
	a, c := net.Pipe()
	go io.Copy(io.Discard, c)
	s := NewSession("p", "db", "inst", a, 1<<20, 4<<20, 8, 64, false)
	b.Cleanup(func() { s.Close(); c.Close() })
	s.SetTransportMode("ws")
	id, _ := s.ReserveStream(1, "ws")
	s.ActivateDataPlane(id)
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i)
	}
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])
	b.SetBytes(size)
	b.ReportAllocs()
	var off uint64
	for i := 0; i < b.N; i++ {
		for {
			_, _, _, err := s.IngestUpFrame(1, off, size, sha, data)
			if err == nil {
				break
			}
			if err != ErrWindowExceeded {
				b.Fatal(err)
			}
			// Confirm what the backend writer has written, as the ingress
			// would, so receipts and the window drain.
			s.mu.Lock()
			ack := s.upAckOffset
			s.mu.Unlock()
			if e := s.ApplyPiggyback(1, id, 0, ack); e != nil {
				b.Fatal(e, ack, off)
			}
			time.Sleep(10 * time.Microsecond)
		}
		off += size
	}
}
