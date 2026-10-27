package stream

import "testing"

// BenchmarkByteWindowDownstream models the egress downstream path: the backend
// reader adopts 32 KiB reads (AppendOwned), the sender takes 256 KiB frames
// back out and the peer's ACK releases them (Acknowledge). "copy" reads
// frames with ReadRange, "segments" with the zero-copy Segments the egress
// sender uses.
func BenchmarkByteWindowDownstream(b *testing.B) {
	const chunk, frame = 32 << 10, 256 << 10
	for _, mode := range []string{"copy", "segments"} {
		b.Run(mode, func(b *testing.B) {
			w := NewByteWindow(0, 4<<20)
			src := make([][]byte, frame/chunk)
			b.SetBytes(frame)
			b.ReportAllocs()
			var off uint64
			for i := 0; i < b.N; i++ {
				for k := range src {
					// Owned buffers are reused across iterations only because
					// nothing reads them after Acknowledge in this benchmark.
					if src[k] == nil {
						src[k] = make([]byte, chunk)
					}
					if err := w.AppendOwned(off+uint64(k*chunk), src[k]); err != nil {
						b.Fatal(err)
					}
				}
				var next uint64
				if mode == "copy" {
					var data []byte
					data, next, _ = w.ReadRange(off, frame)
					if len(data) != frame {
						b.Fatal(len(data))
					}
				} else {
					var segs [][]byte
					segs, next, _ = w.Segments(off, frame)
					if len(segs) != frame/chunk {
						b.Fatal(len(segs))
					}
				}
				w.Acknowledge(next)
				off = next
			}
		})
	}
}
