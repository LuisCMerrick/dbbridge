package stream

import (
	"bytes"
	"testing"
)

func TestSegmentsMatchReadRange(t *testing.T) {
	w := NewByteWindow(10, 1<<20)
	w.SetChunkSize(5)
	src := []byte("abcdefghijklmnopqrstuvwxyz")
	if err := w.Append(10, src[:13]); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendOwned(23, append([]byte(nil), src[13:]...)); err != nil {
		t.Fatal(err)
	}
	w.Acknowledge(12) // partial ACK inside the first block
	for off := uint64(12); off <= 36; off++ {
		for limit := 0; limit <= 30; limit++ {
			want, wantNext, _ := w.ReadRange(off, limit)
			segs, next, ok := w.Segments(off, limit)
			if !ok || next != wantNext || !bytes.Equal(bytes.Join(segs, nil), want) {
				t.Fatalf("off %d limit %d: got %q next %d, want %q next %d", off, limit, bytes.Join(segs, nil), next, want, wantNext)
			}
		}
	}
	if _, _, ok := w.Segments(11, 4); ok {
		t.Fatal("acknowledged offset accepted")
	}
	// Views stay intact after the range is acknowledged and the window reset.
	segs, _, _ := w.Segments(20, 10)
	w.Acknowledge(36)
	w.Reset()
	if got := string(bytes.Join(segs, nil)); got != "klmnopqrst" {
		t.Fatalf("view changed after release: %q", got)
	}
}
