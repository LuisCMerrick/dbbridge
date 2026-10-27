package ingress

import "testing"

func TestFramePoolClasses(t *testing.T) {
	for _, n := range []int{0, 3, 1000, 4095, 3 << 12, 2 << 20} {
		if _, ok := frameClass(n); ok {
			t.Fatalf("%d pooled", n)
		}
	}
	for _, n := range []int{4 << 10, 32 << 10, 256 << 10, 1 << 20} {
		if _, ok := frameClass(n); !ok {
			t.Fatalf("%d not pooled", n)
		}
		b := getFrameBuf(n)
		if len(b) != n || cap(b) != n {
			t.Fatalf("%d: len %d cap %d", n, len(b), cap(b))
		}
		putFrameBuf(b[:10])
	}
}

// A chunk whose HTTP send failed must not return its buffer to the pool.
func TestTaintedChunkIsNotRecycled(t *testing.T) {
	s := &ingressSession{}
	s.m = &IngressManager{}
	buf := getFrameBuf(8 << 10)
	f := &retainedChunk{data: buf, pooled: true, tainted: true}
	s.releaseFrameLocked(f)
	for i := 0; i < 64; i++ {
		if b := getFrameBuf(8 << 10); &b[0] == &buf[0] {
			t.Fatal("tainted buffer recycled")
		}
	}
}
