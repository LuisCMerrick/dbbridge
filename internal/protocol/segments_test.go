package protocol

import (
	"bytes"
	"math/rand"
	"testing"
)

func splitRandom(r *rand.Rand, p []byte) [][]byte {
	var segs [][]byte
	for len(p) > 0 {
		k := 1 + r.Intn(min(len(p), 7))
		segs = append(segs, p[:k])
		p = p[k:]
	}
	return segs
}

func TestSegmentEncodersMatchContiguous(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for size := 1; size < 200; size++ {
		p := make([]byte, size)
		r.Read(p)
		segs := splitRandom(r, p)
		want := EncodeWSFrame(3, 99, p)
		got, err := EncodeWSFrameSegments(3, 99, segs)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("ws size %d: %v", size, err)
		}
		h, err := EncodeWSFrameHeader(3, 99, p)
		if err != nil || !bytes.Equal(append(h[:], p...), want) {
			t.Fatalf("ws header size %d: %v", size, err)
		}
		line, err := AppendSSEDataSegments([]byte("x"), 99, segs)
		if err != nil || string(line) != "x"+FormatSSEData(99, p) {
			t.Fatalf("sse size %d: %v %q", size, err, line)
		}
		off, payload, err := ParseSSEData(string(line[1:]))
		if err != nil || off != 99 || !bytes.Equal(payload, p) {
			t.Fatalf("sse round trip size %d: %v", size, err)
		}
	}
	if _, err := EncodeWSFrameSegments(0, 0, [][]byte{{1}}); err == nil {
		t.Fatal("epoch 0 accepted")
	}
	if _, err := AppendSSEDataSegments(nil, 0, nil); err == nil {
		t.Fatal("empty payload accepted")
	}
}
