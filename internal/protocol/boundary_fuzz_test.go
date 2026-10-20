package protocol

import (
	"bytes"
	"testing"
)

func FuzzBoundaryWSDecode(f *testing.F) {
	f.Add(EncodeWSFrame(1, 0, []byte{0, 255, 10, 13}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 2<<20 {
			return
		}
		ep, off, p, err := DecodeWSFrame(b)
		if err == nil && !bytes.Equal(b, EncodeWSFrame(ep, off, p)) {
			t.Fatal("accepted non-roundtrippable frame")
		}
	})
}
func FuzzBoundarySSEDecode(f *testing.F) {
	f.Add(FormatSSEData(0, []byte{0, 255, 10, 13}))
	f.Add("")
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 2<<20 {
			return
		}
		off, p, err := ParseSSEData(s)
		if err == nil {
			off2, p2, e := ParseSSEData(FormatSSEData(off, p))
			if e != nil || off != off2 || !bytes.Equal(p, p2) {
				t.Fatal("frame roundtrip")
			}
		}
	})
}
