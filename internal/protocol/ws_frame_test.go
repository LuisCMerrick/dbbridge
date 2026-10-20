package protocol

import "testing"

func TestWSReserved(t *testing.T) {
	b := EncodeWSFrame(1, 0, []byte("x"))
	b[2] = 1
	if _, _, _, err := DecodeWSFrame(b); err == nil {
		t.Fatal("nonzero reserved bytes accepted")
	}
}
func TestWSZeroLength(t *testing.T) {
	b := EncodeWSFrame(1, 0, nil)
	if _, _, _, err := DecodeWSFrame(b); err == nil {
		t.Fatal("zero-length WS DATA accepted")
	}
}
func TestWSOverflow(t *testing.T) {
	b := EncodeWSFrame(1, ^uint64(0), []byte("x"))
	if _, _, _, err := DecodeWSFrame(b); err == nil {
		t.Fatal("offset+length overflow accepted")
	}
}
