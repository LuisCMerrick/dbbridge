package protocol

import (
	"math"
	"strings"
	"testing"
)

// SSE offsets and lengths must be canonical decimal like every other offset.
func TestSSEParseRejectsNonCanonical(t *testing.T) {
	line := FormatSSEData(5, []byte("x"))
	rest := line[strings.Index(line, ":")+1:]
	rest = rest[strings.Index(rest, ":"):]
	for _, bad := range []string{"0005:1" + rest, "+5:1" + rest, "5:+1" + rest, "5:01" + rest, ":1" + rest, "5:" + rest} {
		if _, _, err := ParseSSEData(bad); err == nil {
			t.Errorf("accepted %q", bad[:8])
		}
	}
	if off, p, err := ParseSSEData(line); err != nil || off != 5 || string(p) != "x" {
		t.Fatalf("canonical line: off=%d p=%q err=%v", off, p, err)
	}
}

// Frame encoding enforces the same range the decoder accepts.
func TestEncodeWSFrameLengthCheck(t *testing.T) {
	if _, err := EncodeWSFrameChecked(1, 0, make([]byte, MaxFramePayload+1)); err == nil {
		t.Fatal("oversized payload accepted")
	}
	if _, err := EncodeWSFrameChecked(1, 0, nil); err == nil {
		t.Fatal("empty payload accepted")
	}
	if _, err := EncodeWSFrameChecked(1, math.MaxUint64, []byte("x")); err == nil {
		t.Fatal("overflowing offset accepted")
	}
	b, err := EncodeWSFrameChecked(1, 7, make([]byte, MaxFramePayload))
	if err != nil {
		t.Fatal(err)
	}
	if _, off, p, err := DecodeWSFrame(b); err != nil || off != 7 || len(p) != MaxFramePayload {
		t.Fatalf("round trip: off=%d len=%d err=%v", off, len(p), err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("EncodeWSFrame did not reject an oversized payload")
		}
	}()
	EncodeWSFrame(1, 0, make([]byte, MaxFramePayload+1))
}
