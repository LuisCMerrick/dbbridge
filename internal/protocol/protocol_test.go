package protocol_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"dbbridge/internal/protocol"
)

func TestFormatAndParseSSEData(t *testing.T) {
	payload := make([]byte, 1024*64) // 64 KiB
	_, _ = rand.Read(payload)

	formatted := protocol.FormatSSEData(12345, payload)

	offset, decoded, err := protocol.ParseSSEData(formatted)
	if err != nil {
		t.Fatalf("ParseSSEData failed: %v", err)
	}

	if offset != 12345 {
		t.Errorf("expected offset 12345, got %d", offset)
	}

	if !bytes.Equal(decoded, payload) {
		t.Errorf("decoded payload does not match original")
	}
}

func TestParseSSEDataCorruptedSHA(t *testing.T) {
	line := "100:4:0000000000000000000000000000000000000000000000000000000000000000:dGVzdA=="
	_, _, err := protocol.ParseSSEData(line)
	if err == nil {
		t.Fatalf("expected sha256 mismatch error, got nil")
	}
}
