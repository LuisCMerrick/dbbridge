package config

import (
	"strings"
	"testing"

	"dbbridge/internal/protocol"
)

func TestUserAgentDefaultAndValidation(t *testing.T) {
	if DefaultUserAgent != "DBBridge/"+protocol.ProtocolVersionString {
		t.Fatalf("DefaultUserAgent = %q", DefaultUserAgent)
	}
	for _, ok := range []string{"DBBridge/1.x", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "curl/8.0"} {
		if err := validUserAgent(ok); err != nil {
			t.Fatalf("%q rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"a\r\nX-Evil: 1", "tab\there", " lead", "trail ", "caf\u00e9", strings.Repeat("a", 257)} {
		if err := validUserAgent(bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}
