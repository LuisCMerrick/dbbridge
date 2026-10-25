package config

import (
	"fmt"

	"dbbridge/internal/protocol"
)

// DefaultUserAgent is the User-Agent DBBridge sends to its peers unless
// transport.user_agent sets another one.
const DefaultUserAgent = "DBBridge/" + protocol.ProtocolVersionString

// maxUserAgentLen bounds transport.user_agent (peers reject large headers).
const maxUserAgentLen = 256

// validUserAgent accepts printable ASCII (RFC 9110 field-value characters
// without obs-text), so the value can never split or corrupt a header.
func validUserAgent(ua string) error {
	if len(ua) > maxUserAgentLen {
		return fmt.Errorf("transport.user_agent is longer than %d bytes", maxUserAgentLen)
	}
	for i := 0; i < len(ua); i++ {
		if c := ua[i]; c < 0x20 || c > 0x7e {
			return fmt.Errorf("transport.user_agent may contain only printable ASCII characters (byte %d is 0x%02x)", i, c)
		}
	}
	if ua[0] == ' ' || ua[len(ua)-1] == ' ' {
		return fmt.Errorf("transport.user_agent must not start or end with a space")
	}
	return nil
}
