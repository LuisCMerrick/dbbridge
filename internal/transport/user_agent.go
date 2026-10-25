package transport

import (
	"net/http"

	"dbbridge/internal/config"
)

// SetUserAgent sets the User-Agent of an outbound request or WebSocket
// handshake: transport.user_agent, or config.DefaultUserAgent when the
// configuration was not validated (tests). It replaces Go's default
// "Go-http-client/1.1" / "Go-http-client/2.0".
func SetUserAgent(h http.Header, t *config.TransportConfig) {
	ua := config.DefaultUserAgent
	if t != nil && t.UserAgent != "" {
		ua = t.UserAgent
	}
	h.Set("User-Agent", ua)
}
