package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
)

// Every request and WebSocket handshake to a peer carries the configured
// User-Agent instead of Go's "Go-http-client/x".
func TestUserAgentOnRequestsAndHandshakes(t *testing.T) {
	for _, tc := range []struct{ configured, want string }{
		{"", config.DefaultUserAgent},
		{"Mozilla/5.0 (X11; Linux x86_64)", "Mozilla/5.0 (X11; Linux x86_64)"},
	} {
		var mu sync.Mutex
		var seen []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			seen = append(seen, r.Header.Get("User-Agent"))
			mu.Unlock()
			w.WriteHeader(503)
		}))
		cfg := testCfg()
		cfg.UserAgent = tc.configured
		c := NewTripleTransportClientForURL(srv.URL, "tok", cfg, "ing", true)
		_, _ = c.Probe(context.Background(), "B", "n")
		_ = c.PrecheckWS(context.Background(), "B", "")
		c.Close()
		srv.Close()
		mu.Lock()
		if len(seen) < 2 {
			t.Fatalf("%q: server saw %d requests, want a probe and a WS precheck", tc.configured, len(seen))
		}
		for i, ua := range seen {
			if ua != tc.want {
				t.Fatalf("%q: request %d User-Agent %q, want %q", tc.configured, i, ua, tc.want)
			}
		}
		mu.Unlock()
	}
	if config.DefaultUserAgent != "DBBridge/"+protocol.ProtocolVersionString {
		t.Fatalf("default User-Agent %q", config.DefaultUserAgent)
	}
}
