package node

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"

	"dbbridge/internal/protocol"
)

// TestReverseHTTPTransports runs the end-to-end exchange and the tunnel loss
// fault injection over tunnels carried by SSE and by long polling.
func TestReverseHTTPTransports(t *testing.T) {
	for _, mode := range []string{"http-sse", "long-poll"} {
		for _, side := range []string{"public", "database"} {
			t.Run(mode+"/"+side, func(t *testing.T) {
				p := startReversePairWith(t, "{enabled: true, grace: 20s, barrier_timeout: 2s}", mode, nil)
				if got := p.db.tunnelCli.Transports()[mode]; got != 2 {
					t.Fatalf("%d %s tunnels, want 2", got, mode)
				}
				tunnelLossRecovers(t, p, side)
			})
		}
	}
}

// intermediary is an HTTP proxy in front of the public side that refuses
// WebSocket upgrades and, optionally, buffers whole SSE responses.
func intermediary(t *testing.T, bufferSSE bool) func(string) string {
	return func(public string) string {
		u, _ := url.Parse(public)
		rp := httputil.NewSingleHostReverseProxy(u)
		rp.FlushInterval = -1
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Upgrade") != "" {
				http.Error(w, "upgrade not allowed", http.StatusBadGateway)
				return
			}
			if bufferSSE && strings.HasSuffix(r.URL.Path, protocol.ReverseEventsSuffix) {
				req, _ := http.NewRequestWithContext(r.Context(), r.Method, public+r.URL.Path, nil)
				req.Header = r.Header.Clone()
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadGateway)
					return
				}
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)
				for k, v := range resp.Header {
					w.Header()[k] = v
				}
				w.WriteHeader(resp.StatusCode)
				_, _ = w.Write(body)
				return
			}
			rp.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
}

// TestReverseAutoFallbackRecovers: behind an intermediary that refuses
// WebSocket upgrades auto uses SSE, and behind one that also buffers SSE it
// uses long polling; in both cases sessions survive tunnel loss.
func TestReverseAutoFallbackRecovers(t *testing.T) {
	cases := []struct {
		name      string
		bufferSSE bool
		want      string
	}{
		{"ws refused", false, "http-sse"},
		{"ws refused and sse buffered", true, "long-poll"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := startReversePairWith(t, "{enabled: true, grace: 20s, barrier_timeout: 2s}", "auto", intermediary(t, tc.bufferSSE))
			if got := p.db.tunnelCli.Transports()[tc.want]; got != 2 {
				t.Fatalf("transports %v, want 2 %s", p.db.tunnelCli.Transports(), tc.want)
			}
			tunnelLossRecovers(t, p, "public")
		})
	}
}
