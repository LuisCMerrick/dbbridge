package transport

import (
	"context"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"errors"
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestInvalidPrecheckMustNotPermitFallback(t *testing.T) {
	for _, kind := range []string{"subprotocol", "protocol_version", "malformed_hello"} {
		t.Run(kind, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				u := websocket.Upgrader{}
				if kind != "subprotocol" {
					u.Subprotocols = []string{protocol.SecWebSocketProtocol}
				}
				c, e := u.Upgrade(w, r, nil)
				if e != nil {
					return
				}
				defer c.Close()
				if kind == "malformed_hello" {
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello","type":"hello"}`))
					return
				}
				h := protocol.WSHelloMessage{Type: "hello", ProtocolVersion: "1.5", NodeID: "B", InstanceID: "bi", Nonce: r.Header.Get(protocol.HeaderWSNonce), Transport: "ws"}
				if kind == "protocol_version" {
					h.ProtocolVersion = "1.3"
				}
				_ = c.WriteJSON(h)
			}))
			defer srv.Close()
			c := NewTripleTransportClientForURL(srv.URL, "test-credential", &config.TransportConfig{ControlRequestTimeout: time.Second, DialTimeout: time.Second}, "ai", true)
			defer c.Close()
			err := c.PrecheckWS(context.Background(), "B", "bi")
			if !errors.Is(err, ErrProtocol) || errors.Is(err, ErrWSUpgradeRejected) {
				t.Fatalf("contract violation: invalid %s classified as fallback-eligible: %v", kind, err)
			}
		})
	}
}
