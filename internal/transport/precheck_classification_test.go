package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"dbbridge/internal/protocol"
	"github.com/gorilla/websocket"
)

// Section 3.7.2: the allowed fallbacks stay ErrWSUpgradeRejected (raw gateway
// 400/403/404/405/426/501, a plain non-WS response, a trusted
// DBB_TRANSPORT_UNSUPPORTED). The selector still re-probes before using SSE.
func TestPrecheckAllowedFallbacksStayUpgradeRejected(t *testing.T) {
	type tc struct {
		status  int
		trusted bool
	}
	cases := map[string]tc{"400": {400, false}, "403": {403, false}, "404": {404, false}, "405": {405, false}, "426": {426, false}, "501": {501, false}, "200 page": {200, false}, "transport unsupported": {409, true}}
	// Redirects, proxy authentication and WAF/size refusals are ordinary
	// answers of the path as well.
	for _, status := range []int{301, 302, 307, 308, 407, 410, 413, 414, 429, 431, 502} {
		cases[fmt.Sprint(status)] = tc{status, false}
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if c.status/100 == 3 {
					w.Header().Set("Location", "https://sso.example.com/login")
				}
				if c.trusted {
					w.Header().Set(protocol.HeaderInstanceID, "bi")
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(c.status)
					_ = json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: protocol.ErrTransportUnsupported})
					return
				}
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(c.status)
				_, _ = w.Write([]byte("<html>gateway</html>"))
			}))
			defer srv.Close()
			err := fixClient(t, srv.URL, true).PrecheckWS(context.Background(), "B", "bi")
			if !errors.Is(err, ErrWSUpgradeRejected) || errors.Is(err, ErrProtocol) {
				t.Fatalf("err=%v, want ErrWSUpgradeRejected only", err)
			}
			var re *RemoteError
			if !c.trusted && errors.As(err, &re) {
				t.Fatalf("plain answer must not surface as a RemoteError: %v", err)
			}
			var refused *UpgradeRefusedError
			if !c.trusted && (!errors.As(err, &refused) || refused.Status != c.status) {
				t.Fatalf("err=%v, want UpgradeRefusedError{%d}", err, c.status)
			}
			wantTransient := c.status == 429 || c.status >= 500
			if !c.trusted && TransientUpgradeRefusal(err) != wantTransient {
				t.Fatalf("status %d transient=%v", c.status, !wantTransient)
			}
		})
	}
}

// Section 3.7.3(2): a raw 401 and trusted DBBridge refusals keep their
// RemoteError form and are never an upgrade rejection.
func TestPrecheckRawUnauthorizedAndTrustedErrorsAreNotUpgradeRejections(t *testing.T) {
	cases := map[string]struct {
		status  int
		trusted bool
	}{"raw 401": {401, false}, "trusted 401": {401, true}, "trusted 403": {403, true}, "trusted 429": {429, true}}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if c.trusted {
					w.Header().Set(protocol.HeaderInstanceID, "bi")
				}
				w.WriteHeader(c.status)
				_ = json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: protocol.ErrUnauthorized})
			}))
			defer srv.Close()
			err := fixClient(t, srv.URL, true).PrecheckWS(context.Background(), "B", "bi")
			var re *RemoteError
			if errors.Is(err, ErrWSUpgradeRejected) || !errors.As(err, &re) || re.Status != c.status || re.Trusted != c.trusted {
				t.Fatalf("err=%v, want RemoteError %d trusted=%v", err, c.status, c.trusted)
			}
		})
	}
}

// Sections 3.7.3(3), 5.11 (DBB_WS_PROTOCOL_ERROR): after a 101 with the right
// subprotocol, an illegal WebSocket frame or an oversized hello is protocol
// data that must not be tolerated by switching to SSE.
func TestPrecheckIllegalFramesAfterUpgradeAreProtocolErrors(t *testing.T) {
	cases := map[string]func(c *websocket.Conn){
		"reserved opcode": func(c *websocket.Conn) { _, _ = c.UnderlyingConn().Write([]byte{0x83, 0x00}) },
		"oversized hello": func(c *websocket.Conn) {
			_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello","pad":"`+strings.Repeat("x", 20<<10)+`"}`))
		},
	}
	for name, send := range cases {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, e := (&websocket.Upgrader{Subprotocols: []string{protocol.SecWebSocketProtocol}}).Upgrade(w, r, nil)
				if e != nil {
					return
				}
				defer c.Close()
				send(c)
				_, _, _ = c.ReadMessage()
			}))
			defer srv.Close()
			err := fixClient(t, srv.URL, true).PrecheckWS(context.Background(), "B", "bi")
			if !errors.Is(err, ErrProtocol) || errors.Is(err, ErrWSUpgradeRejected) {
				t.Fatalf("err=%v, want ErrProtocol only", err)
			}
		})
	}
}

// DBB-AUD-10 (5.4.3, 5.8(16), 5.9.1(5), TF-13): a successful ACK/POST/FIN
// response with a wrong binding is a comparable ErrProtocol at the transport
// boundary, not an unclassified error.
func TestResponseBindingMismatchIsErrProtocol(t *testing.T) {
	for _, op := range []string{"ack", "up", "fin"} {
		t.Run(op, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(protocol.HeaderInstanceID, "bi")
				w.Header().Set("Content-Type", "application/json")
				mode := &protocol.TransportMode{Mode: "ws"}
				switch op {
				case "ack":
					_ = json.NewEncoder(w).Encode(protocol.AckResponse{SessionID: "s", InstanceID: "bi", Epoch: "1", StreamID: "other-stream", Transport: mode})
				case "up":
					_ = json.NewEncoder(w).Encode(protocol.UpResponse{SessionID: "s", InstanceID: "bi", Epoch: "2", StreamID: "st", Transport: mode})
				case "fin":
					_ = json.NewEncoder(w).Encode(protocol.FinResponse{SessionID: "other", InstanceID: "bi", Epoch: "1", StreamID: "st", Transport: mode})
				}
			}))
			defer srv.Close()
			c := fixClient(t, srv.URL, true)
			c.BindStream("s", 1, "st", "ws")
			var err error
			switch op {
			case "ack":
				_, err = c.SendAck(context.Background(), "s", "tok", "bi", 1, protocol.AckRequest{})
			case "up":
				_, err = c.SendUpFrame(context.Background(), "s", "tok", "bi", 1, 0, []byte("x"), 0, 0)
			case "fin":
				_, err = c.SendFin(context.Background(), "s", "tok", "bi", 1, 0)
			}
			if !errors.Is(err, ErrProtocol) || !strings.Contains(fmt.Sprint(err), "binding mismatch") {
				t.Fatalf("%s binding mismatch err=%v, want ErrProtocol", op, err)
			}
			if !errors.Is(err, protocol.ErrPeerProtocol) {
				t.Fatalf("ErrProtocol must equal protocol.ErrPeerProtocol: %v", err)
			}
		})
	}
}
