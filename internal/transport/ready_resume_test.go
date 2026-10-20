package transport

import (
	"context"
	"dbbridge/internal/protocol"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestInitialReadyProtocolClassification(t *testing.T) {
	for _, kind := range []string{"duplicate", "binary", "syntax", "limit", "disconnect"} {
		t.Run(kind, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				up := websocket.Upgrader{Subprotocols: []string{protocol.SecWebSocketProtocol}}
				c, err := up.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer c.Close()
				switch kind {
				case "duplicate":
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"ready","type":"ready"}`))
				case "binary":
					_ = c.WriteMessage(websocket.BinaryMessage, []byte(`{}`))
				case "syntax":
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":`))
				case "limit":
					_ = c.WriteMessage(websocket.TextMessage, make([]byte, 17<<10))
				}
			}))
			defer ts.Close()
			c := testHTTPClient(ts.URL)
			defer c.Close()
			_, _, err := c.DialWSSession(context.Background(), "sid", "secret", 1, "bi")
			if err == nil || errors.Is(err, ErrProtocol) != (kind != "disconnect") {
				t.Fatalf("%s: protocol classification: %v", kind, err)
			}
		})
	}
}

func TestResumeRejectsUnrepresentableRemainingBudget(t *testing.T) {
	for _, remaining := range []int64{0, -1, math.MaxInt64, 1000} {
		t.Run(time.Duration(remaining).String(), func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(protocol.ResumeResponse{
					SessionID: "sid", InstanceID: "bi", Epoch: "2", ResumeRequestID: "request",
					Transport: protocol.TransportMode{Mode: "ws"}, AckUpOffset: "0", UpWrittenOffset: "0",
					DownReplayFrom: "0", DownProducedOffset: "0", DownRetainedFrom: "0", ResumeRemainingMs: remaining,
				})
			}))
			defer ts.Close()
			c := testHTTPClient(ts.URL)
			defer c.Close()
			_, err := c.ResumeSession(context.Background(), "sid", "secret", "bi", protocol.ResumeRequest{
				ExpectedEpoch: "1", ResumeRequestID: "request", Transport: protocol.TransportMode{Mode: "ws"},
				UpNextOffset: "0", KnownUpAckOffset: "0", DownDeliveredOffset: "0",
			})
			if remaining == 1000 {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrProtocol) {
				t.Fatalf("invalid remaining budget accepted: %d: %v", remaining, err)
			}
		})
	}
}
