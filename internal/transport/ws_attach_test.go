package transport

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"dbbridge/internal/protocol"

	"github.com/gorilla/websocket"
)

func readyAfter(delay time.Duration) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{Subprotocols: []string{protocol.SecWebSocketProtocol}}
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		time.Sleep(delay)
		b, _ := json.Marshal(protocol.ReadyEventData{Type: "ready", ProtocolVersion: "1.4", SessionID: "s1", InstanceID: "bi", Epoch: "1", StreamID: "st", Transport: "ws"})
		_ = c.WriteMessage(websocket.TextMessage, b)
		time.Sleep(time.Second)
	}))
}

// ws.handshake_timeout covers only the Upgrade; the
// ready message may arrive at any time within the attach budget.
func TestWSReadyWaitsForAttachBudgetNotHandshakeTimeout(t *testing.T) {
	srv := readyAfter(600 * time.Millisecond)
	defer srv.Close()
	c := testClient(t, srv.URL, true) // ws.handshake_timeout 300ms
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws, ready, err := c.DialWSSession(ctx, "s1", "tok", 1, "bi")
	if err != nil {
		t.Fatalf("ready 600ms after a fast 101 rejected: %v", err)
	}
	defer ws.Close()
	if ready.StreamID != "st" {
		t.Fatalf("ready=%+v", ready)
	}
}

// An attach budget that runs out while waiting for ready is reported as a
// deadline, not as a carrier read failure.
func TestWSReadyPastAttachBudgetIsDeadlineExceeded(t *testing.T) {
	srv := readyAfter(800 * time.Millisecond)
	defer srv.Close()
	c := testClient(t, srv.URL, true)
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	ws, _, err := c.DialWSSession(ctx, "s1", "tok", 1, "bi")
	if ws != nil {
		ws.Close()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want context.DeadlineExceeded", err)
	}
}
