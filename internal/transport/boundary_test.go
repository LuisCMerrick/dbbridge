package transport

import (
	"context"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"encoding/json"
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBoundaryWSMessageReadHonorsContext(t *testing.T) {
	done := make(chan error, 1)
	budget := resource.New(1 << 20)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{}
		c, e := up.Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		_, _, release, err := ReadWSMessage(ctx, c, budget, 8192)
		if release != nil {
			release()
		}
		done <- err
	}))
	defer ts.Close()
	c, _, e := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	// Send one valid masked, non-final WS fragment with a complete DBBridge
	// header but only part of its payload. Withhold the continuation fragment.
	msg := protocol.EncodeWSFrame(1, 0, make([]byte, 8192))
	fragment := append([]byte{0x02, 0xfe, 0x13, 0x88, 0, 0, 0, 0}, msg[:5000]...)
	if _, e = c.UnderlyingConn().Write(fragment); e != nil {
		t.Fatal(e)
	}
	select {
	case <-done:
	case <-time.After(150 * time.Millisecond):
		t.Logf("partial message holds codec bytes=%d after deadline", budget.Used())
		c.Close()
		<-done
		t.Fatal("WS reader still blocked after 30ms context deadline; production Egress sets no read deadline")
	}
}

func TestBoundarySmallFrameStillAllowsTextControls(t *testing.T) {
	stop := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{Subprotocols: []string{protocol.SecWebSocketProtocol}}
		c, e := up.Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer c.Close()
		c.WriteJSON(protocol.ReadyEventData{Type: "ready", ProtocolVersion: "1.4", SessionID: "sid", InstanceID: "bi", Epoch: "1", StreamID: "stream", Transport: "ws"})
		c.WriteJSON(protocol.WSTextControlMessage{Type: "ping", SessionID: "sid", InstanceID: "bi", Epoch: "1", StreamID: "stream", Nonce: "0123456789abcdef0123456789abcdef"})
		<-stop
	}))
	defer ts.Close()
	defer close(stop)
	cfg := &config.Config{Node: config.NodeConfig{ID: "A"}, Transport: config.TransportConfig{FrameSizeRaw: "32B", MaxFrameSizeRaw: "64B"}}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	c := NewTripleTransportClientForURL(ts.URL, "token", &cfg.Transport, "ai", true)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, _, err := c.DialWSSession(ctx, "sid", "token", 1, "bi")
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	_, b, release, err := ReadWSMessage(ctx, ws.Conn(), resource.New(1<<20), 64)
	if release != nil {
		defer release()
	}
	var ctrl protocol.WSTextControlMessage
	json.Unmarshal(b, &ctrl)
	if err != nil || ctrl.Type != "ping" {
		t.Fatalf("valid config max_frame_size=64 rejected small legal text control: %v", err)
	}
}
