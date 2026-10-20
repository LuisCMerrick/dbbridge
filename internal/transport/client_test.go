package transport

import (
	"bufio"
	"context"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"encoding/json"
	"github.com/gorilla/websocket"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func testHTTPClient(url string) *TripleTransportClient {
	return NewTripleTransportClientForURL(url, "secret", &config.TransportConfig{H2PoolSize: 4, MaxFrameSize: 1024 * 1024, ControlRequestTimeout: time.Second, DialTimeout: time.Second}, "ing", true)
}
func TestEndpointPools(t *testing.T) {
	a := testHTTPClient("https://one.invalid")
	b := a.WithURL("https://two.invalid")
	if a.sseClient.Transport == b.sseClient.Transport {
		t.Fatal("different endpoints share identical SSE/POST/control pools")
	}
}
func TestRejectRedirect(t *testing.T) {
	var leaked string
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked = r.Header.Get("Authorization")
		json.NewEncoder(w).Encode(protocol.ProbeResponse{ProtocolVersion: "1.4"})
	}))
	defer dst.Close()
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, dst.URL, http.StatusTemporaryRedirect)
	}))
	defer src.Close()
	c := testHTTPClient(src.URL)
	_, err := c.Probe(context.Background(), "p", "nonce")
	if err == nil {
		t.Fatalf("followed 307 redirect to another origin; forwarded Authorization=%t", leaked != "")
	}
}
func TestSubscriberCloseUnblocksRead(t *testing.T) {
	rd, wr := io.Pipe()
	defer wr.Close()
	s := &SSESubscriber{body: rd, reader: nil}
	s.reader = bufio.NewReader(rd)
	readDone := make(chan struct{})
	go func() { s.NextEvent(); close(readDone) }()
	time.Sleep(10 * time.Millisecond)
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(80 * time.Millisecond):
		wr.Close()
		<-readDone
		<-done
		t.Fatal("Close waits on parser mutex held across blocking read")
	}
}
func TestWSSessionReadLimit(t *testing.T) {
	u := websocket.Upgrader{Subprotocols: []string{protocol.SecWebSocketProtocol}}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, e := u.Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(protocol.ReadyEventData{Type: "ready", ProtocolVersion: "1.4", SessionID: "sid", InstanceID: "inst", StreamID: "st", Epoch: "1", Transport: "ws"})
		conn.WriteMessage(websocket.BinaryMessage, make([]byte, 2*1024*1024))
	}))
	defer s.Close()
	c := testHTTPClient(s.URL)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, _, err := c.DialWSSession(ctx, "sid", "st", 1, "inst")
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	ws.Conn().SetReadDeadline(time.Now().Add(time.Second))
	_, p, err := ws.Conn().ReadMessage()
	if err == nil && len(p) > 1024*1024+56 {
		t.Fatalf("client allocated/read oversized %d-byte WS message", len(p))
	}
}
