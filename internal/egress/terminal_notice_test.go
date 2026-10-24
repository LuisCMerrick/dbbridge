package egress

import (
	"bufio"
	"bytes"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// readWSTerminalError reads WS messages until the terminal error message and
// checks that the carrier is closed right after it.
func readWSTerminalError(t *testing.T, c *websocket.Conn) protocol.WSTextControlMessage {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		typ, b, e := c.ReadMessage()
		if e != nil {
			t.Fatalf("carrier ended without a terminal error message: %v", e)
		}
		if typ != websocket.TextMessage {
			continue
		}
		var m protocol.WSTextControlMessage
		if e := protocol.DecodeJSON(bytes.NewReader(b), 16<<10, &m); e != nil {
			t.Fatalf("control message %s: %v", b, e)
		}
		if m.Type != "error" {
			continue
		}
		if !strings.Contains(string(b), `"retryable":false`) || !strings.Contains(string(b), `"session_closed":true`) {
			t.Fatalf("terminal error must carry retryable and session_closed: %s", b)
		}
		if _, _, e := c.ReadMessage(); e == nil {
			t.Fatal("carrier stayed open after the terminal error message")
		}
		return m
	}
}

// resettingBackend accepts connections and resets each one after delay.
func resettingBackend(t *testing.T, delay time.Duration) string {
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, e := ln.Accept()
			if e != nil {
				return
			}
			go func() {
				time.Sleep(delay)
				_ = c.(*net.TCPConn).SetLinger(0)
				c.Close()
			}()
		}
	}()
	return ln.Addr().String()
}

func TestSSEStreamEndsWithTerminalErrorWhenBackendIsLost(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	cfg.Targets = map[string]config.TargetConfig{"db": {Network: "tcp", Address: resettingBackend(t, 300*time.Millisecond)}}
	cfg.Transport.ControlRequestTimeout = 10 * time.Second
	s, ts := egrServer(t, cfg)
	st, _, cr := egrCreate(t, ts.URL, "tokA", "e1", "db")
	if st != 201 {
		t.Fatalf("create %d", st)
	}
	r, _ := http.NewRequest("GET", ts.URL+"/v1/sessions/"+cr.SessionID+"/events?epoch=1", nil)
	r.Header.Set("Authorization", "Bearer tokA")
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	r.Header.Set(protocol.HeaderSessionToken, cr.SessionToken)
	ev, e := http.DefaultClient.Do(r)
	if e != nil || ev.StatusCode != 200 {
		t.Fatalf("events %v %v", ev, e)
	}
	defer ev.Body.Close()
	sess := s.registry.Get(cr.SessionID)
	var stream string
	var notice protocol.WSTextControlMessage
	kind := ""
	br := bufio.NewReader(ev.Body)
	for {
		line, e := br.ReadString('\n')
		if e != nil {
			break
		}
		switch {
		case strings.HasPrefix(line, "event: "):
			kind = strings.TrimSpace(strings.TrimPrefix(line, "event: "))
		case strings.HasPrefix(line, "data: ") && kind == "ready":
			var ready protocol.ReadyEventData
			_ = json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ready)
			stream = ready.StreamID
		case strings.HasPrefix(line, "data: ") && kind == "error":
			if e := protocol.DecodeJSON(strings.NewReader(strings.TrimPrefix(line, "data: ")), 16<<10, &notice); e != nil {
				t.Fatalf("error event %q: %v", line, e)
			}
		}
	}
	if sess.TerminalCode() != protocol.ErrBackendLost {
		t.Fatalf("setup: terminal %q", sess.TerminalCode())
	}
	if notice.Type != "error" || notice.Code != protocol.ErrBackendLost || !notice.SessionClosed || notice.Retryable {
		t.Fatalf("stream ended without the terminal error event: %+v", notice)
	}
	if notice.SessionID != cr.SessionID || notice.InstanceID != "bi" || notice.Epoch != "1" || notice.StreamID != stream {
		t.Fatalf("terminal error not bound to the stream %q: %+v", stream, notice)
	}
}

func TestWSCarrierEndsWithTerminalErrorWhenBackendIsLost(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	cfg.Targets = map[string]config.TargetConfig{"db": {Network: "tcp", Address: resettingBackend(t, 300*time.Millisecond)}}
	_, ts := egrServer(t, cfg)
	cr := wsCreate(t, ts.URL, "tokA", "w1")
	c, _, e := (&websocket.Dialer{Subprotocols: []string{protocol.SecWebSocketProtocol}}).Dial(wsURLOf(ts.URL), wsHeader("tokA", cr))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if notice := readWSTerminalError(t, c); notice.Code != protocol.ErrBackendLost || notice.SessionID != cr.SessionID || notice.Epoch != "1" {
		t.Fatalf("terminal notice: %+v", notice)
	}
}

func wsURLOf(base string) string {
	return "ws" + strings.TrimPrefix(base, "http") + "/v1/transport/ws"
}

// wsCreate creates a WS-mode session for token.
func wsCreate(t *testing.T, url, token, id string) protocol.CreateSessionResponse {
	t.Helper()
	b, _ := json.Marshal(protocol.CreateSessionRequest{Limits: protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 2048, DownBufferBytes: 4096}, ProtocolVersion: "1.5", IngressInstanceID: "ai", CreateRequestID: id, TargetID: "db", Transport: protocol.TransportMode{Mode: "ws"}})
	r, _ := http.NewRequest("POST", url+"/v1/sessions", bytes.NewReader(b))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("create %s: %d", id, resp.StatusCode)
	}
	var cr protocol.CreateSessionResponse
	_ = json.NewDecoder(resp.Body).Decode(&cr)
	return cr
}
