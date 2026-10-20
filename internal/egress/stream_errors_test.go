package egress

import (
	"bytes"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// A session WS request that would fail the WebSocket handshake (wrong
// version, bad key) is refused before the attach slot is reserved, so the
// session stays usable.
func TestMalformedWSHandshakeLeavesSessionAttachable(t *testing.T) {
	cfg := fixCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	s, ts := egrServer(t, cfg)
	cr := wsCreate(t, ts.URL, "tokA", "w1")
	for name, h := range map[string][2]string{
		"version 8":   {"8", "dGhlIHNhbXBsZSBub25jZQ=="},
		"no key":      {"13", ""},
		"short key":   {"13", "c2hvcnQ="},
		"invalid key": {"13", "not base64!"},
	} {
		req, _ := http.NewRequest("GET", ts.URL+"/v1/transport/ws", nil)
		req.Header = wsHeader("tokA", cr)
		req.Header.Set("Sec-WebSocket-Protocol", protocol.SecWebSocketProtocol)
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Sec-WebSocket-Version", h[0])
		if h[1] != "" {
			req.Header.Set("Sec-WebSocket-Key", h[1])
		}
		resp, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		var er protocol.ErrorResponse
		_ = json.NewDecoder(resp.Body).Decode(&er)
		resp.Body.Close()
		if resp.StatusCode != 400 || er.Code != protocol.ErrBadFrame {
			t.Fatalf("%s: %d %+v", name, resp.StatusCode, er)
		}
	}
	if sess := s.registry.GetAny(cr.SessionID); sess.GetState() == session.StateClosed {
		t.Fatalf("malformed handshake closed the session: %s", sess.TerminalCode())
	}
	c, _, e := (&websocket.Dialer{Subprotocols: []string{protocol.SecWebSocketProtocol}}).Dial(wsURLOf(ts.URL), wsHeader("tokA", cr))
	if e != nil {
		t.Fatalf("session not attachable after malformed handshakes: %v", e)
	}
	c.Close()
}

// A WS frame body must arrive within a deadline from its first byte; a peer
// that trickles it loses the carrier instead of holding codec indefinitely.
func TestTrickledWSFrameBodyDropsCarrier(t *testing.T) {
	cfg := fixCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	cfg.Transport.UpRequestTimeout = 500 * time.Millisecond
	s, ts := egrServer(t, cfg)
	cr := wsCreate(t, ts.URL, "tokA", "w1")
	c, _, e := (&websocket.Dialer{Subprotocols: []string{protocol.SecWebSocketProtocol}, WriteBufferSize: 64}).Dial(wsURLOf(ts.URL), wsHeader("tokA", cr))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	frame, _ := protocol.EncodeWSFrameChecked(1, 0, bytes.Repeat([]byte{'x'}, 1024))
	w, _ := c.NextWriter(websocket.BinaryMessage)
	start := time.Now()
	sess := s.registry.Get(cr.SessionID)
	// 64-byte fragments every 100 ms: the whole body would need ~1.7 s.
	for off := 0; off+64 <= len(frame) && sess.GetState() != session.StateClosed; off += 64 {
		if _, e := w.Write(frame[off : off+64]); e != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	for i := 0; i < 20 && sess.GetState() != session.StateClosed; i++ {
		time.Sleep(50 * time.Millisecond)
	}
	if sess.GetState() != session.StateClosed {
		t.Fatal("trickled frame body kept the carrier")
	}
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("carrier dropped only after %v", el)
	}
	if used := s.codecFor("A").Used(); used != 0 {
		t.Fatalf("codec still held: %d", used)
	}
}

func finRequest(t *testing.T, url, stream string, cr protocol.CreateSessionResponse, final int) (int, protocol.ErrorResponse) {
	t.Helper()
	b, _ := json.Marshal(protocol.FinRequest{Direction: "up", FinalOffset: fmt.Sprint(final)})
	r, _ := http.NewRequest("POST", fmt.Sprintf("%s/v1/sessions/%s/fin?epoch=1", url, cr.SessionID), bytes.NewReader(b))
	r.Header.Set("Authorization", "Bearer tokA")
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	r.Header.Set(protocol.HeaderSessionToken, cr.SessionToken)
	r.Header.Set(protocol.HeaderStreamID, stream)
	r.Header.Set("Content-Type", "application/json")
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	var er protocol.ErrorResponse
	_ = json.NewDecoder(resp.Body).Decode(&er)
	return resp.StatusCode, er
}

func activeSSESession(t *testing.T, s *Server, url, id string) (protocol.CreateSessionResponse, string, *session.Session) {
	t.Helper()
	st, _, cr := egrCreate(t, url, "tokA", id, "db")
	if st != 201 {
		t.Fatalf("create %d", st)
	}
	stream, _ := egrOpenSSE(t, url, "tokA", cr)
	sess := s.registry.Get(cr.SessionID)
	if e := sess.ActivateDataPlane(stream); e != nil {
		t.Fatal(e)
	}
	return cr, stream, sess
}

// A FIN conflicting with a pending frame terminates the session with
// DBB_FRAME_CONFLICT, as its session_closed:true answer says.
func TestFinConflictClosesSessionWithFrameConflict(t *testing.T) {
	cfg := fixCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	cfg.Transport.ControlRequestTimeout = 10 * time.Second
	s, ts := egrServer(t, cfg)
	cr, stream, sess := activeSSESession(t, s, ts.URL, "f1")
	if st, raw := upFrame(t, ts.URL, "tokA", stream, cr, 100, bytes.Repeat([]byte("a"), 100), 100); st != 202 {
		t.Fatalf("out-of-order /up: %d %s", st, raw)
	}
	st, er := finRequest(t, ts.URL, stream, cr, 50)
	if st != 409 || er.Code != protocol.ErrFrameConflict || !er.SessionClosed {
		t.Fatalf("conflicting FIN: %d %+v", st, er)
	}
	if sess.GetState() != session.StateClosed || sess.TerminalCode() != protocol.ErrFrameConflict {
		t.Fatalf("session after a conflicting FIN: %s %q", sess.GetState(), sess.TerminalCode())
	}
}

// A conflicting /up frame records DBB_FRAME_CONFLICT as the terminal code,
// as the WS transport does.
func TestUpConflictClosesSessionWithFrameConflict(t *testing.T) {
	cfg := fixCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	cfg.Transport.ControlRequestTimeout = 10 * time.Second
	s, ts := egrServer(t, cfg)
	cr, stream, sess := activeSSESession(t, s, ts.URL, "f2")
	upFrame(t, ts.URL, "tokA", stream, cr, 100, bytes.Repeat([]byte("a"), 100), 100)
	st, raw := upFrame(t, ts.URL, "tokA", stream, cr, 100, bytes.Repeat([]byte("b"), 100), 100)
	if st != 409 || !strings.Contains(raw, protocol.ErrFrameConflict) {
		t.Fatalf("conflicting /up: %d %s", st, raw)
	}
	if sess.TerminalCode() != protocol.ErrFrameConflict || sess.TerminalCategory() != "protocol" {
		t.Fatalf("terminal %q %q", sess.TerminalCode(), sess.TerminalCategory())
	}
	st, raw = upFrame(t, ts.URL, "tokA", stream, cr, 0, []byte("c"), 1)
	if st != 410 || !strings.Contains(raw, protocol.ErrFrameConflict) {
		t.Fatalf("request after the conflict: %d %s", st, raw)
	}
}

// A request that finds its session's target ACL revoked before the reload
// sweep closes it records DBB_TARGET_FORBIDDEN as the terminal code.
func TestRevokedTargetOnRequestClosesWithTargetForbidden(t *testing.T) {
	cfg := fixCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	s, ts := egrServer(t, cfg)
	st, _, cr := egrCreate(t, ts.URL, "tokA", "r1", "db")
	if st != 201 {
		t.Fatalf("create %d", st)
	}
	sess := s.registry.Get(cr.SessionID)
	// Rebind the session to a target the peer may not use, as a reload
	// that removed the ACL would leave it until the sweep runs.
	sess.TargetID = "other"
	r, _ := http.NewRequest("POST", ts.URL+"/v1/sessions/"+cr.SessionID+"/ack?epoch=1", strings.NewReader("{}"))
	r.Header.Set("Authorization", "Bearer tokA")
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	r.Header.Set(protocol.HeaderSessionToken, cr.SessionToken)
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("revoked target: %d %s", resp.StatusCode, raw)
	}
	if sess.TerminalCode() != protocol.ErrTargetForbidden {
		t.Fatalf("terminal code %q", sess.TerminalCode())
	}
}
