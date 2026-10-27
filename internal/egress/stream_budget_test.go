package egress

import (
	"bufio"
	"context"
	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// egrOpenSSE opens the events stream of a session and returns the stream ID
// from its ready event.
func egrOpenSSE(t *testing.T, url, token string, cr protocol.CreateSessionResponse) (string, *http.Response) {
	t.Helper()
	r, _ := http.NewRequest("GET", url+"/v1/sessions/"+cr.SessionID+"/events?epoch=1", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	r.Header.Set(protocol.HeaderSessionToken, cr.SessionToken)
	ev, e := http.DefaultClient.Do(r)
	if e != nil || ev.StatusCode != 200 {
		t.Fatalf("events: %v %v", ev, e)
	}
	t.Cleanup(func() { ev.Body.Close() })
	br := bufio.NewReader(ev.Body)
	for {
		line, e := br.ReadString('\n')
		if e != nil {
			t.Fatal(e)
		}
		if data, ok := strings.CutPrefix(line, "data: "); ok {
			var ready protocol.ReadyEventData
			if json.Unmarshal([]byte(data), &ready) == nil && ready.StreamID != "" {
				return ready.StreamID, ev
			}
		}
	}
}

// One session holds at most its negotiated in-flight frames of /up
// bodies (and their codec memory); further concurrent /up get a retryable 409.
func TestUpInflightPerSession(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	cfg.Transport.UpMaxInflightFrames = 2
	cfg.Transport.ControlRequestTimeout = 10 * time.Second
	s, addr := egrStart(t, cfg, 0)
	url := "http://" + addr
	st, _, cr := egrCreate(t, url, "tokA", "u1", "db")
	if st != 201 {
		t.Fatalf("create %d", st)
	}
	stream, _ := egrOpenSSE(t, url, "tokA", cr)
	if e := s.registry.Get(cr.SessionID).ActivateDataPlane(stream); e != nil {
		t.Fatal(e)
	}
	send := func() net.Conn {
		c, e := net.Dial("tcp", addr)
		if e != nil {
			t.Fatal(e)
		}
		fmt.Fprintf(c, "POST /v1/sessions/%s/up?epoch=1 HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tokA\r\n%s: %s\r\n%s: bi\r\n%s: %s\r\n%s: 0\r\n%s: 1000\r\n%s: %s\r\nContent-Type: application/octet-stream\r\nContent-Length: 1000\r\n\r\nx",
			cr.SessionID, protocol.HeaderSessionToken, cr.SessionToken, protocol.HeaderInstanceID, protocol.HeaderStreamID, stream, protocol.HeaderOffset, protocol.HeaderLength, protocol.HeaderSHA256, strings.Repeat("0", 64))
		return c
	}
	var held []net.Conn
	defer func() {
		for _, c := range held {
			c.Close()
		}
	}()
	for i := 0; i < 2; i++ {
		held = append(held, send())
	}
	time.Sleep(200 * time.Millisecond)
	extra := send()
	defer extra.Close()
	_ = extra.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, e := http.ReadResponse(bufio.NewReader(extra), nil)
	if e != nil {
		t.Fatal(e)
	}
	var er protocol.ErrorResponse
	_ = json.NewDecoder(resp.Body).Decode(&er)
	resp.Body.Close()
	if resp.StatusCode != 409 || er.Code != protocol.ErrWindowExceeded || !er.Retryable {
		t.Fatalf("third concurrent /up: %d %+v", resp.StatusCode, er)
	}
	if used := cfg.Resources.Codec.Used(); used > 2000 {
		t.Fatalf("one session holds %d bytes of codec", used)
	}
}

// An established SSE stream gives back its request slot and request
// metadata; only the session's own reservation remains.
func TestStreamReleasesRequestBudget(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	s, ts := egrServer(t, cfg)
	st, _, cr := egrCreate(t, ts.URL, "tokA", "e1", "db")
	if st != 201 {
		t.Fatalf("create %d", st)
	}
	egrOpenSSE(t, ts.URL, "tokA", cr)
	if used := cfg.Resources.DataRequests.Used(); used != 0 {
		t.Fatalf("live SSE stream holds %d data request slots", used)
	}
	// Session reservation + its ~8 KiB idempotency record; the stream's
	// 16 KiB request metadata is gone.
	if used := cfg.Resources.Metadata.Used(); used >= s.sessionMetadataCost()+16<<10 {
		t.Fatalf("metadata %d still includes the request's 16 KiB (session %d)", used, s.sessionMetadataCost())
	}
}

func wsHeader(token string, cr protocol.CreateSessionResponse) http.Header {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+token)
	h.Set(protocol.HeaderInstanceID, "bi")
	h.Set(protocol.HeaderWSPurpose, "session")
	h.Set(protocol.HeaderWSSessionID, cr.SessionID)
	h.Set(protocol.HeaderSessionToken, cr.SessionToken)
	h.Set(protocol.HeaderWSEpoch, "1")
	return h
}

// A session request that is not a WebSocket upgrade is refused before
// the stream is reserved, leaving the session usable. A finished
// session is answered with 410 and its terminal code, like the HTTP routes.
func TestWSSessionErrors(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	s, ts := egrServer(t, cfg)
	b, _ := json.Marshal(protocol.CreateSessionRequest{Limits: protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 2048, DownBufferBytes: 4096}, ProtocolVersion: "1.4", IngressInstanceID: "ai", CreateRequestID: "w1", TargetID: "db", Transport: protocol.TransportMode{Mode: "ws"}})
	r, _ := http.NewRequest("POST", ts.URL+"/v1/sessions", strings.NewReader(string(b)))
	r.Header.Set("Authorization", "Bearer tokA")
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	var cr protocol.CreateSessionResponse
	_ = json.NewDecoder(resp.Body).Decode(&cr)
	resp.Body.Close()
	plain, _ := http.NewRequest("GET", ts.URL+"/v1/transport/ws", nil)
	plain.Header = wsHeader("tokA", cr)
	plain.Header.Set("Sec-WebSocket-Protocol", protocol.SecWebSocketProtocol)
	resp, e = http.DefaultClient.Do(plain)
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("non-upgrade WS session request: %d", resp.StatusCode)
	}
	sess := s.registry.Get(cr.SessionID)
	if sess == nil || sess.GetState() == session.StateClosed {
		t.Fatal("failed upgrade closed the session")
	}
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/transport/ws"
	c, _, e := (&websocket.Dialer{Subprotocols: []string{protocol.SecWebSocketProtocol}}).Dial(wsURL, wsHeader("tokA", cr))
	if e != nil {
		t.Fatalf("session still attachable: %v", e)
	}
	// Terminate the session while its WS is attached: closing the client
	// carrier first would let the recovery-disabled session end as "failed"
	// before the backend loss is recorded. The carrier then carries the
	// terminal error message.
	sess.CloseWithReason(protocol.ErrBackendLost)
	if notice := readWSTerminalError(t, c); notice.Code != protocol.ErrBackendLost || !notice.SessionClosed || notice.Retryable {
		t.Fatalf("terminal notice: %+v", notice)
	}
	c.Close()
	_, resp, e = (&websocket.Dialer{Subprotocols: []string{protocol.SecWebSocketProtocol}}).Dial(wsURL, wsHeader("tokA", cr))
	if e == nil || resp == nil {
		t.Fatalf("closed session attached: %v", e)
	}
	var er protocol.ErrorResponse
	_ = json.NewDecoder(resp.Body).Decode(&er)
	resp.Body.Close()
	if resp.StatusCode != 410 || er.Code != protocol.ErrBackendLost || !er.SessionClosed {
		t.Fatalf("closed session: %d %+v", resp.StatusCode, er)
	}
}

// WS prechecks take the same per-peer share of the probe budget as
// HTTP probes.
func TestWSPrecheckPeerShare(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{
		{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 2},
		{ID: "C", Tokens: []string{"tokC"}, AllowedTargets: []string{"db"}, MaxSessions: 2},
	})
	s, ts := egrServer(t, cfg)
	global := cfg.Resources.Probes.Limit()
	for i := int64(0); i < auth.PeerShare(global, 2); i++ {
		release, ok := s.auth.ReserveFair("A", "probe", global)
		if !ok {
			t.Fatal("setup")
		}
		defer release()
	}
	h := http.Header{}
	h.Set("Authorization", "Bearer tokA")
	h.Set(protocol.HeaderInstanceID, "bi")
	h.Set(protocol.HeaderWSPurpose, "check")
	h.Set(protocol.HeaderWSNonce, "n1")
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/transport/ws"
	_, resp, e := (&websocket.Dialer{Subprotocols: []string{protocol.SecWebSocketProtocol}}).Dial(wsURL, h)
	if e == nil || resp == nil || resp.StatusCode != 429 {
		t.Fatalf("precheck beyond the peer's probe share: %v %v", resp, e)
	}
	resp.Body.Close()
}

// A backend reset is a fatal non-EOF read error. The bytes read
// before it are settled into the produced offset, then the session is closed
// with BACKEND_LOST at once (no FIN), without waiting for downstream ACKs or
// socket_write_timeout, and /resume can no longer commit a new epoch.
// A session closed without both FINs answers 410 DBB_SESSION_CLOSED
// "session terminated: failed".
func TestBackendResetFencesRecovery(t *testing.T) {
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer ln.Close()
	go func() {
		for {
			c, e := ln.Accept()
			if e != nil {
				return
			}
			_, _ = c.Write([]byte("last error packet"))
			time.Sleep(50 * time.Millisecond)
			_ = c.(*net.TCPConn).SetLinger(0)
			c.Close()
		}
	}()
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	cfg.Targets["db"] = config.TargetConfig{Network: "tcp", Address: ln.Addr().String()}
	cfg.Transport.SocketWriteTimeout = 30 * time.Second
	s, ts := egrServer(t, cfg)
	st, _, cr := egrCreate(t, ts.URL, "tokA", "r1", "db")
	if st != 201 {
		t.Fatalf("create %d", st)
	}
	sess := s.registry.Get(cr.SessionID)
	select {
	case <-sess.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session not closed promptly after backend reset")
	}
	if sess.DownProducedOffset() != uint64(len("last error packet")) {
		t.Fatalf("produced %d", sess.DownProducedOffset())
	}
	if code := sess.TerminalCode(); code != protocol.ErrBackendLost {
		t.Fatalf("terminal code %q", code)
	}
	q := &protocol.ResumeRequest{ResumeRequestID: "after-reset", ExpectedEpoch: "1", IngressInstanceID: "ai", Transport: protocol.TransportMode{Mode: "ws"}, KnownUpAckOffset: "0", UpNextOffset: "0", DownDeliveredOffset: "0"}
	if r, err := sess.Resume(q, time.Second); err == nil || sess.GetState() != session.StateClosed || sess.GetEpoch() != 1 {
		t.Fatalf("resume after backend reset: %+v %v state=%s", r, err, sess.GetState())
	}
	w := httptest.NewRecorder()
	a, b := net.Pipe()
	defer b.Close()
	other := session.NewSession("A", "db", "bi", a, 4096, 2048, 8, 0, false)
	other.Close()
	s.writeSessionError(w, other, session.ErrSessionClosed)
	var er protocol.ErrorResponse
	_ = json.NewDecoder(w.Body).Decode(&er)
	if w.Code != 410 || er.Code != protocol.ErrSessionClosed || er.Message != "session terminated: failed" || !er.SessionClosed {
		t.Fatalf("closed session error: %d %+v", w.Code, er)
	}
}

type flushCounter struct {
	*httptest.ResponseRecorder
	flushes int
}

func (f *flushCounter) Flush()                           { f.flushes++ }
func (f *flushCounter) SetWriteDeadline(time.Time) error { return nil }
func (f *flushCounter) Write(p []byte) (int, error)      { return len(p), nil }

// On a fast link a 1 MiB event is not written in ~1400 flushes.
func TestSSEAdaptivePieces(t *testing.T) {
	w := &flushCounter{ResponseRecorder: httptest.NewRecorder()}
	var mu sync.Mutex
	event := "event: data\ndata: " + strings.Repeat("x", 1<<20) + "\n\n"
	if e := writeSSEProgress(context.Background(), w, http.NewResponseController(w), &mu, []byte(event), time.Minute); e != nil {
		t.Fatal(e)
	}
	if w.flushes > 64 {
		t.Fatalf("%d flushes for a 1 MiB event", w.flushes)
	}
}

// Target resolution follows the caller's context (Close cancels it).
func TestResolveFollowsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if _, e := resolveTarget(ctx, "tcp", "db.example.invalid:3306", nil); e == nil || time.Since(start) > time.Second {
		t.Fatalf("resolution with a cancelled context: %v after %v", e, time.Since(start))
	}
}

// A session request is re-validated against the current credential
// snapshot.
func TestBindCredRevalidates(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	s, ts := egrServer(t, cfg)
	st, _, _ := egrCreate(t, ts.URL, "tokA", "b1", "db")
	if st != 201 {
		t.Fatalf("create %d", st)
	}
	if s.bindCred("A", auth.TokenDigest("Bearer revoked")) {
		t.Fatal("revoked credential accepted")
	}
	if !s.bindCred("A", auth.TokenDigest("Bearer tokA")) {
		t.Fatal("valid credential refused")
	}
}
