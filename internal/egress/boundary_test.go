package egress

import (
	"bytes"
	"crypto/sha256"
	"dbbridge/internal/protocol"
	"encoding/hex"
	"encoding/json"
	"github.com/gorilla/websocket"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type gatedProbeBody struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (b *gatedProbeBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.release
	return 0, io.EOF
}
func (b *gatedProbeBody) Close() error { return nil }

func TestBoundaryProbeCannotStarveDelete(t *testing.T) {
	s, c, _, _ := reviewServer(t)
	c.Transport.UnattachedTimeout = time.Second * 10
	cr := reviewCreate(t, s, "probe-control", "bi", false)
	var created protocol.CreateSessionResponse
	if cr.Code != 201 {
		t.Fatal(cr.Body.String())
	}
	json.Unmarshal(cr.Body.Bytes(), &created)
	var bodies []*gatedProbeBody
	var wg sync.WaitGroup
	defer func() {
		for _, b := range bodies {
			close(b.release)
		}
		wg.Wait()
	}()
	for i := 0; i < 16; i++ {
		b := &gatedProbeBody{started: make(chan struct{}), release: make(chan struct{})}
		bodies = append(bodies, b)
		r := httptest.NewRequest("POST", "/v1/peer/probe", b)
		r.Header.Set("Authorization", "Bearer token")
		wg.Add(1)
		go func() { defer wg.Done(); s.ServeHTTP(httptest.NewRecorder(), r) }()
		select {
		case <-b.started:
		case <-time.After(time.Second):
			t.Fatalf("probe %d not admitted", i)
		}
	}
	r := httptest.NewRequest("DELETE", "/v1/sessions/"+created.SessionID+"?epoch=1", nil)
	r.Header.Set("Authorization", "Bearer token")
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	r.Header.Set(protocol.HeaderSessionToken, created.SessionToken)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	t.Logf("control_metadata=%d/%d delete=%d %s", c.Resources.ControlMetadata.Used(), c.Resources.ControlMetadata.Limit(), w.Code, w.Body.String())
	if w.Code != 200 {
		t.Fatal("16 probes exhausted the control reserve and rejected DELETE")
	}
}

func TestBoundaryNegotiatedUpWindowReported(t *testing.T) {
	s, c, reg, _ := reviewServer(t)
	c.Transport.UnattachedTimeout = time.Second * 10
	c.Transport.UpWindowBytes = 1024
	q := protocol.CreateSessionRequest{ProtocolVersion: "1.4", IngressInstanceID: "ai", CreateRequestID: "negotiated-window", TargetID: "db", Transport: protocol.TransportMode{Mode: "http-sse"}, Limits: protocol.SessionLimits{MaxFrameSize: 8, UpMaxInflightFrames: 2, UpWindowBytes: 16, DownBufferBytes: 32}}
	body, _ := json.Marshal(q)
	r := httptest.NewRequest("POST", "/v1/sessions", bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer token")
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	var cr protocol.CreateSessionResponse
	json.Unmarshal(w.Body.Bytes(), &cr)
	sess := reg.Get(cr.SessionID)
	stream, e := sess.ReserveStream(1, "http-sse")
	if e != nil {
		t.Fatal(e)
	}
	sess.ActivateDataPlane(stream)
	data := []byte("x")
	sum := sha256.Sum256(data)
	r = httptest.NewRequest("POST", "/v1/sessions/"+cr.SessionID+"/up?epoch=1", bytes.NewReader(data))
	for k, v := range map[string]string{"Authorization": "Bearer token", protocol.HeaderInstanceID: "bi", protocol.HeaderSessionToken: cr.SessionToken, protocol.HeaderStreamID: stream, protocol.HeaderOffset: "0", protocol.HeaderLength: "1", protocol.HeaderSHA256: hex.EncodeToString(sum[:]), "Content-Type": "application/octet-stream"} {
		r.Header.Set(k, v)
	}
	w = httptest.NewRecorder()
	s.ServeHTTP(w, r)
	var resp protocol.UpResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	ack, _ := protocol.Offset(resp.AckUpOffset)
	limit, _ := protocol.Offset(resp.UpReceiveLimit)
	t.Logf("negotiated_window=%d ack=%d advertised=%d", cr.Limits.UpWindowBytes, ack, limit)
	if limit-ack != uint64(cr.Limits.UpWindowBytes) {
		t.Fatal("up_receive_limit uses local maximum instead of negotiated window")
	}
}

func TestBoundaryMalformedWSMustTerminateNotSuspend(t *testing.T) {
	for _, scenario := range []string{"flags", "short_header", "short_payload", "wire_rsv"} {
		t.Run(scenario, func(t *testing.T) { testMalformedWSMustTerminate(t, scenario) })
	}
}

func testMalformedWSMustTerminate(t *testing.T, scenario string) {
	s, c, reg, _ := reviewServer(t)
	c.Transport.UnattachedTimeout = 3 * time.Second
	c.Transport.Recovery.Enabled = true
	c.Transport.Recovery.Grace = time.Second
	q := protocol.CreateSessionRequest{ProtocolVersion: "1.4", IngressInstanceID: "ai", CreateRequestID: "malformed-ws", TargetID: "db", Transport: protocol.TransportMode{Mode: "ws"}, Recovery: protocol.RecoveryNegotiation{Enabled: true, GraceMs: 1000}, Limits: protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 1024, DownBufferBytes: 1024}}
	body, _ := json.Marshal(q)
	r := httptest.NewRequest("POST", "/v1/sessions", bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer token")
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	var cr protocol.CreateSessionResponse
	json.Unmarshal(w.Body.Bytes(), &cr)
	sess := reg.Get(cr.SessionID)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	h := http.Header{"Authorization": []string{"Bearer token"}, protocol.HeaderInstanceID: []string{"bi"}, protocol.HeaderSessionToken: []string{cr.SessionToken}, protocol.HeaderWSPurpose: []string{"session"}, protocol.HeaderWSSessionID: []string{cr.SessionID}, protocol.HeaderWSEpoch: []string{"1"}}
	d := websocket.Dialer{Subprotocols: []string{protocol.SecWebSocketProtocol}}
	ws, _, err := d.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/v1/transport/ws", h)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	_, readyBody, err := ws.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var ready protocol.ReadyEventData
	json.Unmarshal(readyBody, &ready)
	if err = sess.ActivateDataPlane(ready.StreamID); err != nil {
		t.Fatal(err)
	}
	msg := protocol.EncodeWSFrame(1, 0, []byte("x"))
	switch scenario {
	case "flags":
		msg[1] = 1
	case "short_header":
		msg = msg[:4]
	case "short_payload":
		msg = msg[:len(msg)-1]
	}
	if scenario == "wire_rsv" {
		_, err = ws.UnderlyingConn().Write([]byte{0xc2, 0x81, 0, 0, 0, 0, 7})
	} else {
		err = ws.WriteMessage(websocket.BinaryMessage, msg)
	}
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(200 * time.Millisecond)
	for sess.GetState() == "ACTIVE" && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	t.Logf("invalid DBBridge flags left backend in state=%s", sess.GetState())
	if sess.GetState() != "CLOSED" {
		t.Fatal("explicit WS protocol violation treated as recoverable transport loss")
	}
}
