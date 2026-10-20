package egress

import (
	"bytes"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCreateRequiresRecoveryObject(t *testing.T) {
	srv, _, _, dials := boundaryServer(t)
	q := protocol.CreateSessionRequest{ProtocolVersion: "1.4", Transport: protocol.TransportMode{Mode: "ws"}, CreateRequestID: "missing-recovery", IngressInstanceID: "ai", TargetID: "db", Limits: protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 1024, DownBufferBytes: 1024}}
	encoded, _ := json.Marshal(q)
	var object map[string]any
	json.Unmarshal(encoded, &object)
	delete(object, "recovery")
	encoded, _ = json.Marshal(object)
	r := httptest.NewRequest("POST", "/v1/sessions", bytes.NewReader(encoded))
	r.Header.Set("Authorization", "Bearer token")
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	t.Logf("missing mandatory recovery object -> HTTP %d", w.Code)
	if w.Code != 400 || dials.Load() != 0 {
		t.Fatalf("incomplete schema: status=%d backend dials=%d", w.Code, dials.Load())
	}
}

func TestRejectedCreateMetadataBudget(t *testing.T) {
	srv, cfg, _, _ := boundaryServer(t)
	const count = 32
	target := strings.Repeat("x", 12<<10)
	for i := 0; i < count; i++ {
		q := protocol.CreateSessionRequest{ProtocolVersion: "1.4", Transport: protocol.TransportMode{Mode: "ws"}, CreateRequestID: fmt.Sprintf("rejected-%d", i), IngressInstanceID: "ai", TargetID: target, Limits: protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 1024, DownBufferBytes: 1024}}
		body, _ := json.Marshal(q)
		r := httptest.NewRequest("POST", "/v1/sessions", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer token")
		r.Header.Set(protocol.HeaderInstanceID, "bi")
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
		if w.Code != 403 {
			t.Fatalf("unexpected response: %d %s", w.Code, w.Body.String())
		}
		used := cfg.Resources.Metadata.Used()
		retry := httptest.NewRequest("POST", "/v1/sessions", bytes.NewReader(body))
		retry.Header = r.Header.Clone()
		cached := httptest.NewRecorder()
		srv.ServeHTTP(cached, retry)
		if cached.Code != 403 || cfg.Resources.Metadata.Used() != used {
			t.Fatal("cached rejection changed status or charged metadata twice")
		}
	}
	// A forbidden target is refused before the idempotency table is
	// touched, so refused creates retain no record and no metadata at all.
	if used := cfg.Resources.Metadata.Used(); used != 0 {
		t.Fatalf("refused creates retained %d bytes of metadata", used)
	}
}

func TestWSWaitsForPayloadBudget(t *testing.T) {
	srv, cfg, reg, _ := boundaryServer(t)
	cfg.Transport.UnattachedTimeout = time.Second
	created := boundaryCreate(t, srv, "payload-pressure", "bi", false)
	if created.Code != 201 {
		t.Fatal(created.Body.String())
	}
	var cr protocol.CreateSessionResponse
	json.Unmarshal(created.Body.Bytes(), &cr)
	sess := reg.Get(cr.SessionID)
	ts := httptest.NewServer(srv.Handler())
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
	if _, err = sess.Ack(1, ready.StreamID, protocol.AckRequest{ReadyReceived: true, DownDeliveredOffset: "0", UpConfirmedOffset: "0", ProbeID: "test-credential"}); err != nil {
		t.Fatal(err)
	}
	// The backend reader holds its pre-read reservation. Consume all remaining
	// process payload budget, leaving codec and control resources available.
	deadline := time.Now().Add(time.Second)
	for cfg.Resources.Payload.Used() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	held := cfg.Resources.Payload.Limit() - cfg.Resources.Payload.Used()
	if !cfg.Resources.Payload.Try(held) {
		t.Fatal("fixture budget reservation")
	}
	defer func() { cfg.Resources.Payload.Put(held) }()
	if err = ws.WriteMessage(websocket.BinaryMessage, protocol.EncodeWSFrame(1, 0, []byte("four"))); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(150 * time.Millisecond)
	for sess.GetState() != session.StateClosed && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	t.Logf("payload exhausted; codec/control available; state=%s terminal_code=%s", sess.GetState(), sess.TerminalCode())
	if sess.GetState() == session.StateClosed {
		t.Fatal("temporary payload pressure closed a valid WS session instead of applying backpressure")
	}
	// Independent HTTP control stays responsive and reports the blocked reader.
	body, _ := json.Marshal(protocol.AckRequest{StreamID: ready.StreamID, DownDeliveredOffset: "0", UpConfirmedOffset: "0", ProbeID: "pressure-control", BackpressureReason: "none"})
	r, _ := http.NewRequest("POST", ts.URL+"/v1/sessions/"+cr.SessionID+"/ack?epoch=1", bytes.NewReader(body))
	r.Header = h.Clone()
	r.Header.Set(protocol.HeaderStreamID, ready.StreamID)
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	var ack protocol.AckResponse
	err = protocol.DecodeJSON(resp.Body, 16<<10, &ack)
	resp.Body.Close()
	if err != nil || resp.StatusCode != 200 || !ack.DataRxBlocked || ack.BackpressureReason != "global_buffer" || ack.ProbeID != "pressure-control" {
		t.Fatalf("control under pressure: status=%d ack=%+v error=%v", resp.StatusCode, ack, err)
	}
	cfg.Resources.Payload.Put(held)
	held = 0
	deadline = time.Now().Add(time.Second)
	for sess.UpAckOffset() != 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if sess.UpAckOffset() != 4 {
		t.Fatalf("frame did not resume after release: state=%s ack=%d", sess.GetState(), sess.UpAckOffset())
	}
	// A following frame proves the duplicate has passed through the same reader.
	baseline := cfg.Resources.Payload.Used()
	if err = ws.WriteMessage(websocket.BinaryMessage, protocol.EncodeWSFrame(1, 0, []byte("four"))); err != nil {
		t.Fatal(err)
	}
	if err = ws.WriteMessage(websocket.BinaryMessage, protocol.EncodeWSFrame(1, 4, []byte("more"))); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(time.Second)
	for (sess.UpAckOffset() != 8 || cfg.Resources.Payload.Used() > baseline) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if sess.UpAckOffset() != 8 || sess.GetState() == session.StateClosed || cfg.Resources.Payload.Used() > baseline {
		t.Fatal("duplicate changed stable progress or leaked a payload lease")
	}
}

func TestDownstreamAdaptiveFrames(t *testing.T) {
	srv, cfg, reg, _ := boundaryServer(t)
	cfg.Transport.FrameSize = 32 << 10
	local, peer := net.Pipe()
	defer peer.Close()
	s := session.NewSession("A", "db", "bi", local, 4<<20, 2<<20, 8, 64, false)
	s.SetNegotiatedLimits(protocol.SessionLimits{MaxFrameSize: 256 << 10, UpWindowBytes: 2 << 20, UpMaxInflightFrames: 8})
	reg.Register(s)
	defer s.Close()
	notify := make(chan struct{}, 1)
	go srv.runBackendReader(s, notify)
	go func() { peer.Write(make([]byte, 1<<20)) }()
	until := time.Now().Add(time.Second)
	for s.DownProducedOffset() < 1<<20 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if s.DownProducedOffset() != 1<<20 {
		t.Fatalf("backend read incomplete: %d", s.DownProducedOffset())
	}
	maxSize, frames := 0, 0
	for off := uint64(0); off < s.DownProducedOffset(); {
		n := s.NextDownFrameBytes(off)
		if n == 0 {
			t.Fatal("missing frame")
		}
		if n > maxSize {
			maxSize = n
		}
		off += uint64(n)
		frames++
	}
	t.Logf("continuous 1 MiB write -> frames=%d max_frame=%d negotiated_max=%d", frames, maxSize, s.NegotiatedMaxFrameSize())
	if maxSize <= 32<<10 {
		t.Fatal("downstream never increased frame size under continuous full reads")
	}
}
