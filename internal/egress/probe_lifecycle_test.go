package egress

import (
	"bytes"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"
)

func TestInvalidNegotiationRejectedBeforeDial(t *testing.T) {
	s, _, _, count := boundaryServer(t)
	q := protocol.CreateSessionRequest{ProtocolVersion: "1.4", IngressInstanceID: "ai", CreateRequestID: "invalid-limits", TargetID: "db", Transport: protocol.TransportMode{Mode: "ws"}, Recovery: protocol.RecoveryNegotiation{}, Limits: protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 1, DownBufferBytes: 1024}}
	b, _ := json.Marshal(q)
	r := httptest.NewRequest("POST", "/v1/sessions", bytes.NewReader(b))
	r.Header.Set("Authorization", "Bearer token")
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	time.Sleep(10 * time.Millisecond)
	if n := count.Load(); n != 0 {
		t.Fatalf("invalid negotiation returned HTTP %d after already dialing backend %d time(s)", w.Code, n)
	}
}

func TestProbeKeepsReservedControlAdmission(t *testing.T) {
	s, c, _, _ := boundaryServer(t)
	c.Resources.DataRequests = resource.New(1)
	if !c.Resources.DataRequests.Try(1) {
		t.Fatal("fixture admission")
	}
	defer c.Resources.DataRequests.Put(1)
	if !c.Resources.Metadata.Try(c.Resources.Metadata.Limit()) {
		t.Fatal("fixture metadata")
	}
	defer c.Resources.Metadata.Put(c.Resources.Metadata.Limit())
	q, _ := json.Marshal(protocol.ProbeRequest{ProtocolVersion: "1.4", ExpectedNodeID: "B", Nonce: "reserved-control-probe"})
	r := httptest.NewRequest("POST", "/v1/peer/probe", bytes.NewReader(q))
	r.Header.Set("Authorization", "Bearer token")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("data saturation starved reserved control: %d %s", w.Code, w.Body.String())
	}
}
func TestGraceMillisecondsOverflow(t *testing.T) {
	s, c, reg, _ := boundaryServer(t)
	c.Transport.Recovery.Enabled = true
	c.Transport.Recovery.Grace = time.Second
	q := protocol.CreateSessionRequest{ProtocolVersion: "1.4", IngressInstanceID: "ai", CreateRequestID: "overflow-grace", TargetID: "db", Transport: protocol.TransportMode{Mode: "ws"}, Recovery: protocol.RecoveryNegotiation{Enabled: true, GraceMs: 9223372036854775807}, Limits: protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 1024, DownBufferBytes: 1024}}
	b, _ := json.Marshal(q)
	r := httptest.NewRequest("POST", "/v1/sessions", bytes.NewReader(b))
	r.Header.Set("Authorization", "Bearer token")
	r.Header.Set(protocol.HeaderInstanceID, "bi")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	var result protocol.CreateSessionResponse
	json.Unmarshal(w.Body.Bytes(), &result)
	if w.Code == 201 && result.Recovery.GraceMs <= 0 {
		t.Fatalf("overflow produced successful invalid recovery contract grace_ms=%d active=%d", result.Recovery.GraceMs, len(reg.GetAllSessions()))
	}
}
