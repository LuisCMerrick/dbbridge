package egress_test

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/egress"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"

	"github.com/gorilla/websocket"
)

func setupTestServer(t *testing.T) (*egress.Server, *config.Config, string) {
	t.Helper()

	// Dummy backend listener
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen dummy backend: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					_, _ = c.Write(buf[:n])
				}
			}(conn)
		}
	}()

	peerToken := "test-secret-token"

	cfg := &config.Config{
		Node: config.NodeConfig{
			ID:         "hq-b",
			InstanceID: "inst_test_egress_123",
		},
		HTTP: config.HTTPConfig{
			Enabled: true,
			Listen:  "127.0.0.1:0",
		},
		Auth: config.AuthConfig{
			InboundPeers: []config.InboundPeerConfig{
				{
					ID:             "plant-a",
					Enabled:        config.BoolPtr(true),
					Tokens:         []string{peerToken},
					AllowedTargets: []string{"dummy-target"},
					MaxSessions:    10,
				},
			},
		},
		Targets: map[string]config.TargetConfig{
			"dummy-target": {
				Network: "tcp",
				Address: ln.Addr().String(),
			},
		},
		Transport: config.TransportConfig{
			Protocol:            "http-sse",
			ProtocolVersion:     "1.4",
			MaxFrameSize:        262144,
			UpWindowBytes:       2 * 1024 * 1024,
			UpMaxInflightFrames: 8,
			BufferPerDirection:  4 * 1024 * 1024,
			DialTimeout:         2 * time.Second,
			Recovery: config.RecoveryConfig{
				Enabled: true,
				Grace:   30 * time.Second,
			},
		},
	}

	authMgr := auth.NewAuthenticator(cfg)
	reg := session.NewRegistry(64*1024*1024, 256*1024*1024)
	srv := egress.NewServer(cfg, authMgr, reg)
	return srv, cfg, peerToken
}

func TestProbeEndpoint(t *testing.T) {
	srv, _, token := setupTestServer(t)

	reqBody, _ := json.Marshal(protocol.ProbeRequest{
		ProtocolVersion: "1.4",
		ExpectedNodeID:  "hq-b",
		Nonce:           "test_nonce_123",
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/peer/probe", bytes.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	var resp protocol.ProbeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode probe response: %v", err)
	}

	if resp.ProtocolVersion != "1.4" || resp.NodeID != "hq-b" || resp.Nonce != "test_nonce_123" {
		t.Fatalf("unexpected probe response: %+v", resp)
	}
}

func TestCreateSessionValidation(t *testing.T) {
	srv, cfg, token := setupTestServer(t)

	// 1. Protocol version mismatch must return 409 DBB_PROTOCOL_MISMATCH
	bodyBadVer, _ := json.Marshal(protocol.CreateSessionRequest{
		Limits:            protocol.SessionLimits{MaxFrameSize: cfg.Transport.MaxFrameSize, UpMaxInflightFrames: cfg.Transport.UpMaxInflightFrames, UpWindowBytes: cfg.Transport.UpWindowBytes, DownBufferBytes: cfg.Transport.BufferPerDirection},
		ProtocolVersion:   "1.2",
		Transport:         protocol.TransportMode{Mode: "ws"},
		CreateRequestID:   "cr_bad_ver",
		IngressInstanceID: "inst_ing_1",
		TargetID:          "dummy-target",
	})
	req1 := httptest.NewRequest(http.MethodPost, "/v1/sessions", bytes.NewReader(bodyBadVer))
	req1.Header.Set("Authorization", "Bearer "+token)
	req1.Header.Set(protocol.HeaderInstanceID, cfg.Node.InstanceID)
	w1 := httptest.NewRecorder()
	srv.ServeHTTP(w1, req1)
	if w1.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on bad protocol version, got %d: %s", w1.Code, w1.Body.String())
	}
	var errResp1 protocol.ErrorResponse
	_ = json.NewDecoder(w1.Body).Decode(&errResp1)
	if errResp1.Code != protocol.ErrProtocolMismatch {
		t.Fatalf("expected DBB_PROTOCOL_MISMATCH, got %s", errResp1.Code)
	}

	// 2. Instance ID mismatch must return 409 DBB_INSTANCE_MISMATCH
	bodyOk, _ := json.Marshal(protocol.CreateSessionRequest{
		Limits:            protocol.SessionLimits{MaxFrameSize: cfg.Transport.MaxFrameSize, UpMaxInflightFrames: cfg.Transport.UpMaxInflightFrames, UpWindowBytes: cfg.Transport.UpWindowBytes, DownBufferBytes: cfg.Transport.BufferPerDirection},
		ProtocolVersion:   "1.4",
		Transport:         protocol.TransportMode{Mode: "ws"},
		CreateRequestID:   "cr_inst_mismatch",
		IngressInstanceID: "inst_ing_1",
		TargetID:          "dummy-target",
	})
	req2 := httptest.NewRequest(http.MethodPost, "/v1/sessions", bytes.NewReader(bodyOk))
	req2.Header.Set("Authorization", "Bearer "+token)
	req2.Header.Set("X-DBB-Instance-ID", "wrong_instance_id")
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, req2)
	if w2.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on instance mismatch, got %d: %s", w2.Code, w2.Body.String())
	}
	var errResp2 protocol.ErrorResponse
	_ = json.NewDecoder(w2.Body).Decode(&errResp2)
	if errResp2.Code != protocol.ErrInstanceMismatch {
		t.Fatalf("expected DBB_INSTANCE_MISMATCH, got %s", errResp2.Code)
	}

	// 3. Successful create with correct X-DBB-Instance-ID
	req3 := httptest.NewRequest(http.MethodPost, "/v1/sessions", bytes.NewReader(bodyOk))
	req3.Header.Set("Authorization", "Bearer "+token)
	req3.Header.Set("X-DBB-Instance-ID", cfg.Node.InstanceID)
	w3 := httptest.NewRecorder()
	srv.ServeHTTP(w3, req3)
	if w3.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created on valid request, got %d: %s", w3.Code, w3.Body.String())
	}

	// 4. Idempotency conflict: same request ID but different target must return 409 DBB_IDEMPOTENCY_CONFLICT
	bodyConflict, _ := json.Marshal(protocol.CreateSessionRequest{
		Limits:            protocol.SessionLimits{MaxFrameSize: cfg.Transport.MaxFrameSize, UpMaxInflightFrames: cfg.Transport.UpMaxInflightFrames, UpWindowBytes: cfg.Transport.UpWindowBytes, DownBufferBytes: cfg.Transport.BufferPerDirection},
		ProtocolVersion:   "1.4",
		Transport:         protocol.TransportMode{Mode: "ws"},
		CreateRequestID:   "cr_inst_mismatch",
		IngressInstanceID: "inst_ing_1",
		TargetID:          "different-target",
	})
	req4 := httptest.NewRequest(http.MethodPost, "/v1/sessions", bytes.NewReader(bodyConflict))
	req4.Header.Set("Authorization", "Bearer "+token)
	req4.Header.Set(protocol.HeaderInstanceID, cfg.Node.InstanceID)
	w4 := httptest.NewRecorder()
	srv.ServeHTTP(w4, req4)
	if w4.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on idempotency param conflict, got %d: %s", w4.Code, w4.Body.String())
	}
	var errResp4 protocol.ErrorResponse
	_ = json.NewDecoder(w4.Body).Decode(&errResp4)
	if errResp4.Code != protocol.ErrIdempotencyConflict {
		t.Fatalf("expected DBB_IDEMPOTENCY_CONFLICT, got %s", errResp4.Code)
	}
}

func TestAuditControlBodyLimit(t *testing.T) {
	srv, _, token := setupTestServer(t)

	// Send probe request with body > 16 KiB (e.g. 20,000 bytes)
	largeBody := bytes.Repeat([]byte("a"), 20000)
	req := httptest.NewRequest(http.MethodPost, "/v1/peer/probe", bytes.NewReader(largeBody))
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request on >16KiB control body, got %d", w.Code)
	}
}

func TestAuditInvalidCIDRMustFailClosed(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on dummy target: %v", err)
	}
	defer ln.Close()

	peerToken := "test-token"
	cfg := &config.Config{
		Node: config.NodeConfig{
			ID:         "hq-b",
			InstanceID: "inst_test_egress_cidr",
		},
		HTTP: config.HTTPConfig{
			Enabled: true,
			Listen:  "127.0.0.1:0",
		},
		Auth: config.AuthConfig{
			InboundPeers: []config.InboundPeerConfig{
				{
					ID:             "plant-a",
					Enabled:        config.BoolPtr(true),
					Tokens:         []string{peerToken},
					AllowedTargets: []string{"cidr-target"},
					MaxSessions:    10,
				},
			},
		},
		Targets: map[string]config.TargetConfig{
			"cidr-target": {
				Network:      "tcp",
				Address:      ln.Addr().String(),
				AllowedCIDRs: []string{"not-a-cidr"}, // Invalid CIDRs
			},
		},
		Transport: config.TransportConfig{
			Protocol:        "http-sse",
			ProtocolVersion: "1.4",
			MaxFrameSize:    262144,
			DialTimeout:     2 * time.Second,
		},
	}

	authMgr := auth.NewAuthenticator(cfg)
	reg := session.NewRegistry(64*1024*1024, 256*1024*1024)
	srv := egress.NewServer(cfg, authMgr, reg)

	bodyOk, _ := json.Marshal(protocol.CreateSessionRequest{
		Limits:            protocol.SessionLimits{MaxFrameSize: cfg.Transport.MaxFrameSize, UpMaxInflightFrames: cfg.Transport.UpMaxInflightFrames, UpWindowBytes: cfg.Transport.UpWindowBytes, DownBufferBytes: cfg.Transport.BufferPerDirection},
		ProtocolVersion:   "1.4",
		Transport:         protocol.TransportMode{Mode: "ws"},
		CreateRequestID:   "cr_cidr_fail",
		IngressInstanceID: "inst_ing_1",
		TargetID:          "cidr-target",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/sessions", bytes.NewReader(bodyOk))
	req.Header.Set("Authorization", "Bearer "+peerToken)
	req.Header.Set("X-DBB-Instance-ID", cfg.Node.InstanceID)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	// Must fail closed on invalid CIDR whitelist
	if w.Code == http.StatusCreated {
		t.Fatalf("expected dial to fail closed on invalid CIDR whitelist, got 201 Created")
	}
}

func TestWSPrecheckHello(t *testing.T) {
	srv, cfg, peerToken := setupTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/transport/ws"
	header := http.Header{}
	header.Set("Authorization", "Bearer "+peerToken)
	header.Set("X-DBB-Instance-ID", cfg.Node.InstanceID)
	header.Set("X-DBB-WS-Purpose", "check")
	header.Set("X-DBB-Nonce", "nonce-check-test-999")

	dialer := websocket.Dialer{
		Subprotocols: []string{protocol.SecWebSocketProtocol},
	}
	conn, resp, err := dialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("ws precheck dial failed: %v, resp: %+v", err, resp)
	}
	defer conn.Close()

	if conn.Subprotocol() != protocol.SecWebSocketProtocol {
		t.Fatalf("expected subprotocol %s, got %s", protocol.SecWebSocketProtocol, conn.Subprotocol())
	}

	msgType, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("failed to read hello message: %v", err)
	}
	if msgType != websocket.TextMessage {
		t.Fatalf("expected TextMessage, got %d", msgType)
	}

	var hello protocol.WSHelloMessage
	if err := json.Unmarshal(data, &hello); err != nil {
		t.Fatalf("failed to unmarshal hello message: %v", err)
	}

	if hello.Type != "hello" || hello.ProtocolVersion != "1.4" || hello.Nonce != "nonce-check-test-999" || hello.NodeID != cfg.Node.ID || hello.Transport != "ws" {
		t.Fatalf("unexpected hello message payload: %+v", hello)
	}
}

func TestWSSessionBidirectionalAndMismatch(t *testing.T) {
	srv, cfg, peerToken := setupTestServer(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. Create a session with transport.mode = "ws"
	createBody, _ := json.Marshal(protocol.CreateSessionRequest{
		Limits:            protocol.SessionLimits{MaxFrameSize: cfg.Transport.MaxFrameSize, UpMaxInflightFrames: cfg.Transport.UpMaxInflightFrames, UpWindowBytes: cfg.Transport.UpWindowBytes, DownBufferBytes: cfg.Transport.BufferPerDirection},
		ProtocolVersion:   "1.4",
		Transport:         protocol.TransportMode{Mode: "ws"},
		CreateRequestID:   "cr_ws_test_01",
		IngressInstanceID: "inst_ing_1",
		TargetID:          "dummy-target",
	})
	cReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/sessions", bytes.NewReader(createBody))
	cReq.Header.Set("Authorization", "Bearer "+peerToken)
	cReq.Header.Set("X-DBB-Instance-ID", cfg.Node.InstanceID)
	cResp, err := http.DefaultClient.Do(cReq)
	if err != nil || cResp.StatusCode != http.StatusCreated {
		t.Fatalf("create session failed: status=%v, err=%v", cResp.StatusCode, err)
	}
	var created protocol.CreateSessionResponse
	_ = json.NewDecoder(cResp.Body).Decode(&created)
	cResp.Body.Close()

	if created.Transport.Mode != "ws" {
		t.Fatalf("expected create response transport ws, got %s", created.Transport.Mode)
	}

	// 2. Test mismatch: /up is forbidden in ws mode
	upReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/sessions/"+created.SessionID+"/up?epoch=1", bytes.NewReader([]byte("test")))
	upReq.Header.Set("Authorization", "Bearer "+peerToken)
	upReq.Header.Set("X-DBB-Instance-ID", cfg.Node.InstanceID)
	upReq.Header.Set("X-DBB-Session-Token", created.SessionToken)
	upReq.Header.Set("X-DBB-Offset", "0")
	upReq.Header.Set("X-DBB-Length", "4")
	upReq.Header.Set("X-DBB-SHA256", "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08")
	upResp, err := http.DefaultClient.Do(upReq)
	if err != nil {
		t.Fatalf("do upReq error: %v", err)
	}
	upResp.Body.Close()
	if upResp.StatusCode != http.StatusConflict {
		t.Fatalf("expected /up in ws mode to return 409 Conflict, got %d", upResp.StatusCode)
	}

	// 3. Connect via WebSocket with purpose=session
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/transport/ws"
	header := http.Header{}
	header.Set("Authorization", "Bearer "+peerToken)
	header.Set("X-DBB-Instance-ID", cfg.Node.InstanceID)
	header.Set("X-DBB-WS-Purpose", "session")
	header.Set("X-DBB-Session-ID", created.SessionID)
	header.Set("X-DBB-Session-Token", created.SessionToken)
	header.Set("X-DBB-Epoch", "1")

	dialer := websocket.Dialer{
		Subprotocols: []string{protocol.SecWebSocketProtocol},
	}
	conn, resp, err := dialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("ws session dial failed: %v, resp=%+v", err, resp)
	}
	defer conn.Close()

	// Read initial ready text message
	msgType, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("failed to read ws ready message: %v", err)
	}
	if msgType != websocket.TextMessage {
		t.Fatalf("expected ready TextMessage, got %d", msgType)
	}
	var ready protocol.ReadyEventData
	if err := json.Unmarshal(data, &ready); err != nil {
		t.Fatalf("failed to unmarshal ready message: %v", err)
	}
	if ready.Type != "ready" || ready.StreamID == "" || ready.Transport != "ws" {
		t.Fatalf("unexpected ready message: %+v", ready)
	}

	// 4. Activate data plane via /ack
	ackBody, _ := json.Marshal(protocol.AckRequest{
		DownDeliveredOffset: "0", UpConfirmedOffset: "0", ReadyReceived: true,
		StreamID: ready.StreamID,
	})
	ackReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/sessions/"+created.SessionID+"/ack?epoch=1", bytes.NewReader(ackBody))
	ackReq.Header.Set("Authorization", "Bearer "+peerToken)
	ackReq.Header.Set("X-DBB-Instance-ID", cfg.Node.InstanceID)
	ackReq.Header.Set("X-DBB-Session-Token", created.SessionToken)
	ackReq.Header.Set("X-DBB-Stream-ID", ready.StreamID)
	ackResp, err := http.DefaultClient.Do(ackReq)
	if err != nil || ackResp.StatusCode != http.StatusOK {
		t.Fatalf("ack activation failed: status=%v, err=%v", ackResp.StatusCode, err)
	}
	ackResp.Body.Close()

	// 5. Send binary data frame over WS
	testPayload := []byte("PING DATABASE VIA WS")
	binFrame := protocol.EncodeWSFrame(1, 0, testPayload)
	if err := conn.WriteMessage(websocket.BinaryMessage, binFrame); err != nil {
		t.Fatalf("failed to write ws binary frame: %v", err)
	}

	// 6. Expect ack_up text message or binary echo from dummy backend
	gotAckUp := false
	gotEcho := false
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for i := 0; i < 5 && (!gotAckUp || !gotEcho); i++ {
		mType, mData, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("iter %d error reading from ws (gotAckUp=%v, gotEcho=%v): %v", i, gotAckUp, gotEcho, err)
		}
		t.Logf("iter %d read msgType=%d len=%d", i, mType, len(mData))
		if mType == websocket.TextMessage {
			var ctrl protocol.WSTextControlMessage
			_ = json.Unmarshal(mData, &ctrl)
			if ctrl.Type == "ack_up" {
				gotAckUp = true
			}
		} else if mType == websocket.BinaryMessage {
			frameEpoch, frameOffset, payload, err := protocol.DecodeWSFrame(mData)
			if err == nil && frameEpoch == 1 && frameOffset == 0 && string(payload) == string(testPayload) {
				gotEcho = true
			}
		}
	}

	if !gotAckUp {
		t.Errorf("expected ack_up control message on WS")
	}
	if !gotEcho {
		t.Errorf("expected binary echo frame from dummy backend on WS")
	}
}
