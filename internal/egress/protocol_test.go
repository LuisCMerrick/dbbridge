package egress

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"github.com/gorilla/websocket"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func boundaryServer(t *testing.T) (*Server, *config.Config, *session.Registry, *atomic.Int64) {
	t.Helper()
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	var count atomic.Int64
	go func() {
		for {
			c, e := ln.Accept()
			if e != nil {
				return
			}
			count.Add(1)
			go func() {
				buf := make([]byte, 8)
				for {
					if _, e := c.Read(buf); e != nil {
						c.Close()
						return
					}
				}
			}()
		}
	}()
	cfg := &config.Config{Node: config.NodeConfig{ID: "B", InstanceID: "bi"}, HTTP: config.HTTPConfig{AllowedTransports: []string{"ws", "http-sse"}}, Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{{ID: "A", Tokens: []string{"token"}, AllowedTargets: []string{"db"}, MaxSessions: 1}}}, Targets: map[string]config.TargetConfig{"db": {Address: ln.Addr().String()}}, Transport: config.TransportConfig{MaxFrameSize: 1024, BufferPerDirection: 1024, UpWindowBytes: 1024, UpMaxInflightFrames: 8, DialTimeout: time.Second, UnattachedTimeout: 30 * time.Millisecond, Heartbeat: time.Second}}
	reg := session.NewRegistry(1024, 8192)
	a := auth.NewAuthenticator(cfg)
	srv := NewServer(cfg, a, reg)
	t.Cleanup(func() { reg.CloseAll(); a.Close(); ln.Close() })
	return srv, cfg, reg, &count
}
func boundaryCreate(t *testing.T, s *Server, id, inst string, recovery bool) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(protocol.CreateSessionRequest{Limits: protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 1024, DownBufferBytes: 1024}, ProtocolVersion: "1.4", IngressInstanceID: "ai", CreateRequestID: id, TargetID: "db", Transport: protocol.TransportMode{Mode: "ws"}, Recovery: protocol.RecoveryNegotiation{Enabled: recovery, GraceMs: func() int64 {
		if recovery {
			return 10
		}
		return 0
	}()}})
	r := httptest.NewRequest("POST", "/v1/sessions", bytes.NewReader(b))
	r.Header.Set("Authorization", "Bearer token")
	if inst != "" {
		r.Header.Set(protocol.HeaderInstanceID, inst)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}
func TestCreateRequiresInstance(t *testing.T) {
	s, _, _, _ := boundaryServer(t)
	w := boundaryCreate(t, s, "c1", "", false)
	if w.Code == 201 {
		t.Fatal("creation without bound X-DBB-Instance-ID returned 201")
	}
}
func TestRecoveryRefusalBeforeDial(t *testing.T) {
	s, _, _, count := boundaryServer(t)
	w := boundaryCreate(t, s, "c1", "bi", true)
	time.Sleep(10 * time.Millisecond)
	if w.Code != 409 {
		t.Fatalf("expected refusal, got %d", w.Code)
	}
	if n := count.Load(); n != 0 {
		t.Fatalf("unsupported recovery already dialed backend %d time(s)", n)
	}
}
func TestQuotaReleasedAfterOrphan(t *testing.T) {
	s, _, reg, _ := boundaryServer(t)
	w := boundaryCreate(t, s, "c1", "bi", false)
	if w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	deadline := time.Now().Add(time.Second)
	for len(reg.GetAllSessions()) > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	w = boundaryCreate(t, s, "c2", "bi", false)
	if w.Code != 201 {
		t.Fatalf("orphan removed from registry, but next create rejected %d: %s", w.Code, w.Body.String())
	}
}
func TestDuplicateJSON(t *testing.T) {
	s, _, _, _ := boundaryServer(t)
	r := httptest.NewRequest("POST", "/v1/peer/probe", strings.NewReader(`{"protocol_version":"wrong","protocol_version":"1.4","expected_node_id":"B","nonce":"x"}`))
	r.Header.Set("Authorization", "Bearer token")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code == 200 {
		t.Fatal("duplicate critical protocol_version accepted")
	}
}
func TestWSRequiresSubprotocolAndInstance(t *testing.T) {
	s, _, _, _ := boundaryServer(t)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	h := http.Header{"Authorization": []string{"Bearer token"}, protocol.HeaderWSPurpose: []string{"check"}, protocol.HeaderWSNonce: []string{"nonce"}}
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/transport/ws"
	conn, resp, err := websocket.DefaultDialer.Dial(url, h)
	if err == nil {
		conn.Close()
		t.Fatalf("101 allowed without instance or required subprotocol; selected=%q cache-control=%q", resp.Header.Get("Sec-WebSocket-Protocol"), resp.Header.Get("Cache-Control"))
	}
}
func TestSessionTokenURL(t *testing.T) {
	s, _, _, _ := boundaryServer(t)
	s.cfg.Transport.UnattachedTimeout = time.Second
	w := boundaryCreate(t, s, "c1", "bi", false)
	var c protocol.CreateSessionResponse
	json.Unmarshal(w.Body.Bytes(), &c)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	h := http.Header{"Authorization": []string{"Bearer token"}, protocol.HeaderInstanceID: []string{"bi"}}
	u := "ws" + strings.TrimPrefix(ts.URL, "http") + fmt.Sprintf("/v1/transport/ws?purpose=session&session_id=%s&epoch=1&session_token=%s", c.SessionID, c.SessionToken)
	d := websocket.Dialer{Subprotocols: []string{protocol.SecWebSocketProtocol}}
	conn, _, err := d.Dial(u, h)
	if err == nil {
		conn.Close()
		t.Fatal("WS session secret in URL was accepted")
	}
}
func TestFINAckRace(t *testing.T) {
	srv, _, reg, _ := boundaryServer(t)
	for i := 0; i < 100; i++ {
		a, b := net.Pipe()
		s := session.NewSession("A", "db", "bi", a, 1024, 1024, 8, 64, false)
		reg.Register(s)
		done := make(chan struct{})
		go func() { s.IngestUpFIN(1, 0); close(done) }()
		for j := 0; j < 3; j++ {
			r := httptest.NewRequest("POST", "/v1/sessions/"+s.ID+"/ack?epoch=1", strings.NewReader(`{}`))
			r.Header.Set("Authorization", "Bearer token")
			r.Header.Set(protocol.HeaderSessionToken, s.SessionToken)
			r.Header.Set(protocol.HeaderInstanceID, "bi")
			srv.ServeHTTP(httptest.NewRecorder(), r)
		}
		<-done
		s.Close()
		b.Close()
	}
}
func TestDirectTLSHTTP2(t *testing.T) {
	srv, cfg, _, _ := boundaryServer(t)
	dummy := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	cert := dummy.TLS.Certificates[0]
	dummy.Close()
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0600)
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600)
	cfg.HTTP.Listen = "127.0.0.1:0"
	cfg.HTTP.TLS = config.TLSConfig{Enabled: true, CertFile: certPath, KeyFile: keyPath}
	if err = srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	tr := &http.Transport{ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: time.Second}
	r, _ := http.NewRequest("POST", "https://"+srv.listener.Addr().String()+"/v1/peer/probe", strings.NewReader(`{"protocol_version":"1.4","expected_node_id":"B","nonce":"n"}`))
	r.Header.Set("Authorization", "Bearer token")
	resp, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("direct TLS negotiated %s, ALPN=%q", resp.Proto, resp.TLS.NegotiatedProtocol)
	}
}
