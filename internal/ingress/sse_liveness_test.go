package ingress

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/egress"
	"dbbridge/internal/session"
)

// commentGateway passes the ready event and the first ping, then turns every
// later SSE write into a proxy keep-alive comment.
type commentGateway struct {
	http.ResponseWriter
	firstPing bool
}

func (w *commentGateway) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *commentGateway) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("event: ready\n")) {
		return w.ResponseWriter.Write(p)
	}
	if !w.firstPing && bytes.Contains(p, []byte("event: ping\n")) {
		w.firstPing = true
		return w.ResponseWriter.Write(p)
	}
	_, err := io.WriteString(w.ResponseWriter, ": gateway keepalive\n\n")
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// A gateway that forwards ready and the first ping, then replaces every SSE
// event with a comment, must not keep the session alive: the ingress detects
// the dead downstream although the separate control API stays healthy.
func TestSSECommentOnlyStreamIsDetected(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	go func() {
		for {
			c, e := backend.Accept()
			if e != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	t.Setenv("DBB_TEST_TOKEN", strings.Repeat("a", 64))
	b := &config.Config{Node: config.NodeConfig{ID: "B"}, HTTP: config.HTTPConfig{Enabled: true}, Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{{ID: "A", TokenEnvs: []string{"DBB_TEST_TOKEN"}, AllowedTargets: []string{"db"}}}}, Targets: map[string]config.TargetConfig{"db": {Address: backend.Addr().String()}}, Transport: config.TransportConfig{HeartbeatRaw: "30ms", FailureDetectTimeoutRaw: "150ms", Recovery: config.RecoveryConfig{EnabledRaw: false}}}
	if err = b.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	ba := auth.NewAuthenticator(b)
	defer ba.Close()
	reg := session.NewRegistry(b.Transport.SuspendedBufferLimit, b.Transport.GlobalBufferLimit)
	srv := egress.NewServer(b, ba, reg)
	defer srv.Close()
	base := srv.Handler()
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/events") {
			base.ServeHTTP(&commentGateway{ResponseWriter: w}, r)
			return
		}
		base.ServeHTTP(w, r)
	}))
	defer gw.Close()
	a := &config.Config{Node: config.NodeConfig{ID: "A"}, Peers: []config.PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "DBB_TEST_TOKEN", Endpoints: []config.EndpointConfig{{ID: "primary", URL: gw.URL}}}}, Listeners: []config.ListenerConfig{{ID: "local", Listen: "127.0.0.1:0", Peer: "B", Target: "db"}}, Transport: config.TransportConfig{Protocol: "http-sse", HeartbeatRaw: "30ms", FailureDetectTimeoutRaw: "150ms", AckMaxDelayRaw: "20ms", ControlRequestTimeoutRaw: "250ms", Recovery: config.RecoveryConfig{EnabledRaw: false}}}
	if err = a.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	aa := auth.NewAuthenticator(a)
	defer aa.Close()
	m := NewIngressManager(a, aa)
	if err = m.Start(); err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	c, err := net.Dial("tcp", m.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var s *ingressSession
	for end := time.Now().Add(time.Second); time.Now().Before(end); {
		m.activeConns.Range(func(_, v any) bool { s = v.(*activeConnEntry).session; return false })
		if s != nil {
			s.mu.Lock()
			active := s.g != nil && s.g.active
			s.mu.Unlock()
			if active {
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if s == nil {
		t.Fatal("fixture session not created")
	}
	s.mu.Lock()
	active := s.g != nil && s.g.active
	s.mu.Unlock()
	if !active {
		t.Fatal("fixture never activated")
	}
	timer := time.NewTimer(900 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
		return
	case <-timer.C:
	}
	s.mu.Lock()
	controlAge := time.Since(s.lastControl)
	s.mu.Unlock()
	t.Fatalf("session still active after 6 detection intervals of comment-only SSE; independent control age=%v, backend sessions=%d, recovery disabled", controlAge, len(reg.GetAllSessions()))
}

// An http-sse session whose /up POSTs return 202 Accepted without ever
// advancing AckUpOffset must fault within failure_detect_timeout even while
// SSE downstream pings and /ack controls remain healthy.
func TestSSEUpstreamUnconfirmedFrameIsDetected(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	go func() {
		for {
			c, e := backend.Accept()
			if e != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	t.Setenv("DBB_TEST_TOKEN", strings.Repeat("b", 64))
	b := &config.Config{Node: config.NodeConfig{ID: "B"}, HTTP: config.HTTPConfig{Enabled: true}, Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{{ID: "A", TokenEnvs: []string{"DBB_TEST_TOKEN"}, AllowedTargets: []string{"db"}}}}, Targets: map[string]config.TargetConfig{"db": {Address: backend.Addr().String()}}, Transport: config.TransportConfig{HeartbeatRaw: "30ms", FailureDetectTimeoutRaw: "150ms", Recovery: config.RecoveryConfig{EnabledRaw: false}}}
	if err = b.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	ba := auth.NewAuthenticator(b)
	defer ba.Close()
	reg := session.NewRegistry(b.Transport.SuspendedBufferLimit, b.Transport.GlobalBufferLimit)
	srv := egress.NewServer(b, ba, reg)
	defer srv.Close()
	base := srv.Handler()
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/up") {
			_, _ = io.Copy(io.Discard, r.Body)
			var s *session.Session
			for _, cand := range reg.GetAllSessions() {
				s = cand
			}
			if s == nil {
				w.WriteHeader(404)
				return
			}
			snap := s.Snapshot()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(202)
			_, _ = io.WriteString(w, `{"session_id":"`+snap.SessionID+`","instance_id":"`+snap.InstanceID+`","epoch":"`+snap.Epoch+`","frame_accepted":true,"ack_up_offset":"0","down_ack_offset":"0","up_receive_limit":"2097152","up_fin_applied":false,"stream_id":"`+snap.StreamID+`","transport":{"mode":"http-sse"}}`)
			return
		}
		base.ServeHTTP(w, r)
	}))
	defer gw.Close()
	a := &config.Config{Node: config.NodeConfig{ID: "A"}, Peers: []config.PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "DBB_TEST_TOKEN", Endpoints: []config.EndpointConfig{{ID: "primary", URL: gw.URL}}}}, Listeners: []config.ListenerConfig{{ID: "local", Listen: "127.0.0.1:0", Peer: "B", Target: "db"}}, Transport: config.TransportConfig{Protocol: "http-sse", HeartbeatRaw: "30ms", FailureDetectTimeoutRaw: "150ms", AckMaxDelayRaw: "20ms", ControlRequestTimeoutRaw: "250ms", Recovery: config.RecoveryConfig{EnabledRaw: false}}}
	if err = a.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	aa := auth.NewAuthenticator(a)
	defer aa.Close()
	m := NewIngressManager(a, aa)
	if err = m.Start(); err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	c, err := net.Dial("tcp", m.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("short-query")); err != nil {
		t.Fatal(err)
	}
	var s *ingressSession
	for end := time.Now().Add(time.Second); time.Now().Before(end); {
		m.activeConns.Range(func(_, v any) bool { s = v.(*activeConnEntry).session; return false })
		if s != nil {
			s.mu.Lock()
			hasFrame := len(s.frames) == 1
			cost := int64(0)
			if hasFrame {
				cost = s.frames[0].cost
			}
			s.mu.Unlock()
			if hasFrame {
				if cost != int64(len("short-query")) {
					t.Fatalf("short read retained %d bytes of payload budget, want %d", cost, len("short-query"))
				}
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if s == nil {
		t.Fatal("fixture session not created")
	}
	timer := time.NewTimer(900 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
	case <-timer.C:
		t.Fatal("http-sse session with unconfirmed /up frame stayed alive past failure_detect_timeout")
	}
}
