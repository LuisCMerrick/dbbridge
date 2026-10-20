package ingress

import (
	"bytes"
	"dbbridge/internal/auth"
	"dbbridge/internal/config"
	"dbbridge/internal/egress"
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type reviewEnv struct {
	manager      *IngressManager
	switched     atomic.Bool
	primaryDown  atomic.Bool
	backendDials atomic.Int64
	conn         net.Conn
	cfg          *config.Config
	cfgB         *config.Config
	reg          *session.Registry
	received     chan []byte
	created      atomic.Int64
	wsChecks     atomic.Int64
	stop         func()
}

func reviewIngress(t *testing.T, mode, special string) *reviewEnv {
	t.Helper()
	e := &reviewEnv{received: make(chan []byte, 16)}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var backend sync.Map
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			e.backendDials.Add(1)
			backend.Store(c, true)
			go func() {
				defer c.Close()
				if special == "down_fin_first" {
					c.(*net.TCPConn).CloseWrite()
				}
				defer backend.Delete(c)
				buf := make([]byte, 32768)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						p := append([]byte(nil), buf[:n]...)
						select {
						case e.received <- p:
						default:
						}
						if special != "down_fin_first" {
							c.Write(p)
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Setenv("REVIEW_DBB_TOKEN", "review-token")
	b := &config.Config{Node: config.NodeConfig{ID: "B"}, HTTP: config.HTTPConfig{Enabled: true}, Auth: config.AuthConfig{InboundPeers: []config.InboundPeerConfig{{ID: "A", TokenEnvs: []string{"REVIEW_DBB_TOKEN"}, AllowedTargets: []string{"db"}, MaxSessions: 10}}}, Targets: map[string]config.TargetConfig{"db": {Address: ln.Addr().String()}}, Transport: config.TransportConfig{HeartbeatRaw: "50ms", FailureDetectTimeoutRaw: "150ms", Recovery: config.RecoveryConfig{EnabledRaw: false}}}
	if special == "peer_recovery_off" {
		b.Transport.Recovery.EnabledRaw = true
	}
	if special == "recover_ws" || special == "ws_to_sse" || special == "sse_to_ws" || special == "drop_resume" || special == "path_failover" {
		b.Transport.Recovery.EnabledRaw = true
		b.Transport.Recovery.GraceRaw = "1s"
		b.Transport.Recovery.BarrierTimeoutRaw = "100ms"
	}
	if special == "mass_close" {
		b.Auth.InboundPeers[0].MaxSessions = 256
		b.Transport.HeartbeatRaw = "1s"
		b.Transport.FailureDetectTimeoutRaw = "10s"
		b.Transport.ControlRequestTimeoutRaw = "5s"
	}
	if special == "large_stream" {
		b.Transport.HeartbeatRaw = "200ms"
		b.Transport.FailureDetectTimeoutRaw = "3s"
	}
	if err = b.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	e.reg = session.NewRegistry(b.Transport.SuspendedBufferLimit, b.Transport.GlobalBufferLimit)
	e.cfgB = b
	ba := auth.NewAuthenticator(b)
	srv := egress.NewServer(b, ba, e.reg)
	base := srv.Handler()
	var primaryHost string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if special == "path_failover" && e.primaryDown.Load() && r.Host == primaryHost {
			w.WriteHeader(503)
			return
		}
		if r.URL.Path == "/v1/peer/probe" && (special == "ws_to_sse" || special == "sse_to_ws") {
			var pr protocol.ProbeRequest
			json.NewDecoder(r.Body).Decode(&pr)
			modes := []string{"ws", "http-sse"}
			if special == "ws_to_sse" && e.switched.Load() || special == "sse_to_ws" && !e.switched.Load() {
				modes = []string{"http-sse"}
			}
			json.NewEncoder(w).Encode(protocol.ProbeResponse{ProtocolVersion: "1.4", NodeID: "B", InstanceID: b.Node.InstanceID, Nonce: pr.Nonce, AcceptingNewSessions: true, RecoveryAvailable: true, Transports: modes})
			return
		}
		if special == "drop_resume" && strings.HasSuffix(r.URL.Path, "/resume") && !e.switched.Swap(true) {
			rec := httptest.NewRecorder()
			base.ServeHTTP(rec, r)
			h := w.(http.Hijacker)
			conn, _, err := h.Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}

		if r.URL.Path == "/v1/sessions" {
			e.created.Add(1)
		}
		if r.URL.Path == "/v1/transport/ws" && r.Header.Get(protocol.HeaderWSPurpose) == "check" {
			e.wsChecks.Add(1)
			if special == "bad_hello" {
				u := websocket.Upgrader{Subprotocols: []string{protocol.SecWebSocketProtocol}}
				c, err := u.Upgrade(w, r, nil)
				if err == nil {
					c.WriteJSON(protocol.WSHelloMessage{Type: "hello", ProtocolVersion: "1.4", NodeID: "WRONG", InstanceID: b.Node.InstanceID, Nonce: r.Header.Get(protocol.HeaderWSNonce), Transport: "ws"})
					c.Close()
				}
				return
			}
		}
		if strings.HasSuffix(r.URL.Path, "/ack") && (special == "reject_ready" || special == "terminal_ack") {
			body, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(body))
			var req protocol.AckRequest
			json.Unmarshal(body, &req)
			if special == "reject_ready" && req.ReadyReceived {
				w.WriteHeader(403)
				json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: protocol.ErrUnauthorized, SessionClosed: true})
				return
			}
			if special == "terminal_ack" && !req.ReadyReceived {
				w.WriteHeader(410)
				json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: protocol.ErrSessionClosed, SessionClosed: true})
				return
			}
		}
		if strings.HasSuffix(r.URL.Path, "/events") && (special == "sse_blackhole" || special == "eof_stream" || special == "offset_gap") {
			id := strings.Split(r.URL.Path, "/")[3]
			s := e.reg.Get(id)
			stream, _ := s.ReserveStream(1, "http-sse")
			w.Header().Set("Content-Type", "text/event-stream")
			j, _ := json.Marshal(protocol.ReadyEventData{Type: "ready", ProtocolVersion: "1.4", SessionID: id, InstanceID: b.Node.InstanceID, Epoch: "1", StreamID: stream, Transport: "http-sse"})
			fmt.Fprintf(w, "event: ready\ndata: %s\n\n", j)
			w.(http.Flusher).Flush()
			if special == "eof_stream" {
				return
			}
			if special == "offset_gap" {
				ping, _ := json.Marshal(protocol.WSTextControlMessage{Type: "ping", SessionID: id, InstanceID: b.Node.InstanceID, Epoch: "1", StreamID: stream, Nonce: s.Heartbeat()})
				fmt.Fprintf(w, "event: ping\ndata: %s\n\n", ping)
				w.(http.Flusher).Flush()
				time.Sleep(50 * time.Millisecond)
				fmt.Fprintf(w, "id: 1:14\nevent: data\ndata: %s\n\n", protocol.FormatSSEData(10, []byte("oops")))
				w.(http.Flusher).Flush()
			}
			<-r.Context().Done()
			return
		}
		base.ServeHTTP(w, r)
	}))
	primaryHost = strings.TrimPrefix(ts.URL, "http://")
	var secondary *httptest.Server
	if special == "path_failover" {
		secondary = httptest.NewServer(ts.Config.Handler)
	}
	a := &config.Config{Node: config.NodeConfig{ID: "A"}, Peers: []config.PeerConfig{{ID: "B", ExpectedNodeID: "B", CredentialRef: "B", TokenEnv: "REVIEW_DBB_TOKEN", Endpoints: []config.EndpointConfig{{ID: "e1", URL: ts.URL}}}}, Listeners: []config.ListenerConfig{{ID: "local", Listen: "127.0.0.1:0", Peer: "B", Target: "db"}}, Transport: config.TransportConfig{Protocol: mode, HeartbeatRaw: "50ms", FailureDetectTimeoutRaw: "150ms", AckMaxDelayRaw: "30ms", ControlRequestTimeoutRaw: "150ms", Recovery: config.RecoveryConfig{EnabledRaw: false}}}
	if special == "large_stream" {
		a.Transport.HeartbeatRaw = "200ms"
		a.Transport.FailureDetectTimeoutRaw = "3s"
		a.Transport.ControlRequestTimeoutRaw = "1s"
	}
	if special == "mass_close" {
		a.Transport.HeartbeatRaw = "1s"
		a.Transport.FailureDetectTimeoutRaw = "10s"
		a.Transport.ControlRequestTimeoutRaw = "5s"
		a.Transport.CreateRetryTimeoutRaw = "20s"
	}
	if special == "peer_recovery_off" {
		a.Transport.Recovery.EnabledRaw = true
		a.Peers[0].Recovery = &config.PeerRecoveryConfig{Enabled: config.BoolPtr(false)}
	}
	if special == "failover_create" {
		a.Peers[0].Endpoints = append([]config.EndpointConfig{{ID: "dead", URL: "http://127.0.0.1:1", Priority: 0}}, a.Peers[0].Endpoints...)
		a.Peers[0].Endpoints[1].Priority = 10
	}
	if special == "recover_ws" || special == "ws_to_sse" || special == "sse_to_ws" || special == "drop_resume" || special == "path_failover" {
		a.Peers[0].Failover.CooldownRaw = "10ms"
		a.Transport.Recovery.EnabledRaw = true
		a.Transport.Recovery.GraceRaw = "1s"
		a.Transport.Recovery.BarrierTimeoutRaw = "100ms"
	}
	if secondary != nil {
		a.Peers[0].Endpoints = append(a.Peers[0].Endpoints, config.EndpointConfig{ID: "backup", URL: secondary.URL, Priority: 10})
	}
	if err = a.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}

	e.cfg = a
	aa := auth.NewAuthenticator(a)
	m := NewIngressManager(a, aa)
	e.manager = m
	if err = m.Start(); err != nil {
		t.Fatal(err)
	}
	e.conn, err = net.Dial("tcp", m.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	e.stop = func() {
		e.conn.Close()
		m.Close()
		srv.Close()
		ts.Close()
		if secondary != nil {
			secondary.Close()
		}
		aa.Close()
		ba.Close()
		ln.Close()
		backend.Range(func(k, v interface{}) bool { k.(net.Conn).Close(); return true })
	}
	t.Cleanup(e.stop)
	return e
}
func TestRegressionRealByteRoundTrip(t *testing.T) {
	for _, mode := range []string{"ws", "http-sse"} {
		t.Run(mode, func(t *testing.T) {
			e := reviewIngress(t, mode, "")
			p := bytes.Repeat([]byte{0, 255, 7, 22}, 32768)
			e.conn.SetDeadline(time.Now().Add(2 * time.Second))
			if _, err := e.conn.Write(p); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(p))
			n, err := io.ReadFull(e.conn, got)
			if err != nil || !bytes.Equal(got, p) {
				t.Fatalf("128KiB round trip failed: received=%d err=%v", n, err)
			}
		})
	}
}
func TestRegressionReadyAckFailureClosesBeforeData(t *testing.T) {
	e := reviewIngress(t, "ws", "reject_ready")
	e.conn.Write([]byte("unauthorized-write"))
	select {
	case p := <-e.received:
		t.Fatalf("backend got %q despite rejected ready ACK", p)
	case <-time.After(300 * time.Millisecond):
	}
}
func TestRegressionBadWSIdentityMustNotFallback(t *testing.T) {
	e := reviewIngress(t, "auto", "bad_hello")
	deadline := time.Now().Add(time.Second)
	for e.created.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n := e.created.Load(); n != 0 {
		t.Fatalf("created %d session(s) after hello node identity mismatch", n)
	}
}
func TestRegressionSSEBlackholeDetected(t *testing.T) {
	e := reviewIngress(t, "http-sse", "sse_blackhole")
	e.conn.SetReadDeadline(time.Now().Add(600 * time.Millisecond))
	_, err := e.conn.Read(make([]byte, 1))
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("SSE stayed open 600ms despite failure_detect_timeout=150ms and no stream activity; control ACK still healthy")
	}
}
func TestRegressionTerminalAckClosesClient(t *testing.T) {
	e := reviewIngress(t, "ws", "terminal_ack")
	e.conn.SetReadDeadline(time.Now().Add(600 * time.Millisecond))
	_, err := e.conn.Read(make([]byte, 1))
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("authenticated DBB_SESSION_CLOSED ACK stopped worker but left original client TCP open")
	}
}

func TestRegressionRealWSRecovery(t *testing.T) {
	e := reviewIngress(t, "ws", "recover_ws")
	e.conn.SetDeadline(time.Now().Add(3 * time.Second))
	e.conn.Write([]byte("first"))
	p := make([]byte, 5)
	if _, err := io.ReadFull(e.conn, p); err != nil {
		t.Fatal(err)
	}
	sessions := e.reg.GetAllSessions()
	if len(sessions) != 1 {
		t.Fatal("session count", len(sessions))
	}
	s := sessions[0]
	if err := s.SetStreamCancel(s.GetEpoch(), s.CurrentStreamID(), func() {}); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(2 * time.Second)
	for s.GetEpoch() == 1 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	e.conn.Write([]byte("after"))
	if _, err := io.ReadFull(e.conn, p); err != nil {
		t.Fatalf("post-recovery read: %v", err)
	}
	if string(p) != "after" {
		t.Fatal(string(p))
	}
	t.Logf("same session %s resumed epoch=%d", s.ID, s.GetEpoch())
}
