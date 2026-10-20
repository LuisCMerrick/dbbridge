package ingress

import (
	"context"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"dbbridge/internal/transport"
	"encoding/json"
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestBoundaryFINThrottleDoesNotFaultSession(t *testing.T) {
	var attempts atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(protocol.HeaderInstanceID, "bi")
		if attempts.Add(1) == 1 {
			w.WriteHeader(429)
			json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: protocol.ErrQuotaExceeded, Retryable: true})
			return
		}
		json.NewEncoder(w).Encode(protocol.FinResponse{SessionID: "sid", InstanceID: "bi", Epoch: "1", StreamID: "stream", Transport: &protocol.TransportMode{Mode: "ws"}, UpFinApplied: true})
	}))
	defer ts.Close()
	cfg := &config.Config{Transport: config.TransportConfig{ControlRequestTimeout: time.Second, DialTimeout: time.Second}}
	c := transport.NewTripleTransportClientForURL(ts.URL, "token", &cfg.Transport, "ai", true)
	defer c.Close()
	c.BindStream("sid", 1, "stream", "ws")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := &generation{ctx: ctx, cancel: cancel, c: c, ep: 1, mode: "ws", stream: "stream", active: true, sent: map[*retainedChunk]bool{}}
	s := &ingressSession{m: &IngressManager{cfg: cfg}, ctx: ctx, cancel: cancel, g: g, sid: "sid", owner: "bi", changed: make(chan struct{}), faults: make(chan sessionFault, 1), ackNotify: make(chan struct{}, 1), upEOF: true}
	done := make(chan struct{})
	go func() { defer close(done); s.send(g) }()
	defer func() { cancel(); <-done }()
	select {
	case f := <-s.faults:
		t.Fatalf("side-effect-free FIN 429 triggered session failure after %d request(s): %v", attempts.Load(), f.err)
	case <-time.After(350 * time.Millisecond):
		if attempts.Load() < 2 {
			t.Fatal("FIN not retried")
		}
	}
}

func TestBoundaryRemoteBackpressureReadDoesNotExpireDetectTimer(t *testing.T) {
	closeServer := make(chan struct{})
	var controls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transport/ws" {
			up := websocket.Upgrader{Subprotocols: []string{protocol.SecWebSocketProtocol}}
			c, e := up.Upgrade(w, r, nil)
			if e != nil {
				return
			}
			defer c.Close()
			c.WriteJSON(protocol.ReadyEventData{Type: "ready", ProtocolVersion: "1.4", SessionID: "sid", InstanceID: "bi", Epoch: "1", StreamID: "stream", Transport: "ws"})
			<-closeServer
			return
		}
		var q protocol.AckRequest
		json.NewDecoder(r.Body).Decode(&q)
		controls.Add(1)
		json.NewEncoder(w).Encode(protocol.AckResponse{SessionID: "sid", InstanceID: "bi", Epoch: "1", StreamID: "stream", Transport: &protocol.TransportMode{Mode: "ws"}, AckUpOffset: "0", DownAckOffset: "0", ProbeID: q.ProbeID, DataTxBlocked: true, BackpressureReason: "data_channel_write"})
	}))
	defer ts.Close()
	defer close(closeServer)
	cfg := &config.Config{Resources: resource.NewManager(1<<20, 1<<20, 1<<20, 10), Transport: config.TransportConfig{MaxFrameSize: 1024, FailureDetectTimeout: 60 * time.Millisecond, AckMaxDelay: 10 * time.Millisecond, ControlRequestTimeout: time.Second, DialTimeout: time.Second, WS: config.WSConfig{HandshakeTimeout: time.Second}, BackpressureTimeout: time.Second}}
	c := transport.NewTripleTransportClientForURL(ts.URL, "token", &cfg.Transport, "ai", true)
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ws, _, e := c.DialWSSession(ctx, "sid", "token", 1, "bi")
	if e != nil {
		t.Fatal(e)
	}
	defer ws.Close()
	g := &generation{ctx: ctx, cancel: cancel, c: c, ws: ws, ep: 1, mode: "ws", stream: "stream", active: true, lastActivity: time.Now()}
	s := &ingressSession{m: &IngressManager{cfg: cfg}, ctx: ctx, cancel: cancel, g: g, sid: "sid", owner: "bi", changed: make(chan struct{}), faults: make(chan sessionFault, 1), ackNotify: make(chan struct{}, 1), limits: protocol.SessionLimits{MaxFrameSize: 1024}}
	done := make(chan struct{})
	go func() { defer close(done); s.receive(g) }()
	go s.control(g)
	go s.watchActivity(g)
	defer func() { cancel(); ws.Close(); <-done }()
	select {
	case f := <-s.faults:
		t.Fatalf("read deadline ignored declared remote backpressure after %d successful controls: %v", controls.Load(), f.err)
	case <-time.After(160 * time.Millisecond):
	}
}

func TestBoundaryCachedPrecheckInvalidatedAfterTokenChange(t *testing.T) {
	var calls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set(protocol.HeaderInstanceID, "bi")
		if r.Header.Get("Authorization") == "Bearer test-token" {
			w.WriteHeader(401)
			json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: protocol.ErrUnauthorized})
			return
		}
		up := websocket.Upgrader{Subprotocols: []string{protocol.SecWebSocketProtocol}}
		c, e := up.Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer c.Close()
		c.WriteJSON(protocol.WSHelloMessage{Type: "hello", ProtocolVersion: "1.4", NodeID: "B", InstanceID: "bi", Nonce: r.Header.Get(protocol.HeaderWSNonce), Transport: "ws"})
	}))
	defer ts.Close()
	m, p := testChooser(t, ts.URL)
	c := m.clients[p.ID]
	if err := m.precheck(context.Background(), p.ID, c, "B", "bi"); err == nil {
		t.Fatal("fixture expected initial rejection")
	}
	c.SetTokenProvider(func() string { return "rotated-token" })
	err := m.precheck(context.Background(), p.ID, c, "B", "bi")
	t.Logf("calls=%d error_after_rotation=%v", calls.Load(), err)
	if err != nil || calls.Load() != 2 {
		t.Fatal("new authentication snapshot still uses cached rejection for 30s")
	}
}

func TestBoundaryHeartbeatIndependentOfAckDelay(t *testing.T) {
	var calls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q protocol.AckRequest
		json.NewDecoder(r.Body).Decode(&q)
		calls.Add(1)
		json.NewEncoder(w).Encode(protocol.AckResponse{SessionID: "sid", InstanceID: "bi", Epoch: "1", StreamID: "stream", Transport: &protocol.TransportMode{Mode: "ws"}, AckUpOffset: "0", DownAckOffset: "0", ProbeID: q.ProbeID})
	}))
	defer ts.Close()
	cfg := &config.Config{Node: config.NodeConfig{ID: "A"}, Transport: config.TransportConfig{HeartbeatRaw: "20ms", FailureDetectTimeoutRaw: "90ms", AckMaxDelayRaw: "500ms"}}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	c := transport.NewTripleTransportClientForURL(ts.URL, "token", &cfg.Transport, "ai", true)
	defer c.Close()
	c.BindStream("sid", 1, "stream", "ws")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := &generation{ctx: ctx, cancel: cancel, c: c, ep: 1, mode: "ws", stream: "stream", active: true}
	s := &ingressSession{m: &IngressManager{cfg: cfg}, ctx: ctx, cancel: cancel, g: g, sid: "sid", owner: "bi", changed: make(chan struct{}), faults: make(chan sessionFault, 1), ackNotify: make(chan struct{}, 1)}
	done := make(chan struct{})
	go func() { defer close(done); s.control(g) }()
	defer func() { cancel(); <-done }()
	time.Sleep(120 * time.Millisecond)
	if calls.Load() < 2 {
		t.Fatalf("heartbeat=20ms, detect=90ms, ACK delay=500ms: only %d controls in 120ms", calls.Load())
	}
}

func TestBoundarySSERemoteBackpressureReadDoesNotExpireDetectTimer(t *testing.T) {
	closeServer := make(chan struct{})
	var controls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/sessions/sid/events" {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-closeServer
			return
		}
		var q protocol.AckRequest
		json.NewDecoder(r.Body).Decode(&q)
		controls.Add(1)
		json.NewEncoder(w).Encode(protocol.AckResponse{SessionID: "sid", InstanceID: "bi", Epoch: "1", StreamID: "stream", Transport: &protocol.TransportMode{Mode: "http-sse"}, AckUpOffset: "0", DownAckOffset: "0", ProbeID: q.ProbeID, DataTxBlocked: true, BackpressureReason: "data_channel_write"})
	}))
	defer ts.Close()
	defer close(closeServer)
	cfg := &config.Config{Resources: resource.NewManager(1<<20, 1<<20, 1<<20, 10), Transport: config.TransportConfig{MaxFrameSize: 1024, FailureDetectTimeout: 60 * time.Millisecond, AckMaxDelay: 10 * time.Millisecond, ControlRequestTimeout: time.Second, DialTimeout: time.Second, WS: config.WSConfig{HandshakeTimeout: time.Second}, BackpressureTimeout: time.Second}}
	c := transport.NewTripleTransportClientForURL(ts.URL, "token", &cfg.Transport, "ai", true)
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, e := c.SubscribeEvents(ctx, "sid", "token", "bi", 1, 1024)
	if e != nil {
		t.Fatal(e)
	}
	defer sub.Close()
	c.BindStream("sid", 1, "stream", "http-sse")
	g := &generation{ctx: ctx, cancel: cancel, c: c, sub: sub, ep: 1, mode: "http-sse", stream: "stream", active: true, lastActivity: time.Now()}
	s := &ingressSession{m: &IngressManager{cfg: cfg}, ctx: ctx, cancel: cancel, g: g, sid: "sid", owner: "bi", changed: make(chan struct{}), faults: make(chan sessionFault, 1), ackNotify: make(chan struct{}, 1), limits: protocol.SessionLimits{MaxFrameSize: 1024}}
	done := make(chan struct{})
	go func() { defer close(done); s.receive(g) }()
	go s.control(g)
	go s.watchActivity(g)
	defer func() { s.closed.Store(true); cancel(); sub.Close(); <-done }()
	select {
	case f := <-s.faults:
		t.Fatalf("read deadline ignored declared remote backpressure after %d successful controls: %v", controls.Load(), f.err)
	case <-time.After(160 * time.Millisecond):
		if controls.Load() < 2 {
			t.Fatal("control fixture made no progress")
		}
	}
}
