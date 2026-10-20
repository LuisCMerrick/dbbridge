package ingress

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/resource"
	"dbbridge/internal/transport"
)

// The parser and liveness watchdog of an active SSE generation: when only
// proxy-style comments arrive, the session faults within the detection time.
func TestSSECommentsDoNotRenewLiveness(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-tick.C:
				_, _ = fmt.Fprint(w, ": gateway keepalive\n\n")
				w.(http.Flusher).Flush()
			}
		}
	}))
	defer ts.Close()
	cfg := &config.Config{Transport: config.TransportConfig{ControlRequestTimeout: time.Second, FailureDetectTimeout: 100 * time.Millisecond, BackpressureTimeout: time.Second}, Resources: resource.NewManager(8<<20, 8<<20, 8<<20, 1)}
	c := transport.NewTripleTransportClientForURL(ts.URL, "token", &cfg.Transport, "ingress", true)
	c.SetResources(cfg.Resources)
	defer c.Close()
	sub, err := c.SubscribeEvents(ctx, "sid", "session-token", "owner", 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	g := &generation{ctx: ctx, cancel: cancel, c: c, sub: sub, mode: "http-sse", stream: "stream", ep: 1, active: true, lastActivity: time.Now()}
	s := &ingressSession{m: &IngressManager{cfg: cfg}, ctx: ctx, cancel: cancel, g: g, faults: make(chan sessionFault, 16), sid: "sid", owner: "owner", lastControl: time.Now()}
	readDone := make(chan struct{})
	go func() { defer close(readDone); _, _ = s.readWire(g, time.Time{}) }()
	watchDone := make(chan struct{})
	go func() { defer close(watchDone); s.watchActivity(g) }()
	defer func() { cancel(); sub.Close(); <-readDone; <-watchDone }()
	select {
	case <-s.faults:
	case <-time.After(450 * time.Millisecond):
		g.mu.Lock()
		age := time.Since(g.lastActivity)
		g.mu.Unlock()
		t.Errorf("no bound ping/data/control event for 4.5 detection intervals, but comment bytes renewed lastActivity (age=%v)", age)
	}
}
