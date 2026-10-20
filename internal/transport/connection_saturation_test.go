package transport

import (
	"context"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestV2IdleConnectionCannotStarveAnotherEndpoint(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q protocol.ProbeRequest
		json.NewDecoder(r.Body).Decode(&q)
		json.NewEncoder(w).Encode(protocol.ProbeResponse{ProtocolVersion: "1.4", NodeID: "B", InstanceID: "bi", Nonce: q.Nonce, Transports: []string{"http-sse"}})
	})
	a, b := httptest.NewServer(h), httptest.NewServer(h)
	defer a.Close()
	defer b.Close()
	cfg := &config.Config{Node: config.NodeConfig{ID: "A"}}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	r := cfg.Resources
	r.ControlConnections = resource.New(1)
	c := NewTripleTransportClientForURL(a.URL, "token", &cfg.Transport, "ai", true)
	c.SetResources(r)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.Probe(ctx, "B", "first"); err != nil {
		t.Fatal(err)
	}
	if r.ControlConnections.Used() != 1 {
		t.Fatal("fixture did not retain the first keepalive")
	}
	if _, err := c.WithURL(b.URL).Probe(ctx, "B", "second"); err != nil {
		t.Fatalf("idle connection starved backup: %v", err)
	}
	c.Close()
	// Transport may still be handing its just-completed response to the idle
	// list; CloseIdleConnections also closes that connection on handoff.
	deadline := time.Now().Add(time.Second)
	for r.ControlConnections.Used() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if r.ControlConnections.Used() != 0 {
		t.Fatal("control connection lease leaked")
	}
}

func TestV2DataConnectionSaturationPreservesControl(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/probe" {
			var q protocol.ProbeRequest
			json.NewDecoder(r.Body).Decode(&q)
			json.NewEncoder(w).Encode(protocol.ProbeResponse{ProtocolVersion: "1.4", NodeID: "B", InstanceID: "bi", Nonce: q.Nonce, Transports: []string{"http-sse"}})
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("event: ping\ndata: {}\n\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer ts.Close()
	cfg := &config.Config{Node: config.NodeConfig{ID: "A"}}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	r := cfg.Resources
	r.DataConnections = resource.New(1)
	c := NewTripleTransportClientForURL(ts.URL, "token", &cfg.Transport, "ai", true)
	c.SetResources(r)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sub, err := c.SubscribeEvents(ctx, "s", "session", "bi", 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if r.DataConnections.Used() != 1 {
		t.Fatal("data budget not occupied")
	}
	if _, err = c.Probe(ctx, "B", "control"); err != nil {
		t.Fatalf("data saturation blocked control: %v", err)
	}
}
