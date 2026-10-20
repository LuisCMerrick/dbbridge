package transport

import (
	"context"
	"crypto/x509"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWSSAfterHTTP2Probe(t *testing.T) {
	var mu sync.Mutex
	observed := ""
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/probe" {
			mu.Lock()
			observed = r.Proto
			mu.Unlock()
			var q protocol.ProbeRequest
			json.NewDecoder(r.Body).Decode(&q)
			json.NewEncoder(w).Encode(protocol.ProbeResponse{ProtocolVersion: "1.4", NodeID: "B", InstanceID: "bi", Nonce: q.Nonce, Transports: []string{"ws", "http-sse"}, AcceptingNewSessions: true})
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
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()
	cfg := &config.Config{Node: config.NodeConfig{ID: "A"}}
	if e := cfg.ApplyDefaultsAndValidate(); e != nil {
		t.Fatal(e)
	}
	c := NewTripleTransportClientForURL(ts.URL, "token", &cfg.Transport, "ai", true)
	defer c.Close()
	roots := x509.NewCertPool()
	roots.AddCert(ts.Certificate())
	c.tlsClientConfig.RootCAs = roots
	c.controlClient.Transport.(*http.Transport).TLSClientConfig.RootCAs = roots
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, e := c.Probe(ctx, "B", "nonce"); e != nil {
		t.Fatal(e)
	}
	mu.Lock()
	proto := observed
	mu.Unlock()
	if e := c.PrecheckWS(ctx, "B", "bi"); e != nil {
		t.Fatalf("authenticated probe succeeded via %s; WSS failed with shared ALPN %v: %v", proto, c.tlsClientConfig.NextProtos, e)
	}
}

func TestWSHelloAndReadyHonorCallerDeadline(t *testing.T) {
	for _, kind := range []string{"precheck", "session"} {
		t.Run(kind, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				up := websocket.Upgrader{Subprotocols: []string{protocol.SecWebSocketProtocol}}
				c, e := up.Upgrade(w, r, nil)
				if e != nil {
					return
				}
				defer c.Close()
				// Complete 101, withhold the application hello/ready until the caller closes.
				c.ReadMessage()
			}))
			defer ts.Close()
			cfg := &config.Config{Node: config.NodeConfig{ID: "A"}, Transport: config.TransportConfig{WS: config.WSConfig{HandshakeTimeoutRaw: "400ms"}}}
			if e := cfg.ApplyDefaultsAndValidate(); e != nil {
				t.Fatal(e)
			}
			c := NewTripleTransportClientForURL(ts.URL, "token", &cfg.Transport, "ai", true)
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			start := time.Now()
			if kind == "precheck" {
				c.PrecheckWS(ctx, "B", "bi")
			} else {
				c.DialWSSession(ctx, "sid", "token", 1, "bi")
			}
			if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
				t.Fatalf("%s waited %s despite caller deadline=40ms", kind, elapsed)
			}
		})
	}
}
