package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"dbbridge/internal/config"
)

// Integration follow-up (ING cross-dep 10): cancelling the parent context
// aborts a WebSocket session handshake the server never answers, instead of
// waiting for the handshake timeout.
func TestIntegrationDialWSSessionAbortsOnCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var held []net.Conn
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c) // read nothing, answer nothing
			mu.Unlock()
		}
	}()
	defer func() {
		ln.Close()
		<-accepted
		mu.Lock()
		defer mu.Unlock()
		for _, c := range held {
			c.Close()
		}
	}()
	tc := &config.TransportConfig{}
	tc.WS.HandshakeTimeout = 10 * time.Second
	c := NewTripleTransportClientForURL("http://"+ln.Addr().String(), "tok", tc, "pi_test", false)
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(150*time.Millisecond, cancel)
	start := time.Now()
	_, _, err = c.DialWSSession(ctx, "sid", "stok", 1, "pi_peer")
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("handshake not aborted by cancel: %v", el)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}
