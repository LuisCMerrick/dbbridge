package resource

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestCanceledBudgetDoesNotAcquire(t *testing.T) {
	b := New(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Take(ctx, 1); err == nil || b.Used() != 0 {
		t.Fatalf("canceled acquisition: %v used=%d", err, b.Used())
	}
}

func TestListenerRejectsBeforeHeadersAndReleasesOnce(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	connections, metadata := New(1), New(32<<10)
	limited := LimitListener(ln, connections, metadata)
	accepted := make(chan net.Conn, 1)
	go func() { c, _ := limited.Accept(); accepted <- c }()
	first, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	var server net.Conn
	select {
	case server = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("first accept")
	}
	defer server.Close()
	done := make(chan struct{})
	go func() {
		if c, _ := limited.Accept(); c != nil {
			c.Close()
		}
		close(done)
	}()
	second, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	second.SetReadDeadline(time.Now().Add(time.Second))
	_, err = second.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("over-budget connection remained open")
	}
	if e, ok := err.(net.Error); ok && e.Timeout() {
		t.Fatal("admission waited for request headers")
	}
	server.Close()
	server.Close()
	if connections.Used() != 0 || metadata.Used() != 0 {
		t.Fatalf("leases: connections=%d metadata=%d", connections.Used(), metadata.Used())
	}
	ln.Close()
	<-done
}
