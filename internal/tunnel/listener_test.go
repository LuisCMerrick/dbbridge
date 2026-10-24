package tunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestListenerDeliverAccept(t *testing.T) {
	l := NewListener("tun-1")
	if l.Addr().Network() != AddrNetwork || l.Addr().String() != "tun-1" {
		t.Fatalf("addr = %v/%v", l.Addr().Network(), l.Addr())
	}
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	errc := make(chan error, 1)
	go func() { errc <- l.Deliver(context.Background(), a) }()
	got, err := l.Accept()
	if err != nil || got != a {
		t.Fatalf("accept = %v, %v", got, err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestListenerCloseUnblocks(t *testing.T) {
	l := NewListener("tun")
	acc := make(chan error, 1)
	go func() { _, err := l.Accept(); acc <- err }()
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	del := make(chan error, 1)
	time.Sleep(10 * time.Millisecond)
	_ = l.Close()
	go func() { del <- l.Deliver(context.Background(), a) }()
	for _, ch := range []chan error{acc, del} {
		select {
		case err := <-ch:
			if !errors.Is(err, net.ErrClosed) {
				t.Fatalf("err = %v, want net.ErrClosed", err)
			}
		case <-time.After(time.Second):
			t.Fatal("blocked after Close")
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal("second Close failed")
	}
	select {
	case <-l.Done():
	default:
		t.Fatal("Done not closed")
	}
}

// A blocked Deliver gives up with its context and keeps the connection.
func TestListenerDeliverContext(t *testing.T) {
	l := NewListener("tun")
	defer l.Close()
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := l.Deliver(ctx, a); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

// Concurrent Deliver, Accept and Close never strand a connection: each
// delivered connection is either accepted or returned to its sender.
func TestListenerNoStrandedConns(t *testing.T) {
	for round := 0; round < 50; round++ {
		l := NewListener("tun")
		var wg sync.WaitGroup
		var mu sync.Mutex
		delivered, accepted := 0, 0
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				a, b := net.Pipe()
				defer b.Close()
				if l.Deliver(context.Background(), a) == nil {
					mu.Lock()
					delivered++
					mu.Unlock()
				} else {
					a.Close()
				}
			}()
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				c.Close()
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}()
		time.Sleep(time.Millisecond)
		l.Close()
		wg.Wait()
		<-done
		if delivered != accepted {
			t.Fatalf("round %d: delivered %d, accepted %d", round, delivered, accepted)
		}
	}
}

// The egress http.Server can serve the listener and shuts down cleanly.
func TestListenerServesHTTP(t *testing.T) {
	l := NewListener("tun")
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") })}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(l) }()
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		a, b := net.Pipe()
		if err := l.Deliver(ctx, b); err != nil {
			a.Close()
			b.Close()
			return nil, err
		}
		return a, nil
	}}}
	resp, err := client.Get("http://tunnel.invalid/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "ok" {
		t.Fatalf("body = %q", body)
	}
	client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("Serve = %v", err)
	}
}
