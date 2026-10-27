package egress

import (
	"context"
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

type sseSlowConn struct {
	net.Conn
	slow *atomic.Bool
}

func (c *sseSlowConn) Write(p []byte) (int, error) {
	if !c.slow.Load() {
		return c.Conn.Write(p)
	}
	total := 0
	for len(p) > 0 {
		n, err := c.Conn.Write(p[:min(len(p), 512)])
		total += n
		p = p[n:]
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
		time.Sleep(10 * time.Millisecond)
	}
	return total, nil
}

type sseSlowListener struct {
	net.Listener
	slow *atomic.Bool
}

func (l *sseSlowListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &sseSlowConn{c, l.slow}, nil
}
func TestSSEWriteIdleProgress(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "http1", true: "http2"}[h2], func(t *testing.T) {
			var slow atomic.Bool
			done := make(chan error, 1)
			event := "event: data\ndata: " + strings.Repeat("x", 32<<10) + "\n\n"
			ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if h2 && r.ProtoMajor != 2 {
					done <- io.ErrUnexpectedEOF
					return
				}
				slow.Store(true)
				w.Header().Set("Content-Type", "text/event-stream")
				var mu sync.Mutex
				done <- writeSSEProgress(context.Background(), w, http.NewResponseController(w), &mu, []byte(event), 80*time.Millisecond)
			}))
			ts.Listener = &sseSlowListener{ts.Listener, &slow}
			if h2 {
				ts.EnableHTTP2 = true
				ts.StartTLS()
			} else {
				ts.Start()
			}
			defer ts.Close()
			client := ts.Client()
			client.Timeout = 4 * time.Second
			resp, err := client.Get(ts.URL)
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || string(b) != event {
				t.Fatalf("received=%d expected=%d error=%v", len(b), len(event), err)
			}
			if err = <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
