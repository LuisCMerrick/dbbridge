package transport

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/config"
)

// A request that waits longer than the watch delay for its response headers
// reports its endpoint; CloseStalled then fails it at once, while a
// connection that delivered bytes since the cut-off is kept.
func TestPathWatchAndCloseStalled(t *testing.T) {
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
		w.Write([]byte("ok"))
	}))
	defer ts.Close()
	defer close(release)
	c := NewTripleTransportClientForURL(ts.URL, "token", &config.TransportConfig{ControlRequestTimeout: 10 * time.Second}, "instance", true)
	defer c.Close()
	var slow atomic.Int64
	reported := make(chan *TripleTransportClient, 4)
	c.SetPathWatch(&PathWatch{
		Delay: func(*TripleTransportClient) time.Duration { return 50 * time.Millisecond },
		Slow:  func(e *TripleTransportClient) { slow.Add(1); reported <- e },
	})
	get := func(path string) error {
		// POST: an idempotent GET would be retried on a fresh connection.
		r, _ := http.NewRequestWithContext(context.Background(), "POST", ts.URL+path, nil)
		resp, err := c.do(c.controlClient, r)
		if err == nil {
			resp.Body.Close()
		}
		return err
	}
	if err := get("/fast"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if slow.Load() != 0 {
		t.Fatal("a fast request was reported")
	}
	done := make(chan error, 1)
	go func() { done <- get("/slow") }()
	var e *TripleTransportClient
	select {
	case e = <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("slow request not reported")
	}
	if e != c {
		t.Fatal("reported another endpoint")
	}
	// A second connection that keeps receiving bytes survives.
	busy, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tb := c.track(busy).(*trackedConn)
	defer tb.Close()
	cut := time.Now()
	tb.lastRead.Store(time.Now().Add(time.Millisecond).UnixNano())
	start := time.Now()
	if n := c.CloseStalled(cut); n != 1 {
		t.Fatalf("closed %d connections, want the stalled one", n)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("request on the closed connection succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request on the closed connection still waiting")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("stalled request failed after %v", d)
	}
	if _, loaded := c.conns.Load(tb); !loaded {
		t.Fatal("busy connection was closed")
	}
}
