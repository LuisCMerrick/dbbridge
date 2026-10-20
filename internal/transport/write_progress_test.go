package transport

import (
	"context"
	"errors"
	"github.com/gorilla/websocket"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type testProgressConn struct {
	net.Conn
	slow     *atomic.Bool
	progress *atomic.Int64
}

func (c *testProgressConn) Write(p []byte) (int, error) {
	if !c.slow.Load() {
		return c.Conn.Write(p)
	}
	total := 0
	for len(p) > 0 {
		n := min(512, len(p))
		k, e := c.Conn.Write(p[:n])
		total += k
		p = p[k:]
		if k > 0 {
			c.progress.Add(1)
		}
		if e != nil {
			return total, e
		}
		if k == 0 {
			return total, errors.New("no progress")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return total, nil
}
func TestWSWriteIdleTracksProgress(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u := websocket.Upgrader{}
		c, e := u.Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer c.Close()
		c.ReadMessage()
	}))
	defer ts.Close()
	var slow atomic.Bool
	var progress atomic.Int64
	d := websocket.Dialer{NetDialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		c, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		return NewIdleWriteConn(&testProgressConn{Conn: c, slow: &slow, progress: &progress}), nil
	}}
	c, _, err := d.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	slow.Store(true)
	writer := &WSSessionConn{conn: c, writeIdle: 80 * time.Millisecond}
	started := time.Now()
	err = writer.WriteBinaryFrame(1, 0, make([]byte, 32<<10))
	t.Logf("configured idle=80ms; progress every 10ms; completed progress writes=%d elapsed=%s error=%v", progress.Load(), time.Since(started), err)
	if err != nil {
		t.Fatal("continuous write progress was treated as an idle timeout")
	}
}
