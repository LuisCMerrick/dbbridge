package transport

import (
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Path watch: a peer path that starts dropping packets silently (a firewall
// rule, a dead link) leaves every pooled connection of the endpoint open
// but mute. Requests on them only fail after their own timeouts
// (control_request_timeout, up_request_timeout) and HTTP/2 PING notices after
// heartbeat + 5 s. The watch shortens that: a request without response
// headers after the watch delay reports the endpoint to the slow hook, which
// checks the path (a probe) and, when it is dead, calls CloseStalled so all
// requests on the mute connections fail now and recovery moves them.

// trackedConn records when the connection last received bytes.
type trackedConn struct {
	net.Conn
	owner    *TripleTransportClient
	lastRead atomic.Int64 // unix nanos
	once     sync.Once
}

func (t *trackedConn) Read(b []byte) (int, error) {
	n, err := t.Conn.Read(b)
	if n > 0 {
		t.lastRead.Store(time.Now().UnixNano())
	}
	return n, err
}

func (t *trackedConn) Close() error {
	t.once.Do(func() { t.owner.conns.Delete(t) })
	return t.Conn.Close()
}

func (c *TripleTransportClient) track(conn net.Conn) net.Conn {
	t := &trackedConn{Conn: conn, owner: c}
	t.lastRead.Store(time.Now().UnixNano())
	c.conns.Store(t, struct{}{})
	return t
}

// CloseStalled closes this endpoint's connections that have received nothing
// since since and reports how many it closed. Requests in flight on them
// fail at once; connections that delivered bytes meanwhile are kept.
func (c *TripleTransportClient) CloseStalled(since time.Time) int {
	n := 0
	cut := since.UnixNano()
	c.conns.Range(func(k, _ any) bool {
		t := k.(*trackedConn)
		if t.lastRead.Load() < cut {
			t.Close()
			n++
		}
		return true
	})
	return n
}

// PathWatch is installed by the ingress: Delay returns how long a request
// may wait for response headers before Slow is called for its endpoint.
// Slow must return quickly (it starts its own check).
type PathWatch struct {
	Delay func(c *TripleTransportClient) time.Duration
	Slow  func(c *TripleTransportClient)
}

// SetPathWatch installs w for every endpoint of this peer.
func (c *TripleTransportClient) SetPathWatch(w *PathWatch) {
	c.shared.mu.Lock()
	c.shared.watch = w
	c.shared.mu.Unlock()
}

func (c *TripleTransportClient) pathWatch() *PathWatch {
	c.shared.mu.Lock()
	defer c.shared.mu.Unlock()
	return c.shared.watch
}

// do sends r on client and reports the endpoint to the path watch when the
// response headers take longer than the watch delay.
func (c *TripleTransportClient) do(client *http.Client, r *http.Request) (*http.Response, error) {
	pw := c.pathWatch()
	if pw == nil || pw.Slow == nil || pw.Delay == nil {
		return client.Do(r)
	}
	d := pw.Delay(c)
	if d <= 0 {
		return client.Do(r)
	}
	t := time.AfterFunc(d, func() { pw.Slow(c) })
	defer t.Stop()
	return client.Do(r)
}
