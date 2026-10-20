package transport

import (
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// IdleWriteConn keeps progress timeouts below TLS/WebSocket framing. A partial
// socket write followed by an idle deadline is progress, not a failed message;
// finish its unwritten suffix without replaying any bytes. Explicit deadlines
// (handshake/control/absolute operation limits) always take precedence.
type IdleWriteConn struct {
	net.Conn
	mu       sync.Mutex
	idle     time.Duration
	deadline time.Time
}

func NewIdleWriteConn(c net.Conn) *IdleWriteConn { return &IdleWriteConn{Conn: c} }
func (c *IdleWriteConn) SetWriteIdle(d time.Duration) {
	c.mu.Lock()
	c.idle = d
	c.mu.Unlock()
}
func (c *IdleWriteConn) SetWriteDeadline(d time.Time) error {
	c.mu.Lock()
	c.deadline = d
	c.mu.Unlock()
	return c.Conn.SetWriteDeadline(d)
}
func (c *IdleWriteConn) SetDeadline(d time.Time) error {
	if err := c.SetReadDeadline(d); err != nil {
		return err
	}
	return c.SetWriteDeadline(d)
}
func (c *IdleWriteConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	idle, absolute := c.idle, c.deadline
	c.mu.Unlock()
	if idle <= 0 {
		return c.Conn.Write(p)
	}
	total := 0
	lastProgress := time.Now()
	// A net.Conn.Write can hide partial progress until its deadline fires.
	// Sample that progress so the idle decision is at most one quantum late,
	// rather than granting another full idle period after a partial timeout.
	quantum := min(idle/4, 250*time.Millisecond)
	if quantum <= 0 {
		quantum = idle
	}
	for len(p) > 0 {
		deadline := minTime(lastProgress.Add(idle), time.Now().Add(quantum))
		if !absolute.IsZero() && absolute.Before(deadline) {
			deadline = absolute
		}
		if err := c.Conn.SetWriteDeadline(deadline); err != nil {
			return total, err
		}
		n, err := c.Conn.Write(p[:min(len(p), 32<<10)])
		total += n
		p = p[n:]
		if n > 0 {
			lastProgress = time.Now()
		}
		if err != nil {
			var timeout net.Error
			if errors.As(err, &timeout) && timeout.Timeout() && time.Since(lastProgress) < idle && (absolute.IsZero() || time.Now().Before(absolute)) {
				continue
			}
			return total, err
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
	}
	return total, nil
}

type idleWriteListener struct{ net.Listener }

func IdleWriteListener(l net.Listener) net.Listener { return &idleWriteListener{l} }
func (l *idleWriteListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return NewIdleWriteConn(c), nil
}
func idleWriter(c net.Conn) *IdleWriteConn {
	if v, ok := c.(*IdleWriteConn); ok {
		return v
	}
	if v, ok := c.(*tls.Conn); ok {
		return idleWriter(v.NetConn())
	}
	return nil
}

// WebSocketResponseWriter supplies the same socket adapter to plain HTTP
// embedders. HTTPS embedders must wrap their listener with IdleWriteListener
// before TLS, as the native server does; partial TLS errors are not retryable.
type WebSocketResponseWriter struct{ http.ResponseWriter }

func (w WebSocketResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	c, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err != nil {
		return nil, nil, err
	}
	if idleWriter(c) == nil {
		if _, tlsSocket := c.(*tls.Conn); !tlsSocket {
			c = NewIdleWriteConn(c)
		}
	}
	return c, rw, nil
}

func WriteWSMessage(c *websocket.Conn, typ int, b []byte, idle time.Duration) error {
	if conn := idleWriter(c.UnderlyingConn()); conn != nil {
		conn.SetWriteIdle(idle)
		// IdleWriteConn supplies the rolling deadline. Operation cancellation
		// still closes the socket, independently of continuing data progress.
		if err := c.SetWriteDeadline(time.Time{}); err != nil {
			return err
		}
	} else if idle > 0 {
		if err := c.SetWriteDeadline(time.Now().Add(idle)); err != nil {
			return err
		}
	}
	return c.WriteMessage(typ, b)
}
