package egress

import (
	"context"
	"crypto/tls"
	"dbbridge/internal/observability"
	"dbbridge/internal/transport"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// defaultPreAuthTimeout bounds how long a connection that has not yet carried
// a peer-authenticated request may sit in TLS handshake, header read or idle.
const defaultPreAuthTimeout = 5 * time.Second

// connGuard keeps unauthenticated sockets from holding the inbound connection
// budget. At most half of the InboundConnections slots can be
// unauthenticated at once, so connections that already carried an
// authenticated request always keep room, and each remote IP may hold only a
// fraction of the unauthenticated half. Unauthenticated sockets also get a
// short read deadline instead of ReadHeaderTimeout/IdleTimeout.
type connGuard struct {
	mu        sync.Mutex
	unauth    int
	perIP     map[string]int
	maxUnauth int
	maxPerIP  int
	preAuth   time.Duration
}

func newConnGuard(total int64, preAuth time.Duration) *connGuard {
	if preAuth <= 0 {
		preAuth = defaultPreAuthTimeout
	}
	maxUnauth := int(max(1, total/2))
	return &connGuard{perIP: map[string]int{}, maxUnauth: maxUnauth, maxPerIP: max(8, maxUnauth/4), preAuth: preAuth}
}

func (g *connGuard) admit(ip string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.unauth >= g.maxUnauth || g.perIP[ip] >= g.maxPerIP {
		return false
	}
	g.unauth++
	g.perIP[ip]++
	return true
}

func (g *connGuard) release(ip string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.unauth--
	if g.perIP[ip]--; g.perIP[ip] <= 0 {
		delete(g.perIP, ip)
	}
}

type guardListener struct {
	net.Listener
	g *connGuard
}

func (l *guardListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		ip := remoteIP(c.RemoteAddr())
		if !l.g.admit(ip) {
			atomic.AddInt64(&observability.DefaultMetrics.AdmissionRejected, 1)
			_ = c.Close()
			continue
		}
		gc := &guardConn{Conn: c, g: l.g, ip: ip}
		_ = gc.SetReadDeadline(time.Time{})
		return gc, nil
	}
}

func remoteIP(a net.Addr) string {
	if a == nil {
		return ""
	}
	if host, _, err := net.SplitHostPort(a.String()); err == nil {
		return host
	}
	return a.String()
}

// guardConn counts as unauthenticated until markAuthenticated. While it is,
// every read deadline is clamped to now+preAuth.
type guardConn struct {
	net.Conn
	g        *connGuard
	ip       string
	authed   atomic.Bool
	released atomic.Bool
}

func (c *guardConn) releaseUnauth() {
	if c.released.CompareAndSwap(false, true) {
		c.g.release(c.ip)
	}
}

func (c *guardConn) markAuthenticated() {
	if c.authed.CompareAndSwap(false, true) {
		c.releaseUnauth()
		// Lift the pre-auth clamp; net/http (or the handler) sets the
		// request deadline that applies from here on.
		_ = c.Conn.SetReadDeadline(time.Time{})
	}
}

func (c *guardConn) SetReadDeadline(t time.Time) error {
	if !c.authed.Load() {
		if limit := time.Now().Add(c.g.preAuth); t.IsZero() || t.After(limit) {
			t = limit
		}
	}
	return c.Conn.SetReadDeadline(t)
}

func (c *guardConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.Conn.SetWriteDeadline(t)
}

func (c *guardConn) Close() error {
	err := c.Conn.Close()
	c.releaseUnauth()
	return err
}

type guardKey struct{}

// findGuard unwraps the connection net/http hands to ConnContext: TLS over
// IdleWriteConn over guardConn.
func findGuard(c net.Conn) *guardConn {
	for i := 0; i < 4 && c != nil; i++ {
		switch v := c.(type) {
		case *guardConn:
			return v
		case *tls.Conn:
			c = v.NetConn()
		case *transport.IdleWriteConn:
			c = v.Conn
		default:
			return nil
		}
	}
	return nil
}

func guardConnContext(ctx context.Context, c net.Conn) context.Context {
	if g := findGuard(c); g != nil {
		return context.WithValue(ctx, guardKey{}, g)
	}
	return ctx
}

func markConnAuthenticated(ctx context.Context) {
	if g, ok := ctx.Value(guardKey{}).(*guardConn); ok {
		g.markAuthenticated()
	}
}
