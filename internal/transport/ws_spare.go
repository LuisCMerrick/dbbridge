package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"
)

// A WebSocket session needs a connection of its own (the Upgrade hijacks it),
// so every new session used to pay TCP and TLS handshakes before its Upgrade:
// two round trips to the egress on top of create, Upgrade and the first data.
// After each session dial the client therefore opens one spare connection
// (TCP, and TLS for wss) in the background; the next session dial within
// wsSpareMaxAge sends its Upgrade on it at once. A spare is never renewed
// when it expires unused, so an idle peer carries no extra traffic, and only
// a session dial both consumes and refills it.

// wsSpareMaxAge bounds how long a spare waits for a session. The egress
// closes a connection that has not authenticated within 5s of its accept
// (pre-auth guard), counted from before the TLS handshake; proxies in front
// of it may close sooner, which the liveness check and the one fresh retry
// absorb.
var wsSpareMaxAge = 4 * time.Second

type wsSpareState struct {
	conn    net.Conn
	addr    string
	expiry  *time.Timer
	dialing bool
}

// wsSpareUsable reports whether session dials to wsURL may use a spare: no
// HTTP proxy is in the path (a proxied dial is a CONNECT exchange, not a
// plain socket), and the peer is reached over TCP rather than a reverse
// tunnel.
func (c *TripleTransportClient) wsSpareUsable(wsURL string) bool {
	c.shared.mu.Lock()
	tunneled := c.shared.dialer != nil
	c.shared.mu.Unlock()
	if tunneled {
		// Reverse peers: a stream of the tunnel opens without a round trip.
		return false
	}
	if !c.tCfg.UseEnvironmentProxy {
		return true
	}
	u, err := url.Parse(wsURL)
	if err != nil {
		return false
	}
	probe := &http.Request{URL: &url.URL{Scheme: map[string]string{"wss": "https", "ws": "http"}[u.Scheme], Host: u.Host}}
	p, err := http.ProxyFromEnvironment(probe)
	return err == nil && p == nil && os.Getenv("ALL_PROXY") == "" && os.Getenv("all_proxy") == ""
}

// dialWSSocket opens a connection for a WS dial the way gorilla would:
// budgeted TCP dial wrapped for write-idle detection, then for wss the TLS
// handshake with http/1.1 ALPN and the host name verified.
func (c *TripleTransportClient) dialWSSocket(ctx context.Context, network, addr string, secure bool) (net.Conn, error) {
	raw, err := c.dial(ctx, network, addr, false, nil)
	if err != nil {
		return nil, err
	}
	conn := net.Conn(NewIdleWriteConn(raw))
	if !secure {
		return conn, nil
	}
	cfg := c.wsTLS()
	if cfg.ServerName == "" {
		host, _, e := net.SplitHostPort(addr)
		if e != nil {
			host = addr
		}
		cfg.ServerName = host
	}
	tc := tls.Client(conn, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		tc.Close()
		return nil, err
	}
	if !cfg.InsecureSkipVerify {
		if err := tc.VerifyHostname(cfg.ServerName); err != nil {
			tc.Close()
			return nil, err
		}
	}
	return tc, nil
}

// takeWSSpare returns a live spare connection to addr, or nil.
func (c *TripleTransportClient) takeWSSpare(addr string) net.Conn {
	c.mu.Lock()
	s := &c.wsSpare
	conn := s.conn
	if conn == nil || s.addr != addr {
		c.mu.Unlock()
		return nil
	}
	s.conn = nil
	if s.expiry != nil {
		s.expiry.Stop()
		s.expiry = nil
	}
	c.mu.Unlock()
	if !wsSpareAlive(conn) {
		conn.Close()
		return nil
	}
	return conn
}

// wsSpareAlive reports whether the server has not closed (or written to) a
// spare connection. A server sends nothing before the request apart from
// TLS post-handshake messages, which crypto/tls consumes inside Read.
func wsSpareAlive(conn net.Conn) bool {
	if err := conn.SetReadDeadline(time.Now().Add(time.Millisecond)); err != nil {
		return false
	}
	var b [1]byte
	n, err := conn.Read(b[:])
	if n > 0 {
		return false
	}
	var ne net.Error
	if err == nil || !errors.As(err, &ne) || !ne.Timeout() {
		return false
	}
	return conn.SetReadDeadline(time.Time{}) == nil
}

// refillWSSpare opens the next spare in the background unless one is
// present or being opened.
func (c *TripleTransportClient) refillWSSpare(addr string, secure bool) {
	c.mu.Lock()
	s := &c.wsSpare
	if s.dialing || s.conn != nil {
		c.mu.Unlock()
		return
	}
	s.dialing = true
	c.mu.Unlock()
	go func() {
		timeout := c.wsHandshakeTimeout()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		conn, err := c.dialWSSocket(ctx, "tcp", addr, secure)
		c.mu.Lock()
		s.dialing = false
		if err != nil || s.conn != nil || c.wsSpareClosed {
			c.mu.Unlock()
			if conn != nil {
				conn.Close()
			}
			return
		}
		s.conn, s.addr = conn, addr
		s.expiry = time.AfterFunc(wsSpareMaxAge, func() {
			c.mu.Lock()
			if s.conn == conn {
				s.conn, s.expiry = nil, nil
			}
			c.mu.Unlock()
			conn.Close()
		})
		c.mu.Unlock()
	}()
}

// dropWSSpare closes an idle spare (connection budget eviction or Close).
func (c *TripleTransportClient) dropWSSpare(final bool) {
	c.mu.Lock()
	s := &c.wsSpare
	conn := s.conn
	s.conn = nil
	if s.expiry != nil {
		s.expiry.Stop()
		s.expiry = nil
	}
	if final {
		c.wsSpareClosed = true
	}
	c.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
}
