package resource

import (
	"net"
	"sync"
)

// LeaseConn releases admission exactly once, including TLS, HTTP keepalive and
// hijacked WebSocket connections. It never relies on handler completion.
func LeaseConn(conn net.Conn, release func()) net.Conn {
	return &leasedConn{Conn: conn, release: release}
}

type leasedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *leasedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

type limitedListener struct {
	net.Listener
	connections, metadata *Budget
}

// LimitListener bounds sockets before HTTP headers/TLS/application admission.
// Saturation rejects the accepted connection rather than accumulating waiters.
func LimitListener(ln net.Listener, connections, metadata *Budget) net.Listener {
	return &limitedListener{Listener: ln, connections: connections, metadata: metadata}
}

func (l *limitedListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if !l.connections.Try(1) {
			_ = conn.Close()
			continue
		}
		if !l.metadata.Try(ConnMetadataPerConn) {
			l.connections.Put(1)
			_ = conn.Close()
			continue
		}
		return LeaseConn(conn, func() { l.metadata.Put(ConnMetadataPerConn); l.connections.Put(1) }), nil
	}
}
