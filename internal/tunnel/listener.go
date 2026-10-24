// Package tunnel implements reverse mode: the database-side node dials a
// long-lived tunnel to the public-side node, and DBBridge traffic runs as
// multiplexed streams inside it.
package tunnel

import (
	"context"
	"net"
	"sync"
)

// AddrNetwork is the Network() of tunnel addresses.
const AddrNetwork = "dbbridge-tunnel"

// Addr names a tunnel (or the listener that serves its streams).
type Addr string

func (a Addr) Network() string { return AddrNetwork }
func (a Addr) String() string  { return string(a) }

// Listener is a net.Listener whose connections are streams delivered from
// tunnels rather than accepted from a socket. The egress HTTP server serves
// it beside (or instead of) its TCP listener.
//
// Deliver hands a connection directly to a waiting Accept (no queue), so a
// connection is never stranded inside a closed listener: after Close, Deliver
// returns net.ErrClosed and the caller still owns, and must close, the
// connection.
type Listener struct {
	addr  Addr
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

// NewListener returns an open listener whose Addr is id.
func NewListener(id string) *Listener {
	return &Listener{addr: Addr(id), conns: make(chan net.Conn), done: make(chan struct{})}
}

// Accept waits for the next delivered connection. After Close it returns
// net.ErrClosed, which http.Server.Serve treats as a normal shutdown.
func (l *Listener) Accept() (net.Conn, error) {
	select {
	case <-l.done:
		return nil, net.ErrClosed
	default:
	}
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

// Deliver passes c to Accept. It blocks until a caller of Accept takes it,
// ctx ends (ctx.Err()) or the listener is closed (net.ErrClosed). On error
// the connection was not taken and remains the caller's.
func (l *Listener) Deliver(ctx context.Context, c net.Conn) error {
	select {
	case <-l.done:
		return net.ErrClosed
	default:
	}
	select {
	case l.conns <- c:
		return nil
	case <-l.done:
		return net.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops Accept and Deliver. It is idempotent and safe to call
// concurrently with both. Connections already accepted are not affected.
func (l *Listener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

// Done is closed when the listener is closed.
func (l *Listener) Done() <-chan struct{} { return l.done }

// Addr returns the tunnel identifier the listener was created with.
func (l *Listener) Addr() net.Addr { return l.addr }

var _ net.Listener = (*Listener)(nil)
