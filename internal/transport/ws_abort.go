package transport

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// dialWSAbortable runs the WebSocket handshake so that cancelling ctx aborts
// it at once. gorilla's DialContext only applies ctx's deadline to the socket,
// so a cancelled attach would otherwise wait for the server's answer or the
// handshake timeout. Every socket the dialer opens is closed when ctx ends
// before the handshake returns.
func (c *TripleTransportClient) dialWSAbortable(ctx context.Context, wsURL string, header http.Header) (*websocket.Conn, *http.Response, error) {
	dialer := c.newWSDialer()
	var mu sync.Mutex
	var stops []func() bool
	inner := dialer.NetDialContext
	dialer.NetDialContext = func(dctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := inner(dctx, network, addr)
		if err != nil {
			return nil, err
		}
		stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
		mu.Lock()
		stops = append(stops, stop)
		mu.Unlock()
		return conn, nil
	}
	conn, resp, err := dialer.DialContext(ctx, wsURL, header)
	fired := false
	mu.Lock()
	for _, stop := range stops {
		if !stop() {
			fired = true
		}
	}
	mu.Unlock()
	if err == nil && fired {
		// Cancelled right after the handshake finished: the socket is gone.
		_ = closeWSHard(conn)
		return nil, nil, fmt.Errorf("ws handshake: %w", ctx.Err())
	}
	if err != nil && resp == nil && fired {
		return nil, nil, fmt.Errorf("ws handshake aborted: %w (%v)", ctx.Err(), err)
	}
	return conn, resp, err
}
