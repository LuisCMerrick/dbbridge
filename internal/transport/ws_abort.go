package transport

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
)

// dialWSAbortable runs the WebSocket handshake so that cancelling ctx aborts
// it at once. gorilla's DialContext only applies ctx's deadline to the socket,
// so a cancelled attach would otherwise wait for the server's answer or the
// handshake timeout. Every socket the dialer opens is closed when ctx ends
// before the handshake returns.
func (c *TripleTransportClient) dialWSAbortable(ctx context.Context, wsURL string, header http.Header) (*websocket.Conn, *http.Response, error) {
	return c.dialWSAbortableSpare(ctx, wsURL, header, false)
}

// dialWSAbortableSpare is dialWSAbortable; with spare set (session dials) it
// sends the Upgrade on a pre-opened spare connection when one is ready and
// opens the next spare afterwards (see ws_spare.go). A spare that fails before
// any response is answered by one fresh dial, so a spare the server or a
// proxy dropped never fails the session.
func (c *TripleTransportClient) dialWSAbortableSpare(ctx context.Context, wsURL string, header http.Header, spare bool) (*websocket.Conn, *http.Response, error) {
	header = header.Clone()
	if header == nil {
		header = http.Header{}
	}
	SetUserAgent(header, c.tCfg)
	spare = spare && c.wsSpareUsable(wsURL)
	secure := strings.HasPrefix(wsURL, "wss://")
	for attempt := 0; ; attempt++ {
		usedSpare := false
		var spareAddr string
		dialer := c.newWSDialer()
		var mu sync.Mutex
		var stops []func() bool
		inner := dialer.NetDialContext
		if spare {
			inner = func(dctx context.Context, network, addr string) (net.Conn, error) {
				spareAddr = addr
				if attempt == 0 {
					if conn := c.takeWSSpare(addr); conn != nil {
						usedSpare = true
						return conn, nil
					}
				}
				return c.dialWSSocket(dctx, network, addr, secure)
			}
		}
		wrapped := func(dctx context.Context, network, addr string) (net.Conn, error) {
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
		if spare && secure {
			// The TLS handshake is part of the spare: gorilla must not
			// run a second one.
			dialer.NetDialTLSContext = wrapped
		} else {
			dialer.NetDialContext = wrapped
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
		if spare && spareAddr != "" {
			c.refillWSSpare(spareAddr, secure)
		}
		if err == nil && fired {
			// Cancelled right after the handshake finished: the socket is gone.
			_ = closeWSHard(conn)
			return nil, nil, fmt.Errorf("ws handshake: %w", ctx.Err())
		}
		if err != nil && resp == nil && fired {
			return nil, nil, fmt.Errorf("ws handshake aborted: %w (%v)", ctx.Err(), err)
		}
		if err != nil && resp == nil && usedSpare && ctx.Err() == nil {
			continue
		}
		return conn, resp, err
	}
}
