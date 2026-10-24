package transport

import (
	"context"
	"dbbridge/internal/config"
	"errors"
	"net"
	"time"
)

// PeerDialer opens the byte stream that carries one HTTP or WebSocket
// connection of a peer client. A forward peer dials TCP to the endpoint's
// address; a reverse peer opens a stream on the tunnel the peer dialed in,
// where address only names the virtual endpoint. Connection budgets
// (per-peer and process-wide) are taken by the caller before DialPeer and
// apply to both kinds of dial alike.
type PeerDialer interface {
	DialPeer(ctx context.Context, network, address string) (net.Conn, error)
}

// TCPDialer is the forward-mode PeerDialer: a TCP connect bounded by
// Timeout (peer_dial_timeout) with TCP keepalive.
type TCPDialer struct {
	Timeout   time.Duration
	KeepAlive time.Duration
}

func (d TCPDialer) DialPeer(ctx context.Context, network, address string) (net.Conn, error) {
	nd := &net.Dialer{Timeout: d.Timeout, KeepAlive: d.KeepAlive}
	return nd.DialContext(ctx, network, address)
}

// ErrTunnelUnavailable reports that a reverse peer has no live tunnel to
// open a stream on. It fails the dial at once, like a refused TCP connect,
// so endpoint cooldown, failover and session recovery treat it as a carrier
// failure.
var ErrTunnelUnavailable = errors.New("reverse tunnel unavailable")

// StreamOpener opens one stream on a live tunnel.
type StreamOpener func(ctx context.Context) (net.Conn, error)

// TunnelDialer is the reverse-mode PeerDialer: each dial opens a stream on
// the peer's tunnel through Open, bounded by Timeout. A nil Open (no tunnel
// attached yet) fails with ErrTunnelUnavailable.
type TunnelDialer struct {
	Open    StreamOpener
	Timeout time.Duration
}

func (d TunnelDialer) DialPeer(ctx context.Context, _, _ string) (net.Conn, error) {
	if d.Open == nil {
		return nil, ErrTunnelUnavailable
	}
	if d.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.Timeout)
		defer cancel()
	}
	return d.Open(ctx)
}

// ReverseEndpointURL is the origin of the virtual endpoint a reverse peer's
// client sends its requests to. Requests run as plaintext HTTP inside the
// tunnel (the tunnel itself carries TLS), and the host is never resolved:
// the TunnelDialer ignores the address.
const ReverseEndpointURL = config.ReverseEndpointURL

// SetPeerDialer replaces how every endpoint client of this peer opens its
// connections. nil restores the TCP dial.
func (c *TripleTransportClient) SetPeerDialer(d PeerDialer) {
	c.shared.mu.Lock()
	c.shared.dialer = d
	c.shared.mu.Unlock()
}

// peerDialer returns the peer's dialer; forward peers use TCP bounded by
// peer_dial_timeout.
func (c *TripleTransportClient) peerDialer() PeerDialer {
	c.shared.mu.Lock()
	d := c.shared.dialer
	c.shared.mu.Unlock()
	if d != nil {
		return d
	}
	return TCPDialer{Timeout: c.tCfg.PeerDial(), KeepAlive: 30 * time.Second}
}
