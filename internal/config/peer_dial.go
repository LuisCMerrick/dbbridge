package config

import "time"

// PeerDial returns the timeout for dialing a peer endpoint (TCP connect plus
// the connection-budget wait): transport.peer_dial_timeout, falling back to
// transport.dial_timeout when it is unset.
func (t *TransportConfig) PeerDial() time.Duration {
	if t.PeerDialTimeout > 0 {
		return t.PeerDialTimeout
	}
	return t.DialTimeout
}
