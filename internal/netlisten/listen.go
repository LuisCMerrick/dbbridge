// Package netlisten opens the TCP listeners dbbridge serves on.
package netlisten

import (
	"context"
	"net"
)

// TCP listens on addr with Multipath TCP explicitly disabled.
//
// Go 1.24+ makes net.Listen create MPTCP listeners by default on Linux.
// Kernel MPTCP has had remotely reachable bugs (see the Xray 26.10.31
// advisory), and dbbridge gains nothing from MPTCP, so every listener is a
// plain TCP socket regardless of the GODEBUG multipathtcp setting.
func TCP(addr string) (net.Listener, error) {
	var lc net.ListenConfig
	lc.SetMultipathTCP(false)
	return lc.Listen(context.Background(), "tcp", addr)
}
