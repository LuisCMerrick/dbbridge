package netlisten

import (
	"net"
	"testing"
)

func TestTCPListenerIsNotMultipath(t *testing.T) {
	ln, err := TCP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			done <- nil
			return
		}
		done <- c
	}()
	cc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	sc := <-done
	if sc == nil {
		t.Fatal("accept failed")
	}
	defer sc.Close()
	if mp, err := sc.(*net.TCPConn).MultipathTCP(); err == nil && mp {
		t.Fatal("accepted connection uses MPTCP")
	}
}
