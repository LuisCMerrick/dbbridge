package ingress

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"dbbridge/internal/config"
)

// A short controlled 200-session close check, not a 24-hour resource test.
func TestAudit101811Acceptance200ClientClose(t *testing.T) {
	const n = 200
	e, _ := fix2Setup(t, "ws", false, func(c *config.Config) { c.Transport.Recovery.EnabledRaw = false }, func(c *config.Config) { c.Auth.InboundPeers[0].MaxSessions = n }, nil)
	if err := e.m.Start(); err != nil {
		t.Fatal(err)
	}
	clients := make([]net.Conn, n)
	defer func() {
		for _, c := range clients {
			if c != nil {
				c.Close()
			}
		}
	}()
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range clients {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
			if err != nil {
				errs <- err
				return
			}
			clients[i] = c
			_ = c.SetDeadline(time.Now().Add(30 * time.Second))
			if err = fixEcho2(t, c, "audit"); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	byRemote := map[string]*ingressSession{}
	e.m.activeConns.Range(func(_, v any) bool {
		if s := v.(*activeConnEntry).session; s != nil {
			byRemote[s.conn.RemoteAddr().String()] = s
		}
		return true
	})
	if len(byRemote) != n || len(e.reg.GetAllSessions()) != n {
		t.Fatalf("fixture: ingress=%d egress=%d", len(byRemote), len(e.reg.GetAllSessions()))
	}
	startAll := make(chan struct{})
	durations := make(chan time.Duration, n)
	for _, c := range clients {
		s := byRemote[c.LocalAddr().String()]
		if s == nil {
			t.Fatal("fixture: session binding missing")
		}
		wg.Add(1)
		go func(c net.Conn, s *ingressSession) {
			defer wg.Done()
			<-startAll
			s.mu.Lock()
			g := s.g
			s.mu.Unlock()
			start := time.Now()
			s.fault(g, fmt.Errorf("controlled transport fault: %w", context.DeadlineExceeded), false)
			_ = c.SetReadDeadline(start.Add(3 * time.Second))
			_, err := c.Read(make([]byte, 1))
			if err == nil {
				t.Error("client stayed readable")
			}
			durations <- time.Since(start)
		}(c, s)
	}
	close(startAll)
	wg.Wait()
	close(durations)
	var worst time.Duration
	for d := range durations {
		worst = max(worst, d)
	}
	t.Logf("200 actual loopback client TCP sockets; recovery=false; worst fault-to-observed-read-error=%v", worst)
	if worst > time.Second {
		t.Fatalf("200-session close exceeded 1s: %v", worst)
	}
}
