package ingress

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"dbbridge/internal/config"
)

// The backend closing first (HTTP "Connection: close", a server that answers
// and hangs up) lets egress close the carrier while the ACK confirming the
// upstream FIN is still in flight. That race must stay a normal close: no
// recovery and no carrier cooldown, or every following new connection waits
// failover.cooldown (5s by default).
func TestServerCloseFirstDoesNotCoolCarrier(t *testing.T) {
	for _, mode := range []string{"ws", "http-sse"} {
		t.Run(mode, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { ln.Close() })
			go func() {
				for {
					c, err := ln.Accept()
					if err != nil {
						return
					}
					go func() {
						defer c.Close()
						buf := make([]byte, 64)
						n, _ := c.Read(buf)
						c.Write(append([]byte("re:"), buf[:n]...))
					}()
				}
			}()
			target := func(b *config.Config) {
				b.Targets = map[string]config.TargetConfig{"db": {Address: ln.Addr().String()}}
			}
			logs := captureLog(t)
			e, _ := dataplaneSetup(t, mode, false, nil, target, nil)
			if err := e.m.Start(); err != nil {
				t.Fatal(err)
			}
			const n = 30
			var worst time.Duration
			for i := 0; i < n; i++ {
				start := time.Now()
				c, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
				if err != nil {
					t.Fatal(err)
				}
				c.SetDeadline(time.Now().Add(15 * time.Second))
				if _, err := c.Write([]byte("ping")); err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(c)
				if err != nil || string(got) != "re:ping" {
					t.Fatalf("connection %d: %q %v", i, got, err)
				}
				c.Close()
				worst = max(worst, time.Since(start))
			}
			if worst > 2*time.Second {
				t.Fatalf("a connection waited %v after earlier server-first closes", worst)
			}
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) && strings.Count(logs.String(), "session terminated") < n {
				time.Sleep(20 * time.Millisecond)
			}
			if strings.Contains(logs.String(), "attempts=1") || strings.Contains(logs.String(), "session recovered") {
				t.Fatalf("a server-first normal close entered recovery:\n%s", logs.String())
			}
			ep := e.m.linkManagers["B"].GetEndpoints()[0]
			if !ep.TransportAvailable(time.Now(), mode) {
				t.Fatalf("server-first normal closes cooled the %s carrier down", mode)
			}
		})
	}
}
