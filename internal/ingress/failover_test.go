package ingress

import (
	"io"
	"net"
	"net/url"
	"testing"
	"time"

	"dbbridge/internal/config"
)

// failoverSetup runs a peer with two endpoints, each behind its own
// delayProxy to the same egress; control_request_timeout stays at 10s so a
// fix that only shortened a timeout would not pass.
func failoverSetup(t *testing.T, mode string) (*testEnv, *delayProxy, *delayProxy) {
	t.Helper()
	const oneWay = 20 * time.Millisecond
	var p1, p2 *delayProxy
	tweak := func(a *config.Config) {
		u, _ := url.Parse(a.Peers[0].Endpoints[0].URL)
		mk := func() *delayProxy {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			p := &delayProxy{ln: ln, oneWay: oneWay, target: u.Host}
			go p.serve()
			t.Cleanup(func() { ln.Close() })
			return p
		}
		p1, p2 = mk(), mk()
		a.Peers[0].Endpoints = []config.EndpointConfig{
			{ID: "e1", URL: "http://" + p1.ln.Addr().String(), Priority: 1},
			{ID: "e2", URL: "http://" + p2.ln.Addr().String(), Priority: 2},
		}
		a.Transport.ControlRequestTimeoutRaw = "10s"
	}
	e, _ := dataplaneSetup(t, mode, false, tweak, nil, nil)
	if err := e.m.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for e.probes.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	return e, p1, p2
}

func echoOnce(t *testing.T, c net.Conn, msg string) time.Duration {
	// msg names the step in a failure.
	t.Helper()
	start := time.Now()
	c.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != msg {
		t.Fatalf("echo %q after %v: got %q, %v", msg, time.Since(start), buf, err)
	}
	return time.Since(start)
}

// When the primary path starts dropping everything silently, a new session
// built on the cached health-loop answer must not wait out
// control_request_timeout on the mute connection: its guard probe gives up
// after guardTimeout (1s floor here) and the session moves to the backup.
func TestNewSessionFailsOverFromBlackholedPrimary(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	for _, mode := range []string{"http-sse", "auto"} {
		t.Run(mode, func(t *testing.T) {
			e, p1, _ := failoverSetup(t, mode)
			addr := e.m.listeners[0].Addr().String()
			warm, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			echoOnce(t, warm, "warm")
			warm.Close()
			time.Sleep(200 * time.Millisecond)
			p1.dead.Store(true)
			c, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if d := echoOnce(t, c, "hello"); d > 3*time.Second {
				t.Fatalf("first session after the primary went mute took %v", d)
			}
			c2, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer c2.Close()
			if d := echoOnce(t, c2, "again"); d > time.Second {
				t.Fatalf("second session took %v: the mute primary was tried again", d)
			}
		})
	}
}

// An established session whose primary path goes mute is moved by recovery
// within a few seconds: the path watch notices the request waiting for its
// answer, the check finds the endpoint mute and closes its stalled
// connections, instead of the session waiting for control_request_timeout
// or the HTTP/2 PING timeout.
func TestEstablishedSessionLeavesBlackholedPrimary(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	e, p1, _ := failoverSetup(t, "http-sse")
	c, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	echoOnce(t, c, "before")
	time.Sleep(100 * time.Millisecond)
	p1.dead.Store(true)
	if d := echoOnce(t, c, "after"); d > 5*time.Second {
		t.Fatalf("echo after the primary went mute took %v", d)
	}
	if d := echoOnce(t, c, "steady"); d > time.Second {
		t.Fatalf("echo on the recovered session took %v", d)
	}
}
