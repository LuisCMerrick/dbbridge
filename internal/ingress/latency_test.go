package ingress

import (
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
)

// delayProxy forwards TCP to target, delaying every byte by oneWay in each
// direction and the connection setup by one round trip.
type delayProxy struct {
	ln     net.Listener
	target string
	oneWay time.Duration
	mu     sync.Mutex
	last   net.Conn // the most recently accepted client connection
}

func (p *delayProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		p.last = c
		p.mu.Unlock()
		go func() {
			// connect: SYN/SYN-ACK costs one RTT before the client may send.
			time.Sleep(2 * p.oneWay)
			u, err := net.Dial("tcp", p.target)
			if err != nil {
				c.Close()
				return
			}
			go p.pipe(u, c)
			go p.pipe(c, u)
		}()
	}
}

func (p *delayProxy) pipe(dst, src net.Conn) {
	type chunk struct {
		at time.Time
		b  []byte
	}
	q := make(chan chunk, 1024)
	go func() {
		for ch := range q {
			if d := time.Until(ch.at); d > 0 {
				time.Sleep(d)
			}
			if ch.b == nil {
				if tc, ok := dst.(*net.TCPConn); ok {
					tc.CloseWrite()
				} else {
					dst.Close()
				}
				return
			}
			dst.Write(ch.b)
		}
	}()
	buf := make([]byte, 64<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			q <- chunk{time.Now().Add(p.oneWay), append([]byte(nil), buf[:n]...)}
		}
		if err != nil {
			q <- chunk{time.Now().Add(p.oneWay), nil}
			close(q)
			return
		}
	}
}

// stripImplicit hides protocol.HeaderImplicitReady, as an egress that
// predates it would answer.
type stripImplicit struct{ http.ResponseWriter }

func (w stripImplicit) WriteHeader(code int) {
	w.Header().Del(protocol.HeaderImplicitReady)
	w.ResponseWriter.WriteHeader(code)
}
func (w stripImplicit) Write(b []byte) (int, error) {
	w.Header().Del(protocol.HeaderImplicitReady)
	return w.ResponseWriter.Write(b)
}
func (w stripImplicit) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w stripImplicit) Flush()                      { w.ResponseWriter.(http.Flusher).Flush() }

// A short client-first exchange (an HTTP request through the tunnel) on a warm
// peer costs, in round trips to the egress: create, the stream (Upgrade on a
// spare connection, or the events GET) and the data itself. It used to add a
// probe, a TCP (and TLS) handshake, the ready ACK and the ping ACK.
func TestWarmSessionRoundTrips(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	const oneWay = 100 * time.Millisecond
	const rtt = 2 * oneWay
	cases := []struct {
		name, mode string
		legacy     bool    // egress without implicit ready
		maxRTT     float64 // bound for the third connection
	}{
		{"ws", "ws", false, 3.6},
		{"http-sse", "http-sse", false, 4.6},              // plain HTTP/1.1: the events GET needs its own TCP connection
		{"http-sse-legacy-egress", "http-sse", true, 6.6}, // + the ready ACK before data, no ping ACK
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var probes, creates atomic.Int64
			hook := func(w http.ResponseWriter, r *http.Request, base http.Handler) bool {
				if r.URL.Path == "/v1/peer/probe" {
					probes.Add(1)
				}
				if r.URL.Path == "/v1/sessions" {
					creates.Add(1)
				}
				if tc.legacy && strings.HasSuffix(r.URL.Path, "/events") {
					base.ServeHTTP(stripImplicit{w}, r)
					return true
				}
				return false
			}
			var px *delayProxy
			tweak := func(a *config.Config) {
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				px = &delayProxy{ln: ln, oneWay: oneWay}
				u, _ := url.Parse(a.Peers[0].Endpoints[0].URL)
				px.target = u.Host
				a.Peers[0].Endpoints[0].URL = "http://" + ln.Addr().String()
				go px.serve()
			}
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
						buf := make([]byte, 512)
						if _, err := c.Read(buf); err == nil {
							c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"))
						}
					}()
				}
			}()
			target := func(b *config.Config) {
				b.Targets = map[string]config.TargetConfig{"db": {Address: ln.Addr().String()}}
			}
			e, _ := dataplaneSetup(t, tc.mode, false, tweak, target, hook)
			if err := e.m.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { px.ln.Close() })
			// Let the start-up health probe land.
			deadline := time.Now().Add(5 * time.Second)
			for probes.Load() == 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			time.Sleep(rtt)
			var last time.Duration
			for i := 0; i < 3; i++ {
				start := time.Now()
				c, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
				if err != nil {
					t.Fatal(err)
				}
				c.SetDeadline(time.Now().Add(15 * time.Second))
				c.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n"))
				buf := make([]byte, 512)
				if _, err := io.ReadAtLeast(c, buf, 40); err != nil {
					t.Fatalf("connection %d: %v", i, err)
				}
				last = time.Since(start)
				c.Close()
				// Let the close handshake finish: over HTTP/1.1 its control
				// requests would otherwise make the next create open a
				// connection (h2 multiplexes them on one).
				time.Sleep(4 * rtt)
			}
			if got := float64(last) / float64(rtt); got > tc.maxRTT {
				t.Fatalf("warm connection took %v = %.2f RTT, want <= %.1f", last, got, tc.maxRTT)
			}
			if n := probes.Load(); n > 2 {
				t.Fatalf("%d probes for 3 connections: the health-loop answer was not reused", n)
			}
			if n := creates.Load(); n != 3 {
				t.Fatalf("%d creates, want 3", n)
			}
		})
	}
}

// A reused probe answer that names an instance the endpoint no longer runs
// (it restarted between health checks) costs one refused create: the session
// probes again and is created, instead of failing.
func TestReusedProbeInstanceMismatchRecovers(t *testing.T) {
	var creates, probes atomic.Int64
	var stale atomic.Bool
	hook := func(w http.ResponseWriter, r *http.Request, base http.Handler) bool {
		if r.URL.Path == "/v1/peer/probe" {
			probes.Add(1)
		}
		if r.URL.Path == "/v1/sessions" && r.Method == http.MethodPost {
			creates.Add(1)
			if stale.CompareAndSwap(true, false) {
				r.Header.Set(protocol.HeaderInstanceID, "previous-instance")
			}
		}
		return false
	}
	e, _ := dataplaneSetup(t, "ws", false, nil, nil, hook)
	if err := e.m.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for probes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	before := probes.Load()
	stale.Store(true)
	c, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "hello" {
		t.Fatalf("echo: %q %v", buf, err)
	}
	if n := creates.Load(); n != 2 {
		t.Fatalf("%d creates, want the refused one and one more", n)
	}
	if probes.Load() == before {
		t.Fatal("no fresh probe after the refused create")
	}
}

// A spare WS connection the egress side dropped while it waited is never
// used: the next session dials afresh and works.
func TestWSSpareDroppedByServer(t *testing.T) {
	var px *delayProxy
	tweak := func(a *config.Config) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		px = &delayProxy{ln: ln, oneWay: time.Millisecond}
		u, _ := url.Parse(a.Peers[0].Endpoints[0].URL)
		px.target = u.Host
		a.Peers[0].Endpoints[0].URL = "http://" + ln.Addr().String()
		go px.serve()
	}
	var upgrades atomic.Int64
	hook := func(w http.ResponseWriter, r *http.Request, base http.Handler) bool {
		if r.URL.Path == "/v1/transport/ws" && r.Header.Get(protocol.HeaderWSPurpose) == "session" {
			upgrades.Add(1)
		}
		return false
	}
	e, _ := dataplaneSetup(t, "ws", false, tweak, nil, hook)
	if err := e.m.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { px.ln.Close() })
	echo := func(i int) {
		c, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(10 * time.Second))
		c.Write([]byte("hello"))
		buf := make([]byte, 5)
		if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "hello" {
			t.Fatalf("connection %d: %q %v", i, buf, err)
		}
	}
	echo(0)
	// The spare opens right after the session's Upgrade; drop it from the
	// server side.
	time.Sleep(200 * time.Millisecond)
	px.mu.Lock()
	px.last.Close()
	px.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	echo(1)
	if n := upgrades.Load(); n != 2 {
		t.Fatalf("%d session upgrades, want 2", n)
	}
}
