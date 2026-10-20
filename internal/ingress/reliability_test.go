package ingress

import (
	"bytes"
	"context"
	"dbbridge/internal/config"
	"dbbridge/internal/resource"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestRecoveryDeadlineClosesBlockedLocalSocket(t *testing.T) {
	local, client := net.Pipe()
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &config.Config{Resources: resource.NewManager(1<<20, 1<<20, 1<<20, 10), Transport: config.TransportConfig{Recovery: config.RecoveryConfig{BarrierTimeout: time.Second}}}
	s := &ingressSession{m: &IngressManager{cfg: cfg}, conn: local, ctx: ctx, cancel: cancel, changed: make(chan struct{}), gate: make(chan struct{}, 1), grace: 50 * time.Millisecond}
	start := time.Now()
	if s.recover(fmt.Errorf("injected transport fault"), "auto") {
		t.Fatal("blocked write barrier recovered")
	}
	client.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("local socket stayed open")
	} else if n, ok := err.(net.Error); ok && n.Timeout() {
		t.Fatal("deadline did not close local socket")
	}
	if time.Since(start) > time.Second {
		t.Fatal("deadline decision blocked on write gate")
	}
}

func TestResourceLeasesReleasedOnClose(t *testing.T) {
	for _, mode := range []string{"ws", "http-sse"} {
		t.Run(mode, func(t *testing.T) {
			e := e2eIngress(t, mode, "")
			e.conn.SetDeadline(time.Now().Add(2 * time.Second))
			payload := bytes.Repeat([]byte{3, 8, 255}, 64000)
			done := make(chan error, 1)
			go func() { _, err := e.conn.Write(payload); done <- err }()
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(e.conn, got); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("roundtrip: %v", err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			e.stop()
			until := time.Now().Add(2 * time.Second)
			for {
				used := int64(0)
				for _, r := range []*config.Config{e.cfg, e.cfgB} {
					b := r.Resources
					used += b.Payload.Used() + b.Codec.Used() + b.Metadata.Used() + b.ControlMetadata.Used() + b.ProbeMetadata.Used() + b.Suspended.Used() + b.Sessions.Used() + b.Creates.Used() + b.Probes.Used() + b.Recovery.Used() + b.Cleanup.Used()
				}
				if used == 0 {
					break
				}
				if time.Now().After(until) {
					t.Fatalf("unreleased leases: %d", used)
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestCarrierAndPathRecovery(t *testing.T) {
	for _, kind := range []string{"ws_to_sse", "sse_to_ws", "drop_resume", "path_failover"} {
		t.Run(kind, func(t *testing.T) {
			e := e2eIngress(t, "auto", kind)
			e.conn.SetDeadline(time.Now().Add(6 * time.Second))
			e.conn.Write([]byte("before"))
			b := make([]byte, 6)
			if _, err := io.ReadFull(e.conn, b); err != nil {
				t.Fatal(err)
			}
			all := e.reg.GetAllSessions()
			if len(all) != 1 {
				t.Fatal("session count")
			}
			sess := all[0]
			oldConn := sess.BackendConn
			e.switched.Store(kind != "drop_resume")
			e.primaryDown.Store(true)
			if err := sess.SetStreamCancel(sess.GetEpoch(), sess.CurrentStreamID(), func() {}); err != nil {
				t.Fatal(err)
			}
			until := time.Now().Add(3 * time.Second)
			for sess.GetEpoch() == 1 && time.Now().Before(until) {
				time.Sleep(time.Millisecond)
			}
			time.Sleep(70 * time.Millisecond)
			e.conn.Write([]byte("after!"))
			if _, err := io.ReadFull(e.conn, b); err != nil {
				t.Fatal(err)
			}
			if string(b) != "after!" || sess.BackendConn != oldConn || e.backendDials.Load() != 1 || sess.GetEpoch() != 2 {
				t.Fatalf("recovery changed bytes/socket: %q dials=%d epoch=%d", b, e.backendDials.Load(), sess.GetEpoch())
			}
			if kind == "ws_to_sse" && sess.GetTransportMode() != "http-sse" || kind == "sse_to_ws" && sess.GetTransportMode() != "ws" {
				t.Fatal("carrier did not change")
			}
		})
	}
}
func TestHalfCloseBothOrders(t *testing.T) {
	for _, mode := range []string{"ws", "http-sse"} {
		for _, kind := range []string{"", "down_fin_first"} {
			t.Run(mode+"/"+kind, func(t *testing.T) {
				e := e2eIngress(t, mode, kind)
				e.conn.SetDeadline(time.Now().Add(3 * time.Second))
				if kind == "down_fin_first" {
					_, err := e.conn.Read(make([]byte, 1))
					if err != io.EOF {
						t.Fatalf("expected server half close: %v", err)
					}
				}
				data := bytes.Repeat([]byte("half-close"), 1000)
				if _, err := e.conn.Write(data); err != nil {
					t.Fatal(err)
				}
				if err := e.conn.(*net.TCPConn).CloseWrite(); err != nil {
					t.Fatal(err)
				}
				if kind == "" {
					got, err := io.ReadAll(e.conn)
					if err != nil || !bytes.Equal(got, data) {
						t.Fatalf("half-close truncated payload: bytes=%d err=%v", len(got), err)
					}
				} else {
					received := 0
					for received < len(data) {
						select {
						case b := <-e.received:
							received += len(b)
						case <-time.After(time.Second):
							t.Fatal("upstream lost after downstream FIN")
						}
					}
				}
				until := time.Now().Add(time.Second)
				for len(e.reg.GetAllSessions()) != 0 && time.Now().Before(until) {
					time.Sleep(time.Millisecond)
				}
				if len(e.reg.GetAllSessions()) != 0 {
					t.Fatal("normal completion leaked session")
				}
			})
		}
	}
}
func TestLargeBidirectionalByteStream(t *testing.T) {
	for _, mode := range []string{"ws", "http-sse"} {
		t.Run(mode, func(t *testing.T) {
			e := e2eIngress(t, mode, "large_stream")
			e.conn.SetDeadline(time.Now().Add(15 * time.Second))
			block := bytes.Repeat([]byte{0, 1, 255, 127, 0, 9, 31, 4}, 8192)
			const blocks = 800
			errCh := make(chan error, 1)
			go func() {
				for i := 0; i < blocks; i++ {
					if _, err := e.conn.Write(block); err != nil {
						errCh <- err
						return
					}
				}
				errCh <- nil
			}()
			buf := make([]byte, len(block))
			for i := 0; i < blocks; i++ {
				if _, err := io.ReadFull(e.conn, buf); err != nil {
					t.Fatalf("block %d: %v", i, err)
				}
				if !bytes.Equal(buf, block) {
					t.Fatalf("byte mismatch block %d", i)
				}
			}
			if err := <-errCh; err != nil {
				t.Fatal(err)
			}
			t.Logf("verified %d bytes in each direction", len(block)*blocks)
		})
	}
}
func TestTwoHundredSessionsClose(t *testing.T) {
	e := e2eIngress(t, "ws", "mass_close")
	clients := []net.Conn{e.conn}
	for i := 1; i < 200; i++ {
		c, err := net.Dial("tcp", e.manager.listeners[0].Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, c)
		defer c.Close()
	}
	until := time.Now().Add(20 * time.Second)
	for len(e.reg.GetAllSessions()) < 200 && time.Now().Before(until) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(e.reg.GetAllSessions()) != 200 {
		t.Fatalf("created %d sessions", len(e.reg.GetAllSessions()))
	}
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	start := time.Now()
	for i, c := range clients {
		wg.Add(1)
		go func(i int, c net.Conn) {
			defer wg.Done()
			c.SetReadDeadline(start.Add(time.Second))
			_, err := c.Read(make([]byte, 1))
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				errs <- fmt.Errorf("client %d exceeded 1s", i)
			}
		}(i, c)
	}
	e.manager.Close()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	t.Logf("200-client forced close observed in %s", time.Since(start))
}
