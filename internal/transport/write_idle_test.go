package transport

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"github.com/gorilla/websocket"
)

func TestWriteIdleStallAndAbsoluteDeadline(t *testing.T) {
	t.Run("no_progress", func(t *testing.T) {
		local, peer := net.Pipe()
		defer peer.Close()
		defer local.Close()
		c := NewIdleWriteConn(local)
		c.SetWriteIdle(30 * time.Millisecond)
		started := time.Now()
		n, err := c.Write([]byte("blocked"))
		var timeout net.Error
		if n != 0 || !errors.As(err, &timeout) || !timeout.Timeout() || time.Since(started) > time.Second {
			t.Fatalf("n=%d error=%v elapsed=%s", n, err, time.Since(started))
		}
	})
	t.Run("initial_progress_then_stall", func(t *testing.T) {
		local, peer := net.Pipe()
		defer local.Close()
		defer peer.Close()
		go func() { _, _ = peer.Read(make([]byte, 512)) }()
		c := NewIdleWriteConn(local)
		c.SetWriteIdle(120 * time.Millisecond)
		started := time.Now()
		n, err := c.Write(make([]byte, 32<<10))
		var timeout net.Error
		if n != 512 || !errors.As(err, &timeout) || !timeout.Timeout() || time.Since(started) > 210*time.Millisecond {
			t.Fatalf("partial progress extended idle by a full extra interval: n=%d error=%v elapsed=%s", n, err, time.Since(started))
		}
	})
	t.Run("absolute_deadline_with_progress", func(t *testing.T) {
		local, peer := net.Pipe()
		defer local.Close()
		defer peer.Close()
		go func() {
			buf := make([]byte, 512)
			for {
				if _, err := peer.Read(buf); err != nil {
					return
				}
			}
		}()
		var slow atomic.Bool
		var progress atomic.Int64
		slow.Store(true)
		c := NewIdleWriteConn(&testProgressConn{Conn: local, slow: &slow, progress: &progress})
		c.SetWriteIdle(200 * time.Millisecond)
		c.SetWriteDeadline(time.Now().Add(45 * time.Millisecond))
		started := time.Now()
		n, err := c.Write(make([]byte, 32<<10))
		var timeout net.Error
		if n == 0 || !errors.As(err, &timeout) || !timeout.Timeout() || time.Since(started) > time.Second {
			t.Fatalf("n=%d error=%v elapsed=%s", n, err, time.Since(started))
		}
	})
}

type slowWriteListener struct {
	net.Listener
	slow     *atomic.Bool
	progress *atomic.Int64
}

func (l *slowWriteListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return NewIdleWriteConn(&testProgressConn{Conn: c, slow: l.slow, progress: l.progress}), nil
}

func TestServerWSWriteIdleProgress(t *testing.T) {
	for _, tlsMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "ws", true: "wss"}[tlsMode], func(t *testing.T) {
			var slow atomic.Bool
			var progress atomic.Int64
			done := make(chan error, 1)
			ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				u := websocket.Upgrader{}
				c, err := u.Upgrade(WebSocketResponseWriter{w}, r, nil)
				if err != nil {
					done <- err
					return
				}
				defer c.Close()
				slow.Store(true)
				done <- WriteWSMessage(c, websocket.BinaryMessage, protocol.EncodeWSFrame(1, 0, bytes.Repeat([]byte("p"), 32<<10)), 80*time.Millisecond)
			}))
			ts.Listener = &slowWriteListener{ts.Listener, &slow, &progress}
			if tlsMode {
				ts.StartTLS()
			} else {
				ts.Start()
			}
			defer ts.Close()
			d := websocket.Dialer{}
			if tlsMode {
				d.TLSClientConfig = ts.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
			}
			url := "ws" + strings.TrimPrefix(ts.URL, "http")
			c, _, err := d.Dial(url, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, b, err := c.ReadMessage()
			if err != nil {
				t.Fatal(err)
			}
			_, _, data, err := protocol.DecodeWSFrame(b)
			if err != nil || len(data) != 32<<10 {
				t.Fatalf("length=%d error=%v", len(data), err)
			}
			if err = <-done; err != nil {
				t.Fatal(err)
			}
			if progress.Load() < 32 {
				t.Fatal("fixture did not exercise slow writes")
			}
		})
	}
}

func TestWSPayloadReservationCancelledBeforeAllocation(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(map[bool]string{false: "budget_timeout", true: "bad_digest"}[malformed], func(t *testing.T) {
			raw, codec := resource.New(8192), resource.New(16384)
			held := int64(8192)
			if malformed {
				held = 0
			}
			raw.Try(held)
			done := make(chan error, 1)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					done <- err
					return
				}
				defer c.Close()
				_, b, release, payloadRelease, err := ReadWSMessageWithPayload(context.Background(), c, codec, raw, 8192, 40*time.Millisecond, nil, nil)
				if release != nil {
					defer release()
				}
				if payloadRelease != nil {
					defer payloadRelease()
				}
				if err == nil {
					_, _, _, err = protocol.DecodeWSFrame(b)
				}
				done <- err
			}))
			defer ts.Close()
			c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			frame := protocol.EncodeWSFrame(1, 0, bytes.Repeat([]byte("p"), 8192))
			if malformed {
				frame[len(frame)-1] ^= 1
			}
			if err = c.WriteMessage(websocket.BinaryMessage, frame); err != nil {
				t.Fatal(err)
			}
			select {
			case err = <-done:
				if err == nil || !malformed && !errors.Is(err, ErrBackpressure) {
					t.Fatalf("unexpected error %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("reader did not stop")
			}
			// Wait for handler defers, including both independent leases.
			until := time.Now().Add(time.Second)
			for (codec.Used() != 0 || raw.Used() != held) && time.Now().Before(until) {
				time.Sleep(time.Millisecond)
			}
			if codec.Used() != 0 || raw.Used() != held {
				t.Fatalf("leak: codec=%d payload=%d held=%d", codec.Used(), raw.Used(), held)
			}
			raw.Put(held)
		})
	}
}
