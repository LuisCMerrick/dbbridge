package transport

import (
	"bytes"
	"context"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"errors"
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPartialWSReadProgressRefreshesStallDeadline(t *testing.T) {
	result := make(chan error, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{}
		c, e := up.Upgrade(w, r, nil)
		if e != nil {
			result <- e
			return
		}
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, b, release, e := ReadWSMessageWithProgress(ctx, c, resource.New(1<<20), 1024, 100*time.Millisecond, nil)
		if release != nil {
			defer release()
		}
		if e == nil {
			_, _, payload, err := protocol.DecodeWSFrame(b)
			e = err
			if e == nil && !bytes.Equal(payload, bytes.Repeat([]byte{7}, 100)) {
				e = ErrProtocol
			}
		}
		result <- e
	}))
	defer ts.Close()
	c, _, e := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	message := protocol.EncodeWSFrame(1, 0, bytes.Repeat([]byte{7}, 100))
	for i, span := range [][2]int{{0, 76}, {76, 96}, {96, 116}, {116, 136}, {136, 156}} {
		opcode := byte(0)
		if i == 0 {
			opcode = 2
		}
		if i == 4 {
			opcode |= 0x80
		}
		payload := message[span[0]:span[1]]
		fragment := append([]byte{opcode, 0x80 | byte(len(payload)), 0, 0, 0, 0}, payload...)
		if _, e = c.UnderlyingConn().Write(fragment); e != nil {
			t.Fatal(e)
		}
		if i < 4 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	select {
	case e := <-result:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("message reader did not complete")
	}
}

func TestIdleWSReadDoesNotUseMessageStallAsConnectionLifetime(t *testing.T) {
	result := make(chan error, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{}
		c, e := up.Upgrade(w, r, nil)
		if e != nil {
			result <- e
			return
		}
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _, release, e := ReadWSMessageWithProgress(ctx, c, resource.New(1<<20), 1024, 30*time.Millisecond, nil)
		if release != nil {
			release()
		}
		result <- e
	}))
	defer ts.Close()
	c, _, e := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	time.Sleep(100 * time.Millisecond)
	if e = c.WriteMessage(websocket.BinaryMessage, protocol.EncodeWSFrame(1, 0, []byte("after idle"))); e != nil {
		t.Fatal(e)
	}
	select {
	case e := <-result:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("idle reader did not finish")
	}
}

func TestPartialWSWireDisconnectRemainsRecoverable(t *testing.T) {
	result := make(chan error, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{}
		c, e := up.Upgrade(w, r, nil)
		if e != nil {
			result <- e
			return
		}
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _, release, e := ReadWSMessageWithProgress(ctx, c, resource.New(1<<20), 1024, 100*time.Millisecond, nil)
		if release != nil {
			release()
		}
		result <- e
	}))
	defer ts.Close()
	c, _, e := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if e != nil {
		t.Fatal(e)
	}
	message := protocol.EncodeWSFrame(1, 0, []byte("partial data"))
	// A complete non-final fragment; the peer disappears before continuation.
	fragment := append([]byte{2, 0x80 | byte(len(message)), 0, 0, 0, 0}, message...)
	if _, e = c.UnderlyingConn().Write(fragment); e != nil {
		t.Fatal(e)
	}
	c.Close()
	select {
	case e := <-result:
		if e == nil || errors.Is(e, ErrProtocol) {
			t.Fatalf("incomplete wire message was classified as protocol violation: %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("disconnected reader did not exit")
	}
}
