package transport

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
)

func sseServer(body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		fmt.Fprint(w, body)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
}

func codecClient(t *testing.T, url string, codec int64) (*TripleTransportClient, *resource.Manager) {
	t.Helper()
	cfg := &config.Config{Node: config.NodeConfig{ID: "A"}}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	r := resource.NewManager(256<<20, 32<<20, 32<<20, 10)
	r.Codec = resource.New(codec)
	c := NewTripleTransportClientForURL(url, "token", &cfg.Transport, "ai", true)
	c.SetResources(r)
	t.Cleanup(c.Close)
	return c, r
}

// An SSE data line reserves codec budget for the frame size it declares, so a
// small frame is readable while most of the codec budget is held elsewhere.
func TestSSESmallFrameReadableUnderCodecPressure(t *testing.T) {
	payload := []byte("x")
	srv := sseServer(fmt.Sprintf("id: 1:1\nevent: data\ndata: %s\n\n", protocol.FormatSSEData(0, payload)))
	defer srv.Close()
	c, r := codecClient(t, srv.URL, 2<<20)
	if !r.Codec.Try(700 << 10) { // resident frames of other sessions
		t.Fatal("fixture")
	}
	sub, err := c.SubscribeEvents(context.Background(), "s1", "tok", "bi", 1, 256<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	ev, err := sub.NextEventWithProgress(context.Background(), 500*time.Millisecond, nil)
	if err != nil {
		t.Fatalf("1-byte frame not readable with %d bytes of codec free: %v", r.Codec.Limit()-r.Codec.Used(), err)
	}
	if _, got, err := protocol.ParseSSEData(ev.Data); err != nil || string(got) != "x" {
		t.Fatalf("data=%q err=%v", got, err)
	}
	ev.Release()
	if used := r.Codec.Used(); used != 700<<10 {
		t.Fatalf("codec leak: used=%d", used)
	}
}

// A data line longer than its declared frame (plus room for the rest of the
// event) is a protocol error.
func TestSSEDataLineLongerThanDeclaredIsProtocolError(t *testing.T) {
	line := protocol.FormatSSEData(0, []byte("x")) + strings.Repeat("A", 8<<10)
	srv := sseServer("id: 1:1\nevent: data\ndata: " + line + "\n\n")
	defer srv.Close()
	c, _ := codecClient(t, srv.URL, 32<<20)
	sub, err := c.SubscribeEvents(context.Background(), "s1", "tok", "bi", 1, 256<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if _, err := sub.NextEventWithProgress(context.Background(), time.Second, nil); !errors.Is(err, ErrProtocol) {
		t.Fatalf("err=%v, want ErrProtocol", err)
	}
}

// Frames up to the negotiated size, data lines without the space after the
// colon, and data lines that precede the id/event lines are still read.
func TestSSEDeclaredReservationAcceptsLegalLayouts(t *testing.T) {
	big := []byte(strings.Repeat("y", 200<<10))
	cases := map[string]string{
		"max frame":    fmt.Sprintf("id: 1:1\nevent: data\ndata: %s\n\n", protocol.FormatSSEData(0, big)),
		"no space":     fmt.Sprintf("id: 1:1\nevent: data\ndata:%s\n\n", protocol.FormatSSEData(0, []byte("z"))),
		"data first":   fmt.Sprintf("data: %s\nid: 1:1\nevent: data\n\n", protocol.FormatSSEData(0, []byte("z"))),
		"control json": "id: 1:1\nevent: ping\ndata: {\"type\":\"ping\"}\n\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			srv := sseServer(body)
			defer srv.Close()
			c, r := codecClient(t, srv.URL, 32<<20)
			sub, err := c.SubscribeEvents(context.Background(), "s1", "tok", "bi", 1, 256<<10)
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			ev, err := sub.NextEventWithProgress(context.Background(), time.Second, nil)
			if err != nil {
				t.Fatal(err)
			}
			ev.Release()
			if r.Codec.Used() != 0 {
				t.Fatalf("codec leak %d", r.Codec.Used())
			}
		})
	}
}
