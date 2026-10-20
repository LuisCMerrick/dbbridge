package transport

import (
	"bufio"
	"bytes"
	"context"
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/gorilla/websocket"
)

func fixSub(r io.Reader, codec *resource.Budget, maxFrame int) *SSESubscriber {
	body := io.NopCloser(r)
	return &SSESubscriber{ctx: context.Background(), body: body, reader: bufio.NewReaderSize(body, 4096), codec: codec, maxFrameSize: maxFrame}
}

func fixReservation(maxFrame int, codecLimit int64) int64 {
	limit := maxFrame*4/3 + 4096
	if limit < 16<<10 {
		limit = 16 << 10
	}
	return min(int64(limit)*4+64*(sseMaxLines+1), codecLimit)
}

// M1: two SSE readers share a codec budget that fits one event's reservation
// but not two. Each event arrives in two halves. Before the fix both readers
// held a partial charge while waiting for the rest and one died with
// ErrBackpressure; now the second waits without holding anything.
func TestFixSSECodecReservesWholeEventWithoutHoldAndWait(t *testing.T) {
	const maxFrame = 256 << 10
	want := fixReservation(maxFrame, 1<<62)
	codec := resource.New(want + want/2)
	data := strings.Repeat("A", 4096*24)
	full := "event: data\ndata: " + data + "\n\n"
	split := len(full) / 2
	errs := make(chan error, 2)
	var writers sync.WaitGroup
	for i := 0; i < 2; i++ {
		pr, pw := io.Pipe()
		s := fixSub(pr, codec, maxFrame)
		go func() {
			ev, err := s.NextEventWithProgress(context.Background(), 2*time.Second, nil)
			if err == nil && ev.Data != data {
				err = errors.New("payload corrupted")
			}
			if ev != nil {
				time.Sleep(50 * time.Millisecond) // consumer holds the event briefly
				ev.Release()
			}
			errs <- err
		}()
		writers.Add(1)
		go func() {
			defer writers.Done()
			pw.Write([]byte(full[:split]))
			time.Sleep(100 * time.Millisecond)
			pw.Write([]byte(full[split:]))
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("reader failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("readers stalled")
		}
	}
	writers.Wait()
	if codec.Used() != 0 {
		t.Fatalf("codec lease leaked: %d", codec.Used())
	}
}

// M1: the reservation is taken once, before the event is buffered, and the
// unused part is refunded when the event completes.
func TestFixSSECodecSingleReservationAndRefund(t *testing.T) {
	codec := resource.New(64 << 20)
	pr, pw := io.Pipe()
	s := fixSub(pr, codec, 1024)
	got := make(chan *SSEEvent, 1)
	go func() {
		ev, err := s.NextEventWithProgress(context.Background(), 2*time.Second, nil)
		if err != nil {
			t.Error(err)
		}
		got <- ev
	}()
	pw.Write([]byte("event: data\n"))
	deadline := time.Now().Add(time.Second)
	want := fixReservation(1024, codec.Limit())
	for codec.Used() != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if codec.Used() != want {
		t.Fatalf("mid-event charge=%d, want one reservation of %d", codec.Used(), want)
	}
	go pw.Write([]byte("data: {}\n\n"))
	ev := <-got
	if ev == nil {
		t.Fatal("no event")
	}
	if u := codec.Used(); u >= want || u == 0 {
		t.Fatalf("unused reservation not refunded: used=%d reservation=%d", u, want)
	}
	ev.Release()
	if codec.Used() != 0 {
		t.Fatalf("codec lease leaked: %d", codec.Used())
	}
}

// M1: on any error the whole reservation goes back.
func TestFixSSECodecRefundOnError(t *testing.T) {
	codec := resource.New(64 << 20)
	for name, in := range map[string]string{
		"oversized": "event: data\ndata: " + strings.Repeat("x", 20<<10) + "\n\n",
		"truncated": "event: data\ndata: abc",
		"bad utf8":  "event: data\ndata: \xff\n\n",
	} {
		_, err := fixSub(strings.NewReader(in), codec, 1024).NextEventWithProgress(context.Background(), time.Second, nil)
		if err == nil {
			t.Fatalf("%s: accepted", name)
		}
		if codec.Used() != 0 {
			t.Fatalf("%s: codec lease leaked: %d", name, codec.Used())
		}
	}
}

// M1: a small codec budget still serves an event (reservation is capped at
// the budget limit instead of failing with ErrLimit).
func TestFixSSECodecReservationCappedAtBudget(t *testing.T) {
	codec := resource.New(4096)
	ev, err := fixSub(strings.NewReader("event: ping\ndata: {}\n\n"), codec, 256<<10).NextEventWithProgress(context.Background(), time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	ev.Release()
	if codec.Used() != 0 {
		t.Fatalf("codec lease leaked: %d", codec.Used())
	}
}

// L3: keep-alive comments and empty/ignored-only blocks between events neither
// accumulate toward the event size limit nor hold codec budget.
func TestFixSSECommentsAndEmptyBlocksResetAccounting(t *testing.T) {
	codec := resource.New(1 << 20)
	in := strings.Repeat(": keepalive\n\n", 2000) + strings.Repeat("retry: 1000\n\n", 200) + "event: ping\ndata: {}\n\n"
	ev, err := fixSub(strings.NewReader(in), codec, 1024).NextEventWithProgress(context.Background(), time.Second, nil)
	if err != nil {
		t.Fatalf("keep-alives broke the stream: %v", err)
	}
	if ev.Event != "ping" || ev.Data != "{}" {
		t.Fatalf("event=%+v", ev)
	}
	ev.Release()
	if codec.Used() != 0 {
		t.Fatalf("codec lease leaked: %d", codec.Used())
	}
	// An idle stream of comments holds no codec budget while waiting.
	pr, pw := io.Pipe()
	s := fixSub(pr, codec, 1024)
	done := make(chan error, 1)
	go func() {
		ev, err := s.NextEventWithProgress(context.Background(), 2*time.Second, nil)
		if ev != nil {
			ev.Release()
		}
		done <- err
	}()
	for i := 0; i < 100; i++ {
		pw.Write([]byte(": keepalive\n\n"))
	}
	if codec.Used() != 0 {
		t.Fatalf("idle keep-alives hold codec budget: %d", codec.Used())
	}
	pw.Write([]byte("event: ping\ndata: {}\n\n"))
	if err := <-done; err != nil || codec.Used() != 0 {
		t.Fatalf("err=%v used=%d", err, codec.Used())
	}
}

// L2: the parser follows the WHATWG event-stream rules while keeping the
// protocol's size limits.
func TestFixSSEParserWHATWG(t *testing.T) {
	type want struct{ event, id, data string }
	ok := map[string]struct {
		in   string
		want []want
	}{
		"multi-line data":     {"event: x\ndata: a\ndata: b\n\n", []want{{"x", "", "a\nb"}}},
		"retry ignored":       {"retry: 1000\nevent: x\ndata: a\n\n", []want{{"x", "", "a"}}},
		"unknown ignored":     {"event: x\nfoo: bar\ndata: a\n\n", []want{{"x", "", "a"}}},
		"CR endings":          {"event: x\rdata: a\r\r", []want{{"x", "", "a"}}},
		"CRLF endings":        {"event: x\r\nid: 7\r\ndata: a\r\n\r\n", []want{{"x", "7", "a"}}},
		"mixed endings":       {"event: x\rdata: a\r\r\nevent: y\ndata: b\r\n\nevent: z\r\ndata: c\r\r", []want{{"x", "", "a"}, {"y", "", "b"}, {"z", "", "c"}}},
		"field without colon": {"event: x\ndata: a\nid\n\n", []want{{"x", "", "a"}}},
		"bare data":           {"event: x\ndata\n\n", []want{{"x", "", ""}}},
		"one space stripped":  {"event:x\ndata:  a\n\n", []want{{"x", "", " a"}}},
		"value with colon":    {"event: x\ndata: a:b\n\n", []want{{"x", "", "a:b"}}},
		"no data skipped":     {"event: x\n\nevent: y\ndata: b\n\n", []want{{"y", "", "b"}}},
		"comment mid-event":   {"event: x\n: hi\ndata: a\n\n", []want{{"x", "", "a"}}},
	}
	for name, tc := range ok {
		for _, slow := range []bool{false, true} {
			var r io.Reader = strings.NewReader(tc.in)
			if slow {
				r = iotest.OneByteReader(r) // CR and LF arrive in separate reads
			}
			s := fixSub(r, resource.New(1<<20), 1024)
			for i, w := range tc.want {
				ev, err := s.NextEventWithProgress(context.Background(), time.Second, nil)
				if err != nil {
					t.Fatalf("%s (slow=%v) event %d: %v", name, slow, i, err)
				}
				if ev.Event != w.event || ev.ID != w.id || ev.Data != w.data {
					t.Fatalf("%s (slow=%v) event %d: got %+v want %+v", name, slow, i, ev, w)
				}
				ev.Release()
			}
			if _, err := s.NextEventWithProgress(context.Background(), time.Second, nil); !errors.Is(err, io.EOF) {
				t.Fatalf("%s (slow=%v): trailing input: %v", name, slow, err)
			}
		}
	}
	bad := map[string]string{
		"data without event": "data: a\n\n",
		"duplicate event":    "event: x\nevent: y\ndata: a\n\n",
		"duplicate id":       "event: x\nid: 1\nid: 2\ndata: a\n\n",
		"too many lines":     "event: x\n" + strings.Repeat("data: a\n", 64) + "\n",
		"joined too large":   "event: x\n" + strings.Repeat("data: "+strings.Repeat("a", 1000)+"\n", 20) + "\n",
		"line too large CR":  "event: x\rdata: " + strings.Repeat("a", 20<<10) + "\r\r",
		"comment too large":  ":" + strings.Repeat("a", 20<<10) + "\n\nevent: x\ndata: a\n\n",
	}
	for name, in := range bad {
		_, err := fixSub(strings.NewReader(in), resource.New(1<<20), 1024).NextEventWithProgress(context.Background(), time.Second, nil)
		if !errors.Is(err, ErrProtocol) {
			t.Fatalf("%s: err=%v, want ErrProtocol", name, err)
		}
	}
}

// L1: Close sends a real Close frame before tearing the socket down.
func TestFixWSCloseSendsCloseFrame(t *testing.T) {
	up := websocket.Upgrader{}
	codes := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_, _, err = c.ReadMessage()
		var ce *websocket.CloseError
		if errors.As(err, &ce) {
			codes <- fmt.Sprint(ce.Code)
		} else {
			codes <- "abrupt: " + err.Error()
		}
	}))
	defer srv.Close()
	for i := 0; i < 20; i++ {
		c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		w := &WSSessionConn{conn: c}
		start := time.Now()
		w.Close()
		if d := time.Since(start); d > 500*time.Millisecond {
			t.Fatalf("Close took %s", d)
		}
		if got := <-codes; got != fmt.Sprint(websocket.CloseNormalClosure) {
			t.Fatalf("run %d: peer saw %s, want 1000", i, got)
		}
	}
}

// L4: the WS reader reports blocked only while it actually waits for budget.
func TestFixWSReaderBlockedOnlyWhenWaiting(t *testing.T) {
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		c.WriteMessage(websocket.TextMessage, []byte(`{"type":"x"}`))
		c.WriteMessage(websocket.BinaryMessage, protocol.EncodeWSFrame(1, 0, []byte("payload")))
		c.WriteMessage(websocket.TextMessage, []byte(`{"type":"y"}`))
		c.ReadMessage()
	}))
	defer srv.Close()
	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	budget, raw := resource.New(1<<20), resource.New(1<<20)
	var mu sync.Mutex
	var calls []bool
	blocked := func(v bool) { mu.Lock(); calls = append(calls, v); mu.Unlock() }
	for i := 0; i < 2; i++ {
		_, _, release, releasePayload, err := ReadWSMessageWithPayload(context.Background(), c, budget, raw, 1024, time.Second, nil, blocked)
		if err != nil {
			t.Fatal(err)
		}
		release()
		if releasePayload != nil {
			releasePayload()
		}
	}
	mu.Lock()
	if len(calls) != 0 {
		t.Fatalf("free budget reported blocked: %v", calls)
	}
	mu.Unlock()
	if !budget.Try(budget.Limit()) {
		t.Fatal("fixture")
	}
	go func() { time.Sleep(50 * time.Millisecond); budget.Put(budget.Limit()) }()
	_, _, release, _, err := ReadWSMessageWithPayload(context.Background(), c, budget, raw, 1024, time.Second, nil, blocked)
	if err != nil {
		t.Fatal(err)
	}
	release()
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 || !calls[0] || calls[1] {
		t.Fatalf("blocked transitions=%v, want [true false]", calls)
	}
}

func fixClient(t *testing.T, url string, verify bool) *TripleTransportClient {
	t.Helper()
	cfg := &config.Config{Node: config.NodeConfig{ID: "A"}, Transport: config.TransportConfig{WS: config.WSConfig{HandshakeTimeoutRaw: "300ms"}}}
	if e := cfg.ApplyDefaultsAndValidate(); e != nil {
		t.Fatal(e)
	}
	c := NewTripleTransportClientForURL(url, "token", &cfg.Transport, "ai", verify)
	t.Cleanup(c.Close)
	return c
}

// L6 + wsUnavailable: only genuine upgrade refusals become
// ErrWSUpgradeRejected; identity, subprotocol, version and illegal hello data
// are ErrProtocol (3.7.3(1,3), TF-07/TF-08, audit DBB-AUD-01); cancellation,
// deadlines and dial failures are neither.
func TestFixPrecheckErrorClassification(t *testing.T) {
	hello := func(r *http.Request) protocol.WSHelloMessage {
		return protocol.WSHelloMessage{Type: "hello", ProtocolVersion: protocol.ProtocolVersionString, NodeID: "B", InstanceID: "bi", Nonce: r.Header.Get(protocol.HeaderWSNonce), Transport: "ws"}
	}
	server := func(subprotocol bool, send func(c *websocket.Conn, r *http.Request)) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			up := websocket.Upgrader{}
			if subprotocol {
				up.Subprotocols = []string{protocol.SecWebSocketProtocol}
			}
			c, e := up.Upgrade(w, r, nil)
			if e != nil {
				return
			}
			defer c.Close()
			send(c, r)
			c.ReadMessage()
		}))
	}
	type kind int
	const (
		rejected kind = iota
		fatal
		other
		ok
	)
	cases := map[string]struct {
		srv  *httptest.Server
		want kind
	}{
		"valid":                {server(true, func(c *websocket.Conn, r *http.Request) { c.WriteJSON(hello(r)) }), ok},
		"subprotocol stripped": {server(false, func(c *websocket.Conn, r *http.Request) { c.WriteJSON(hello(r)) }), fatal},
		"binary hello":         {server(true, func(c *websocket.Conn, r *http.Request) { c.WriteMessage(websocket.BinaryMessage, []byte("x")) }), fatal},
		"garbled hello":        {server(true, func(c *websocket.Conn, r *http.Request) { c.WriteMessage(websocket.TextMessage, []byte("<html>")) }), fatal},
		"wrong version": {server(true, func(c *websocket.Conn, r *http.Request) {
			h := hello(r)
			h.ProtocolVersion = "0.9"
			c.WriteJSON(h)
		}), fatal},
		"closed after 101": {server(true, func(c *websocket.Conn, r *http.Request) { c.Close() }), rejected},
		"wrong nonce": {server(true, func(c *websocket.Conn, r *http.Request) {
			h := hello(r)
			h.Nonce = "forged"
			c.WriteJSON(h)
		}), fatal},
		"wrong node": {server(true, func(c *websocket.Conn, r *http.Request) {
			h := hello(r)
			h.NodeID = "X"
			c.WriteJSON(h)
		}), fatal},
		"wrong instance": {server(true, func(c *websocket.Conn, r *http.Request) {
			h := hello(r)
			h.InstanceID = "other"
			c.WriteJSON(h)
		}), fatal},
		"hello withheld": {server(true, func(c *websocket.Conn, r *http.Request) {}), other},
	}
	for name, tc := range cases {
		c := fixClient(t, tc.srv.URL, true)
		err := c.PrecheckWS(context.Background(), "B", "bi")
		tc.srv.Close()
		isRej, isFatal := errors.Is(err, ErrWSUpgradeRejected), errors.Is(err, ErrProtocol)
		switch tc.want {
		case ok:
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case rejected:
			if !isRej || isFatal {
				t.Fatalf("%s: err=%v, want ErrWSUpgradeRejected only", name, err)
			}
		case fatal:
			if !isFatal || isRej {
				t.Fatalf("%s: err=%v, want ErrProtocol only", name, err)
			}
		case other:
			if err == nil || isRej || isFatal || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s: err=%v, want unclassified deadline error", name, err)
			}
		}
	}

	// Dial failure: generic, not a rejection.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	err := fixClient(t, "http://"+addr, true).PrecheckWS(context.Background(), "B", "bi")
	if err == nil || errors.Is(err, ErrWSUpgradeRejected) || errors.Is(err, ErrProtocol) {
		t.Fatalf("refused dial: err=%v, want unclassified", err)
	}

	// Caller cancellation: context.Canceled, not a rejection.
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer hang.Close()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)
	err = fixClient(t, hang.URL, true).PrecheckWS(ctx, "B", "bi")
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrWSUpgradeRejected) {
		t.Fatalf("cancelled precheck: err=%v", err)
	}
	// Deadline while waiting for the 101: DeadlineExceeded, not a rejection.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()
	err = fixClient(t, hang.URL, true).PrecheckWS(ctx2, "B", "bi")
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrWSUpgradeRejected) {
		t.Fatalf("timed-out precheck: err=%v", err)
	}
}

// L7: a certificate that fails verification is fatal for the session dial as
// it is for the precheck.
func TestFixDialWSSessionTLSVerifyFailureIsFatal(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer ts.Close()
	c := fixClient(t, ts.URL, true)
	_, _, err := c.DialWSSession(context.Background(), "sid", "token", 1, "bi")
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("session dial with untrusted cert: err=%v, want ErrProtocol", err)
	}
	if err := c.PrecheckWS(context.Background(), "B", "bi"); !errors.Is(err, ErrProtocol) || errors.Is(err, ErrWSUpgradeRejected) {
		t.Fatalf("precheck with untrusted cert: err=%v", err)
	}
}

// L8: budget waits inside dial() end after DialTimeout even though net/http
// dials on a context that is never cancelled.
func TestFixDialBudgetWaitBoundedByDialTimeout(t *testing.T) {
	cfg := &config.Config{Node: config.NodeConfig{ID: "A"}, Transport: config.TransportConfig{DialTimeoutRaw: "100ms"}}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	r := cfg.Resources
	r.ControlConnections = resource.New(1)
	c := NewTripleTransportClientForURL("http://127.0.0.1:1", "token", &cfg.Transport, "ai", true)
	c.SetResources(r)
	defer c.Close()
	if !r.ControlConnections.Try(1) {
		t.Fatal("fixture")
	}
	defer r.ControlConnections.Put(1)
	start := time.Now()
	_, err := c.dial(context.WithoutCancel(context.Background()), "tcp", "127.0.0.1:1", true, nil)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrLocalConnBudget) {
		t.Fatalf("err=%v, want ErrLocalConnBudget and DeadlineExceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("budget wait took %s", d)
	}
	if r.ControlConnections.Used() != 1 || r.ControlMetadata.Used() != 0 {
		t.Fatalf("leases leaked: conns=%d meta=%d", r.ControlConnections.Used(), r.ControlMetadata.Used())
	}
}

// M2: disabling verification for a TLS peer is logged loudly.
func TestFixInsecureTLSWarns(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(old)
	cfg := &config.Config{Node: config.NodeConfig{ID: "A"}}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	NewTripleTransportClientForURL("https://peer.example:8443", "t", &cfg.Transport, "ai", true).WithURL("https://backup.example").Close()
	if buf.Len() != 0 {
		t.Fatalf("warned with verification on: %s", buf.String())
	}
	c := NewTripleTransportClientForURL("https://peer.example:8443", "t", &cfg.Transport, "ai", false)
	c.WithURL("https://backup.example")
	c.Close()
	out := buf.String()
	if strings.Count(out, "level=WARN") != 2 || !strings.Contains(out, "peer.example:8443") || !strings.Contains(out, "backup.example") {
		t.Fatalf("missing insecure TLS warnings: %s", out)
	}
}
