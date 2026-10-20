package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
)

func budgetClient(t *testing.T, url string, sessions int, peerDial string) (*TripleTransportClient, *resource.Manager) {
	t.Helper()
	cfg := &config.Config{Node: config.NodeConfig{ID: "A"}, Limits: config.LimitsConfig{MaxSessions: sessions}, Transport: config.TransportConfig{PeerDialTimeoutRaw: peerDial}}
	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		t.Fatal(err)
	}
	r := resource.NewManager(256<<20, 32<<20, 32<<20, sessions)
	c := NewTripleTransportClientForURL(url, "token", &cfg.Transport, "ai", true)
	c.SetResources(r)
	t.Cleanup(c.Close)
	return c, r
}

func upHandler(delay time.Duration, active, peak *atomic.Int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/events") {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		n := active.Add(1)
		for m := peak.Load(); n > m && !peak.CompareAndSwap(m, n); m = peak.Load() {
		}
		time.Sleep(delay)
		active.Add(-1)
		w.Header().Set(protocol.HeaderInstanceID, "bi")
		_ = json.NewEncoder(w).Encode(protocol.UpResponse{SessionID: "s1", InstanceID: "bi", Epoch: "1", FrameAccepted: true, AckUpOffset: "0", DownAckOffset: "0", UpReceiveLimit: "0", StreamID: "st", Transport: &protocol.TransportMode{Mode: "http-sse"}})
	}
}

// Over HTTP/1.1 each SSE stream pins a connection and every in-flight up POST
// needs another one. All POSTs the request budget admits next to the open
// streams must get a connection, even when they are slow compared to
// peer_dial_timeout.
func TestPerPeerDataBudgetCarriesSSEAndInflightPOSTsOverHTTP1(t *testing.T) {
	var active, peak atomic.Int64
	srv := httptest.NewServer(upHandler(1500*time.Millisecond, &active, &peak))
	defer srv.Close()
	sessions := 5
	c, r := budgetClient(t, srv.URL, sessions, "300ms")
	if got, want := c.shared.dataConnections.Limit(), r.DataRequests.Limit(); got != want {
		t.Fatalf("per-peer data connections %d, want the data request budget %d", got, want)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < sessions; i++ {
		sub, err := c.SubscribeEvents(ctx, fmt.Sprintf("s%d", i), "tok", "bi", 1, 32<<10)
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
	}
	c.BindStream("s1", 1, "st", "http-sse")
	posts := int(r.DataRequests.Limit()) - sessions
	var wg sync.WaitGroup
	var failed atomic.Int64
	var firstErr atomic.Value
	for i := 0; i < posts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.SendUpFrame(ctx, "s1", "tok", "bi", 1, 0, []byte("x"), 0, 0); err != nil {
				failed.Add(1)
				firstErr.CompareAndSwap(nil, err.Error())
			}
		}()
	}
	wg.Wait()
	if failed.Load() > 0 {
		t.Fatalf("%d of %d concurrent up POSTs failed (peak %d at server): %v", failed.Load(), posts, peak.Load(), firstErr.Load())
	}
}

// A POST that cannot get a connection slot waits up to its own deadline and
// then fails with ErrLocalConnBudget, distinguishable from a carrier fault.
func TestUpPOSTWaitingForConnectionSlotFailsWithLocalBudgetError(t *testing.T) {
	var active, peak atomic.Int64
	srv := httptest.NewServer(upHandler(0, &active, &peak))
	defer srv.Close()
	c, _ := budgetClient(t, srv.URL, 1, "100ms")
	c.BindStream("s1", 1, "st", "http-sse")
	held := c.shared.dataConnections.Limit()
	if !c.shared.dataConnections.Try(held) {
		t.Fatal("fixture")
	}
	release := time.AfterFunc(400*time.Millisecond, func() { c.shared.dataConnections.Put(held) })
	defer release.Stop()
	// The slot frees up after 400ms, well past peer_dial_timeout (100ms) but
	// inside the request's deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if _, err := c.SendUpFrame(ctx, "s1", "tok", "bi", 1, 0, []byte("x"), 0, 0); err != nil {
		t.Fatalf("POST did not wait for a slot within its deadline: %v", err)
	}
	cancel()

	c.closeIdle(false)
	if !c.shared.dataConnections.Try(held) {
		t.Fatal("fixture")
	}
	defer c.shared.dataConnections.Put(held)
	ctx, cancel = context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.SendUpFrame(ctx, "s1", "tok", "bi", 1, 0, []byte("x"), 0, 0)
	if !errors.Is(err, ErrLocalConnBudget) {
		t.Fatalf("err=%v, want ErrLocalConnBudget", err)
	}
	if d := time.Since(start); d < 250*time.Millisecond {
		t.Fatalf("gave up after %s, before the request deadline", d)
	}
}

// A caller cancelling its request is a cancellation, not local backpressure.
func TestCancelledRequestIsNotLocalBudgetError(t *testing.T) {
	var active, peak atomic.Int64
	srv := httptest.NewServer(upHandler(0, &active, &peak))
	defer srv.Close()
	c, _ := budgetClient(t, srv.URL, 1, "100ms")
	c.BindStream("s1", 1, "st", "http-sse")
	held := c.shared.dataConnections.Limit()
	if !c.shared.dataConnections.Try(held) {
		t.Fatal("fixture")
	}
	defer c.shared.dataConnections.Put(held)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	_, err := c.SendUpFrame(ctx, "s1", "tok", "bi", 1, 0, []byte("x"), 0, 0)
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrLocalConnBudget) {
		t.Fatalf("err=%v, want a plain cancellation", err)
	}
}

// Idle peer connections are dropped before the egress server (90s) and
// common proxies (60-75s) close them.
func TestPeerPoolsDropIdleConnectionsBeforeServersAndProxies(t *testing.T) {
	c := NewTripleTransportClientForURL("http://127.0.0.1:1", "token", fixCfg(), "ai", true)
	defer c.Close()
	clients := append([]*http.Client{c.dataClient, c.controlClient}, c.nextSSEShard())
	for _, cl := range clients {
		if d := cl.Transport.(*http.Transport).IdleConnTimeout; d <= 0 || d >= 60*time.Second {
			t.Fatalf("IdleConnTimeout %s, want below 60s", d)
		}
	}
}
