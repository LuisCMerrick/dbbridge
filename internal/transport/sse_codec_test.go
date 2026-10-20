package transport

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
)

func newTestSubscriber(r io.Reader, m *resource.Manager, maxFrame int) *SSESubscriber {
	rc, ok := r.(io.ReadCloser)
	if !ok {
		rc = io.NopCloser(r)
	}
	return &SSESubscriber{body: rc, reader: bufio.NewReader(rc), maxFrameSize: maxFrame, client: &TripleTransportClient{resources: m}, ctx: context.Background()}
}

// Release of a previous event on another goroutine must not race with
// the reader preparing the next event (run with -race).
func TestSSECodecReleaseNoRace(t *testing.T) {
	pr, pw := io.Pipe()
	m := resource.NewManager(64<<20, 64<<20, 16<<20, 10)
	s := newTestSubscriber(pr, m, 1024)
	go func() {
		for i := 0; i < 200; i++ {
			_, _ = pw.Write([]byte("event: data\ndata: x\n\n"))
		}
		_ = pw.Close()
	}()
	var wg sync.WaitGroup
	for {
		ev, err := s.NextEventWithProgress(context.Background(), time.Second, nil)
		if err != nil {
			break
		}
		wg.Add(1)
		go func() { defer wg.Done(); ev.Release(); ev.Release() }()
	}
	wg.Wait()
	if u := m.Codec.Used(); u != 0 {
		t.Fatalf("codec leaked %d bytes", u)
	}
}

// A completed data event keeps only its resident size charged
// (Data text + decoded payload), not 4x the base64 text.
func TestSSECodecChargeResident(t *testing.T) {
	const n = 256 << 10
	payload := make([]byte, n)
	for i := range payload {
		payload[i] = byte(i)
	}
	line := protocol.FormatSSEData(0, payload)
	m := resource.NewManager(64<<20, 32<<20, 16<<20, 10)
	s := newTestSubscriber(strings.NewReader("id: 1:262144\nevent: data\ndata: "+line+"\n\n"), m, n)
	ev, err := s.NextEventWithProgress(context.Background(), time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	used := m.Codec.Used()
	if ratio := float64(used) / n; ratio > 2.5 {
		t.Fatalf("charged %d bytes for a %d byte frame (ratio %.2f > 2.5; was 5.34 before TRN-2)", used, n, ratio)
	}
	if used < int64(len(ev.Data)) {
		t.Fatalf("charge %d below resident Data %d", used, len(ev.Data))
	}
	_, p, err := protocol.ParseSSEData(ev.Data)
	if err != nil {
		t.Fatal(err)
	}
	ev.Data = ""
	ev.Shrink(int64(len(p)))
	if u := m.Codec.Used(); u != int64(len(p)) {
		t.Fatalf("after Shrink used=%d want %d", u, len(p))
	}
	ev.Shrink(int64(len(p)) * 2) // never grows
	ev.Release()
	ev.Release()
	if u := m.Codec.Used(); u != 0 {
		t.Fatalf("codec leaked %d bytes", u)
	}
}

// Control events are read under a small reservation, so a heartbeat
// still gets through when the codec has no room for a worst-case data frame;
// a large data frame in the same state waits (and hits the backpressure
// timeout).
func TestSSEControlEventSmallReservation(t *testing.T) {
	m := resource.NewManager(64<<20, 8<<20, 16<<20, 10)
	if !m.Codec.Try(8<<20 - 200<<10) {
		t.Fatal("fixture")
	}
	pr, pw := io.Pipe()
	s := newTestSubscriber(pr, m, 1<<20)
	go func() {
		_, _ = pw.Write([]byte("id: x\nevent: ping\ndata: {\"type\":\"ping\"}\n\n"))
		_, _ = pw.Write([]byte("event: data\ndata: 0:1048576:"))
	}()
	ev, err := s.NextEventWithProgress(context.Background(), 2*time.Second, nil)
	if err != nil {
		t.Fatalf("ping blocked behind worst-case reservation: %v", err)
	}
	if ev.Event != "ping" || ev.ID != "x" {
		t.Fatalf("unexpected event %+v", ev)
	}
	ev.Release()
	blocked := false
	_, err = s.NextEventWithProgress(context.Background(), 200*time.Millisecond, nil, func(b bool) { blocked = blocked || b })
	if !errors.Is(err, ErrBackpressure) || !blocked {
		t.Fatalf("data frame should wait for codec: err=%v blocked=%v", err, blocked)
	}
	_ = pw.Close()
	if u := m.Codec.Used(); u != 8<<20-200<<10 {
		t.Fatalf("codec accounting off: %d", u)
	}
}

// A control event larger than sseControlLimit is a protocol error and
// cannot pin a worst-case reservation.
func TestSSEControlEventLimit(t *testing.T) {
	m := resource.NewManager(64<<20, 32<<20, 16<<20, 10)
	big := strings.Repeat("a", sseControlLimit+1)
	s := newTestSubscriber(strings.NewReader("event: ping\ndata: "+big+"\n\n"), m, 1<<20)
	if _, err := s.NextEventWithProgress(context.Background(), time.Second, nil); !errors.Is(err, ErrProtocol) {
		t.Fatalf("oversized control event accepted: %v", err)
	}
	if u := m.Codec.Used(); u != 0 {
		t.Fatalf("codec leaked %d", u)
	}
	// A data event without a preceding event: line may still carry a frame.
	payload := make([]byte, 64<<10)
	s = newTestSubscriber(strings.NewReader("data: "+protocol.FormatSSEData(0, payload)+"\nevent: data\n\n"), m, 1<<20)
	ev, err := s.NextEventWithProgress(context.Background(), time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	ev.Release()
}

// Parent cancellation ends the whole subscription (documented).
func TestNextEventCancelClosesSubscription(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()
	s := newTestSubscriber(pr, nil, 1024)
	released := 0
	s.releaseRequest = func() { released++ }
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.NextEventWithProgress(ctx, 0, nil); err == nil {
		t.Fatal("expected error")
	}
	if released != 1 {
		t.Fatalf("subscription not closed on cancel (released=%d)", released)
	}
	if _, err := s.NextEventWithProgress(context.Background(), 100*time.Millisecond, nil); err == nil {
		t.Fatal("closed subscription returned an event")
	}
}
