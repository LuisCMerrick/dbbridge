package ingress

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
)

// brokenWSAttach serves clients through a path whose session-less WS
// precheck succeeds but whose real WS session upgrade always fails.
func brokenWSAttach(t *testing.T, recovery bool) (sessionDials int64, served int) {
	t.Helper()
	var wsSession atomic.Int64
	hook := func(w http.ResponseWriter, r *http.Request, _ http.Handler) bool {
		if r.URL.Path == "/v1/transport/ws" && r.Header.Get(protocol.HeaderWSPurpose) == "session" {
			wsSession.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			return true
		}
		return false
	}
	e, _ := fix2Setup(t, "auto", false, func(c *config.Config) { c.Transport.Recovery.EnabledRaw = recovery }, nil, hook)
	if err := e.m.Start(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		c, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		c.SetDeadline(time.Now().Add(8 * time.Second))
		if fixEcho2(t, c, "q") == nil {
			served++
		}
		c.Close()
	}
	return wsSession.Load(), served
}

func TestBrokenWSAttachWithRecoveryUsesSSE(t *testing.T) {
	n, served := brokenWSAttach(t, true)
	if served != 3 || n != 1 {
		t.Fatalf("recovery on: served=%d/3 ws session attempts=%d", served, n)
	}
}

// With recovery disabled the failed session ends, but its carrier still
// cools down and the cached precheck is dropped, so later clients use SSE.
func TestBrokenWSAttachWithoutRecoveryCoolsCarrier(t *testing.T) {
	n, served := brokenWSAttach(t, false)
	if n != 1 || served < 2 {
		t.Fatalf("recovery off: ws session attempts=%d served=%d/3; want one failed WS attempt, later clients on SSE", n, served)
	}
}

// A healthy but slow WS path must not be cooled down when the local create
// deadline cuts the first attach short.
func TestLocalCreateDeadlineDoesNotCoolCarrier(t *testing.T) {
	hook := func(w http.ResponseWriter, r *http.Request, base http.Handler) bool {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/sessions" {
			time.Sleep(1300 * time.Millisecond)
		}
		if r.URL.Path == "/v1/transport/ws" && r.Header.Get(protocol.HeaderWSPurpose) == "session" {
			time.Sleep(300 * time.Millisecond)
		}
		return false
	}
	e, _ := fix2Setup(t, "ws", false, func(c *config.Config) {
		c.Transport.CreateRetryTimeoutRaw = "1500ms"
		c.Transport.Recovery.EnabledRaw = true
	}, nil, hook)
	if err := e.m.Start(); err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = c.Read(make([]byte, 1))
	c.Close()
	ep := e.m.linkManagers["B"].GetEndpoints()[0]
	if !ep.TransportAvailable(time.Now(), "ws") {
		t.Fatal("local create-deadline expiry during attach cooled the healthy WS carrier")
	}
}

type localBudgetError struct{}

func (localBudgetError) Error() string       { return "local connection budget wait ended" }
func (localBudgetError) LocalResource() bool { return true }

func TestCarrierFailureExcludesLocalCauses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &ingressSession{ctx: ctx}
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancelExpired()
	cases := []struct {
		name   string
		parent context.Context
		err    error
		want   bool
	}{
		{"carrier reset", nil, errors.New("read: connection reset by peer"), true},
		{"detection timeout", nil, context.DeadlineExceeded, true},
		{"local deadline", expired, errors.New("use of closed network connection"), false},
		{"local probe slot", nil, fmt.Errorf("%w: busy", errLocalBusy), false},
		{"local budget", nil, fmt.Errorf("dial: %w", resource.ErrLimit), false},
		{"local budget hook", nil, fmt.Errorf("post: %w", localBudgetError{}), false},
		{"client socket", nil, localSocketError(errors.New("broken pipe")), false},
	}
	for _, c := range cases {
		if got := s.carrierFailure(c.parent, c.err); got != c.want {
			t.Errorf("%s: carrierFailure=%v want %v", c.name, got, c.want)
		}
	}
	s.closed.Store(true)
	if s.carrierFailure(nil, errors.New("reset")) {
		t.Error("a stopped session must not cool its carrier")
	}
}

// The first attach is bounded by the owner's unattached timeout when that is
// earlier than the create deadline; once it passed, the session ends without
// a recovery attempt and without cooling the carrier.
func TestFirstAttachBoundedByUnattachedTimeout(t *testing.T) {
	var resumes atomic.Int64
	hook := func(w http.ResponseWriter, r *http.Request, base http.Handler) bool {
		if r.URL.Path == "/v1/transport/ws" && r.Header.Get(protocol.HeaderWSPurpose) == "session" {
			time.Sleep(800 * time.Millisecond)
		}
		if r.Method == http.MethodPost && len(r.URL.Path) > len("/resume") && r.URL.Path[len(r.URL.Path)-len("/resume"):] == "/resume" {
			resumes.Add(1)
		}
		return false
	}
	e, _ := fix2Setup(t, "ws", false, func(c *config.Config) {
		c.Transport.Recovery.EnabledRaw = true
	}, func(b *config.Config) { b.Transport.UnattachedTimeoutRaw = "400ms" }, hook)
	if err := e.m.Start(); err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	_, rerr := c.Read(make([]byte, 1))
	elapsed := time.Since(start)
	c.Close()
	ep := e.m.linkManagers["B"].GetEndpoints()[0]
	if rerr == nil || elapsed > 2*time.Second || resumes.Load() != 0 || !ep.TransportAvailable(time.Now(), "ws") {
		t.Fatalf("read err=%v after %v, resumes=%d, ws available=%v", rerr, elapsed, resumes.Load(), ep.TransportAvailable(time.Now(), "ws"))
	}
}
