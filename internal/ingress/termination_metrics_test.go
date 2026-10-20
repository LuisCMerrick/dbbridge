package ingress

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
)

func TestReasonCategoryShutdownAndLocalSocket(t *testing.T) {
	if got := reasonCategory(errNodeStopped); got != "shutdown" {
		t.Fatalf("node stop: %q", got)
	}
	if got := reasonCategory(localSocketError(errors.New("write: broken pipe"))); got != "local_socket" {
		t.Fatalf("client socket error: %q", got)
	}
	if got := reasonCategory(errors.New("read: connection reset by peer")); got != "network" {
		t.Fatalf("carrier error: %q", got)
	}
}

func TestTerminationCountersByRecoveryMode(t *testing.T) {
	m := observability.DefaultMetrics
	read := func() (int64, int64, int64) {
		return atomic.LoadInt64(&m.TerminatedDirectTotal), atomic.LoadInt64(&m.TerminatedAfterResumeFailTotal), atomic.LoadInt64(&m.ResumeFailed)
	}
	d0, f0, r0 := read()
	countTermination(errNodeStopped, true, true)
	countTermination(errNodeStopped, false, false)
	countTermination(errClosedNormal, true, false)
	if d, f, r := read(); d != d0 || f != f0 || r != r0 {
		t.Fatalf("shutdown/normal counted as failures: direct %d->%d final %d->%d resume %d->%d", d0, d, f0, f, r0, r)
	}
	// Recovery enabled, fatal error while active: final failure, no failed resume.
	countTermination(errors.New("peer HTTP 410: DBB_BACKEND_LOST"), true, false)
	if d, f, r := read(); d != d0 || f != f0+1 || r != r0 {
		t.Fatalf("recovery-enabled fatal: direct %d->%d final %d->%d resume %d->%d", d0, d, f0, f, r0, r)
	}
	countTermination(context.DeadlineExceeded, true, true)
	if _, f, r := read(); f != f0+2 || r != r0+1 {
		t.Fatalf("recovery deadline: final %d->%d resume %d->%d", f0, f, r0, r)
	}
	countTermination(errors.New("reset"), false, false)
	if d, _, _ := read(); d != d0+1 {
		t.Fatalf("recovery disabled: direct %d->%d", d0, d)
	}
}

// The recovery window starts at the fault decision, not when recover runs.
func TestRecoveryDeadlineAnchoredAtFault(t *testing.T) {
	m, p := testChooser(t, "http://127.0.0.1:1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fault := time.Now().Add(-1500 * time.Millisecond)
	s := &ingressSession{m: m, p: p, ctx: ctx, cancel: cancel, changed: make(chan struct{}), gate: make(chan struct{}, 1), grace: 5 * time.Second, faultTime: fault}
	s.closed.Store(true)
	s.recover(errors.New("reset"), "auto")
	if !s.deadline.Equal(fault.Add(5 * time.Second)) {
		t.Fatalf("deadline %v, want fault time + grace %v", s.deadline, fault.Add(5*time.Second))
	}
	s.deadlineTimer.Stop()
}

// Early messages buffered during attach release their codec leases when the
// receive loop ends before processing them.
func TestReceiveReleasesUnprocessedEarlyMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var released atomic.Int64
	rel := func() { released.Add(1) }
	g := &generation{ctx: ctx, cancel: cancel, pending: []wireMessage{{kind: "data", release: rel}, {kind: "data", release: rel}, {kind: "data", release: rel}}}
	s := &ingressSession{ctx: ctx}
	s.receive(g)
	if released.Load() != 3 || g.pending != nil {
		t.Fatalf("released %d of 3 early messages", released.Load())
	}
}

func TestBoundSessionsCountOnlyActiveCarriers(t *testing.T) {
	m, p := testChooser(t, "http://127.0.0.1:1")
	ep := m.linkManagers[p.ID].GetEndpoints()[0]
	active := &ingressSession{p: p, g: &generation{endpoint: ep, mode: "ws", active: true}}
	recovering := &ingressSession{p: p, g: &generation{endpoint: ep, mode: "ws"}, deadline: time.Now().Add(time.Minute)}
	m.activeConns.Store(1, &activeConnEntry{session: active})
	m.activeConns.Store(2, &activeConnEntry{session: recovering})
	var b bytes.Buffer
	m.WriteMetrics(&b)
	out := b.String()
	if !strings.Contains(out, `dbb_ingress_bound_sessions{peer="B",endpoint="a",transport="ws",recovery="false"} 1`) {
		t.Fatalf("bound sessions:\n%s", out)
	}
	if !strings.Contains(out, "dbb_ingress_recovering_sessions 1") {
		t.Fatalf("recovering sessions:\n%s", out)
	}
	m.activeConns.Delete(1)
	m.activeConns.Delete(2)
}

var fallbackSample = regexp.MustCompile(`dbb_session_events_total\{role="ingress",event="fallback",reason="transport",from="ws",to="http-sse"\} (\d+)`)

func fallbackEvents() int {
	var b bytes.Buffer
	observability.WriteEventMetrics(&b)
	m := fallbackSample.FindStringSubmatch(b.String())
	if m == nil {
		return 0
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

// The create-time WS->SSE fallback is counted once per created session, not
// per selection pass.
func TestFallbackCountedOncePerCreatedSession(t *testing.T) {
	hook := func(w http.ResponseWriter, r *http.Request, _ http.Handler) bool {
		if r.URL.Path == "/v1/transport/ws" && r.Header.Get(protocol.HeaderWSPurpose) == "check" {
			w.WriteHeader(http.StatusUpgradeRequired)
			return true
		}
		return false
	}
	e, _ := dataplaneSetup(t, "auto", false, nil, nil, hook)
	before := fallbackEvents()
	if _, _, mode, err := e.m.choose(context.Background(), e.m.cfg.Peers[0], "", "auto", true); err != nil || mode != "http-sse" {
		t.Fatalf("selection: mode=%q err=%v", mode, err)
	}
	if got := fallbackEvents(); got != before {
		t.Fatalf("a bare selection counted a fallback: %d -> %d", before, got)
	}
	if err := e.m.Start(); err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp", e.m.listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if err := testEcho2(t, c, "q"); err != nil {
		t.Fatal(err)
	}
	if got := fallbackEvents(); got != before+1 {
		t.Fatalf("fallback events %d -> %d, want +1", before, got)
	}
}

func TestSingleRecoveryCarrierWarning(t *testing.T) {
	no := false
	p := config.PeerConfig{ID: "B", Endpoints: []config.EndpointConfig{{ID: "a"}}, Transport: &config.PeerTransportConfig{Protocol: "ws"}}
	if !config.SingleRecoveryCarrier(p, "auto") {
		t.Fatal("forced ws with one endpoint is a single carrier")
	}
	p.Transport = nil
	if config.SingleRecoveryCarrier(p, "auto") {
		t.Fatal("auto has two carriers per endpoint")
	}
	p.Endpoints = append(p.Endpoints, config.EndpointConfig{ID: "b"})
	p.Failover.Enabled = &no
	if !config.SingleRecoveryCarrier(p, "http-sse") {
		t.Fatal("failover disabled leaves one endpoint")
	}
}
