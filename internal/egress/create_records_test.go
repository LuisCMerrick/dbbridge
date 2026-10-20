package egress

import (
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"dbbridge/internal/session"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"
)

// Two concurrent creates with the same create_request_id: the request that
// was admitted to the per-peer tracker first is not the one that reserved
// the table record. The failed record the winner leaves must stay counted.
func TestConcurrentSameKeyCreatesKeepFailedRecordCounted(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "X", Tokens: []string{"tokX"}, AllowedTargets: []string{"dead"}, MaxSessions: 4}})
	cfg.Targets["dead"] = config.TargetConfig{Network: "tcp", Address: deadAddr(t)}
	s, ts := egrServer(t, cfg)
	key := `["X","ai","dup"]`
	lim := protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 2048, DownBufferBytes: 4096}
	a := &createLease{s: s, peer: "X", target: "dead", key: key, metadataCost: s.sessionMetadataCost()}
	b := &createLease{s: s, peer: "X", target: "dead", key: key, metadataCost: s.sessionMetadataCost()}
	if st, _, msg := a.acquire(); st != 0 {
		t.Fatalf("a acquire %d %s", st, msg)
	}
	if st, _, msg := b.acquire(); st != 0 {
		t.Fatalf("b acquire %d %s", st, msg)
	}
	if st, _, _, _ := s.registry.ReserveCreate(key, "dead", lim, protocol.RecoveryNegotiation{}, "http-sse"); st != session.CreateReserved {
		t.Fatalf("b reserve: %v", st)
	}
	b.reserve()
	if st, _, _, _ := s.registry.ReserveCreate(key, "dead", lim, protocol.RecoveryNegotiation{}, "http-sse"); st != session.CreatePending {
		t.Fatalf("a reserve: %v", st)
	}
	a.rollback() // a answered 202 pending
	if n := s.idem.count("X"); n != 1 {
		t.Fatalf("the loser's rollback dropped the winner's record: tracked %d", n)
	}
	b.fail(httptest.NewRecorder(), 502, protocol.ErrDialFailed)
	b.rollback()
	if n := s.idem.count("X"); n != 1 {
		t.Fatalf("failed record retained by the registry but tracked %d times", n)
	}
	if st, er, _ := egrCreate(t, ts.URL, "tokX", "dup", "dead"); st != 502 || er.Code != protocol.ErrDialFailed {
		t.Fatalf("replay: %d %+v", st, er)
	}
	if n := s.idem.count("X"); n != 1 {
		t.Fatalf("replay changed the tracked records: %d", n)
	}
}

// Records of completed creates outlive their sessions for the tombstone TTL.
// One peer's create/DELETE churn is bounded by its share of the create record
// budget, so another peer can still create.
func TestCreateCloseChurnDoesNotStarveOtherPeers(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{
		{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 2},
		{ID: "B", Tokens: []string{"tokB"}, AllowedTargets: []string{"db"}, MaxSessions: 2},
	})
	cfg.Resources = resource.NewManager(256<<20, 32<<20, 1<<20, 8) // Metadata 512 KiB
	s, ts := egrServer(t, cfg)
	cycles := 0
	for ; cycles < 2000; cycles++ {
		st, er, cr := egrCreate(t, ts.URL, "tokA", fmt.Sprintf("c-%d", cycles), "db")
		if st != 201 {
			if st != 429 || er.Message != "peer idempotency quota" || !er.Retryable {
				t.Fatalf("churn refused with %d %+v", st, er)
			}
			break
		}
		egrDelete(t, ts.URL, "tokA", cr)
	}
	if cycles == 2000 {
		t.Fatal("create/DELETE churn was never bounded")
	}
	if got, limit := s.idem.count("A"), s.idemLimits("A").total; got != limit {
		t.Fatalf("peer A charged %d records, limit %d", got, limit)
	}
	if st, er, _ := egrCreate(t, ts.URL, "tokB", "victim", "db"); st != 201 {
		t.Fatalf("peer B create after A's churn: %d %+v", st, er)
	}
	if used, limit := s.records.Used(), s.records.Limit(); used > limit {
		t.Fatalf("records hold %d of a %d byte budget", used, limit)
	}
}

// The create record budget leaves every egress session slot its session
// metadata and never takes the local ingress reserve.
func TestCreateRecordBudgetLeavesSessionsAndIngressReserve(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 2}})
	cfg.Listeners = []config.ListenerConfig{{ID: "l1", Listen: "127.0.0.1:0", Peer: "A"}}
	s, _ := egrServer(t, cfg)
	res := cfg.Resources
	sessFloor, metaFloor := res.IngressReserve()
	if metaFloor == 0 {
		t.Fatal("setup: no ingress reserve")
	}
	slots := res.Sessions.Limit() - sessFloor
	if got, want := s.records.Limit(), res.Metadata.Limit()-metaFloor-slots*s.sessionMetadataCost(); got != want {
		t.Fatalf("record budget %d, want %d", got, want)
	}
	if !s.records.Try(s.records.Limit()) {
		t.Fatal("record budget not available")
	}
	defer s.records.Put(s.records.Limit())
	for i := int64(0); i < slots; i++ {
		if !res.TryEgressMetadata(s.sessionMetadataCost()) {
			t.Fatalf("session %d metadata refused with full records", i)
		}
		defer res.Metadata.Put(s.sessionMetadataCost())
	}
	if !res.Metadata.Try(metaFloor) {
		t.Fatal("local ingress reserve taken")
	}
	res.Metadata.Put(metaFloor)
}

// A retry of a failed create is answered with the cached failure even when
// the peer has no session quota left.
func TestReplayOfFailedCreateAnsweredFromTable(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "X", Tokens: []string{"tokX"}, AllowedTargets: []string{"dead", "db"}, MaxSessions: 1}})
	cfg.Targets["dead"] = config.TargetConfig{Network: "tcp", Address: deadAddr(t)}
	_, ts := egrServer(t, cfg)
	if st, _, _ := egrCreate(t, ts.URL, "tokX", "K", "dead"); st != 502 {
		t.Fatalf("first create: %d", st)
	}
	if st, _, _ := egrCreate(t, ts.URL, "tokX", "L", "db"); st != 201 {
		t.Fatalf("live session: %d", st)
	}
	if st, er, _ := egrCreate(t, ts.URL, "tokX", "K", "dead"); st != 502 || er.Code != protocol.ErrDialFailed {
		t.Fatalf("replay of a cached failure: %d %+v", st, er)
	}
	if st, er, _ := egrCreate(t, ts.URL, "tokX", "K", "db"); st != 409 || er.Code != protocol.ErrIdempotencyConflict {
		t.Fatalf("replay with other parameters: %d %+v", st, er)
	}
}

// A retry of a create that is still dialing is answered 202 pending even when
// the in-flight create holds the peer's only session slot.
func TestReplayOfPendingCreateAnsweredFromTable(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "X", Tokens: []string{"tokX"}, AllowedTargets: []string{"db"}, MaxSessions: 1}})
	s, ts := egrServer(t, cfg)
	key := `["X","ai","P"]`
	lim := protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 2048, DownBufferBytes: 4096}
	l := &createLease{s: s, peer: "X", target: "db", key: key, metadataCost: s.sessionMetadataCost()}
	defer l.rollback()
	if st, _, msg := l.acquire(); st != 0 {
		t.Fatalf("acquire %d %s", st, msg)
	}
	if st, _, _, _ := s.registry.ReserveCreate(key, "db", lim, protocol.RecoveryNegotiation{}, "http-sse"); st != session.CreateReserved {
		t.Fatalf("reserve: %v", st)
	}
	l.reserve()
	st, er, _ := egrCreate(t, ts.URL, "tokX", "P", "db")
	if st != 202 || er.Code != protocol.ErrCreatePending || !er.Retryable {
		t.Fatalf("retry of a pending create: %d %+v", st, er)
	}
}

// A target name that does not resolve is a link failure (502, counted as a
// dial failure), not an authorization verdict.
func TestUnresolvableTargetIsDialFailure(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	cfg.Targets = map[string]config.TargetConfig{"db": {Network: "tcp", Address: "no-such-host.invalid:3306"}}
	cfg.Transport.ControlRequestTimeout = 10 * time.Second
	_, ts := egrServer(t, cfg)
	st, er, _ := egrCreate(t, ts.URL, "tokA", "d1", "db")
	if st != 502 || er.Code != protocol.ErrDialFailed {
		t.Fatalf("unresolvable target: %d %+v", st, er)
	}
	if st, er, _ := egrCreate(t, ts.URL, "tokA", "d1", "db"); st != 502 || er.Code != protocol.ErrDialFailed {
		t.Fatalf("replay: %d %+v", st, er)
	}
}

// A resolved target whose addresses allowed_cidrs excludes stays forbidden.
func TestTargetOutsideAllowedCIDRsIsForbidden(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "A", Tokens: []string{"tokA"}, AllowedTargets: []string{"db"}, MaxSessions: 5}})
	cfg.Targets = map[string]config.TargetConfig{"db": {Network: "tcp", Address: "127.0.0.1:3306", AllowedCIDRs: []string{"10.0.0.0/8"}}}
	_, ts := egrServer(t, cfg)
	if st, er, _ := egrCreate(t, ts.URL, "tokA", "c1", "db"); st != 403 || er.Code != protocol.ErrTargetForbidden {
		t.Fatalf("target outside allowed_cidrs: %d %+v", st, er)
	}
}

// A create whose dial is cancelled by Close answers 503 DRAINING.
func TestCreateCancelledByCloseIsDraining(t *testing.T) {
	cfg := testCfg(t, []config.InboundPeerConfig{{ID: "X", Tokens: []string{"tokX"}, AllowedTargets: []string{"slow"}, MaxSessions: 1}})
	cfg.Targets["slow"] = config.TargetConfig{Network: "tcp", Address: "10.255.255.1:3306"}
	cfg.Transport.DialTimeout = 5 * time.Second
	cfg.Transport.ControlRequestTimeout = 10 * time.Second
	s, ts := egrServer(t, cfg)
	type result struct {
		st int
		er protocol.ErrorResponse
	}
	done := make(chan result, 1)
	go func() { st, er, _ := egrCreate(t, ts.URL, "tokX", "P", "slow"); done <- result{st, er} }()
	time.Sleep(300 * time.Millisecond)
	select {
	case r := <-done:
		t.Skipf("blackhole address answered before Close (%d); no route in this sandbox", r.st)
	default:
	}
	go s.Close()
	select {
	case r := <-done:
		if r.st != 503 || r.er.Code != protocol.ErrDraining || !r.er.Retryable {
			t.Fatalf("create cancelled by Close: %d %+v", r.st, r.er)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("create not cancelled by Close")
	}
}
