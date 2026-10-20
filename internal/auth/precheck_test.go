package auth

import (
	"dbbridge/internal/config"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPrecheckRateBurstAndIdempotentRelease(t *testing.T) {
	q := newPrecheckQuota()
	now := q.updated
	for i := 0; i < precheckBurst; i++ {
		release, ok := q.reserve(now)
		if !ok {
			t.Fatalf("request %d rejected inside burst", i)
		}
		release()
		release()
	}
	if _, ok := q.reserve(now); ok {
		t.Fatal("unbounded rate after releasing connections")
	}
	release, ok := q.reserve(now.Add(time.Second / prechecksPerSecond))
	if !ok {
		t.Fatal("rate allowance did not refill")
	}
	release()
	if q.active != 0 {
		t.Fatal("duplicate release corrupted concurrency accounting")
	}
}

func TestPrecheckConcurrentAdmission(t *testing.T) {
	q := newPrecheckQuota()
	now := q.updated
	var admitted atomic.Int64
	var wg sync.WaitGroup
	var held sync.WaitGroup
	blocked := make(chan struct{})
	for i := 0; i < 24; i++ {
		wg.Add(1)
		held.Add(1)
		go func() {
			defer wg.Done()
			release, ok := q.reserve(now)
			if ok {
				admitted.Add(1)
			}
			held.Done()
			if ok {
				<-blocked
				release()
			}
		}()
	}
	held.Wait()
	close(blocked)
	wg.Wait()
	if got := admitted.Load(); got != precheckConcurrency {
		t.Fatalf("admitted %d simultaneous connections", got)
	}
}

func TestPrecheckIdentityIsolationAndRotation(t *testing.T) {
	f := &config.AuthConfigFile{Version: 1, InboundPeers: []config.InboundPeerConfig{
		{ID: "a", Tokens: []string{"one"}, AllowedTargets: []string{"db"}},
		{ID: "b", Tokens: []string{"two"}, AllowedTargets: []string{"db"}},
	}}
	a := NewAuthenticator(&config.Config{Auth: config.AuthConfig{InboundPeers: f.InboundPeers}})
	defer a.Close()
	for i := 0; i < precheckConcurrency; i++ {
		release, ok := a.ReservePrecheck("a")
		if !ok {
			t.Fatal("initial precheck rejected")
		}
		defer release()
	}
	f.InboundPeers[0].Tokens = []string{"rotated"}
	a.install(f, nil, false)
	if _, ok := a.ReservePrecheck("a"); ok {
		t.Fatal("credential rotation bypassed existing reservations")
	}
	release, ok := a.ReservePrecheck("b")
	if !ok {
		t.Fatal("one identity exhausted another identity's allowance")
	}
	release()
	if _, ok := a.ReservePrecheck("unknown"); ok {
		t.Fatal("unknown identity admitted")
	}
	f.InboundPeers[1].Enabled = config.BoolPtr(false)
	a.install(f, nil, false)
	if _, ok := a.ReservePrecheck("b"); ok {
		t.Fatal("disabled identity admitted")
	}
}
