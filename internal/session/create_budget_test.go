package session

import (
	"fmt"
	"testing"
	"time"

	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
)

// When metadata is exhausted the NEW key is rejected;
// normally closed records inside the guarantee are neither evicted nor reused.
func TestFullBudgetRejectsNewKeepsGuaranteed(t *testing.T) {
	metadata := resource.New(4 * createRecordCost)
	r := NewRegistry(1<<20, 4<<20)
	r.SetResources(nil, metadata, 10*time.Minute)
	defer r.CloseAll()
	var keys []string
	for i := 0; ; i++ {
		key := fmt.Sprintf("k%d", i)
		st, _, code, _ := r.ReserveCreate(key, "db", protocol.SessionLimits{}, protocol.RecoveryNegotiation{}, "ws")
		if st != CreateReserved {
			if st != CreateFailed || code != protocol.ErrQuotaExceeded || len(keys) == 0 {
				t.Fatalf("i=%d status=%v code=%s", i, st, code)
			}
			break
		}
		s := NewSession("p", "db", "i", afSink{}, 1024, 1024, 8, 64, false)
		r.Register(s)
		r.CompleteCreate(key, &protocol.CreateSessionResponse{SessionID: s.ID})
		s.CloseWithReason("closed_normal")
		keys = append(keys, key)
	}
	for _, key := range keys {
		st, resp, _, _ := r.ReserveCreate(key, "db", protocol.SessionLimits{}, protocol.RecoveryNegotiation{}, "ws")
		if st != CreateDone || resp == nil || r.GetAny(resp.SessionID) == nil {
			t.Fatalf("%s inside the guarantee: status=%v resp=%v", key, st, resp)
		}
	}
}

// A create completed after its session already ended keeps the record for the
// full guarantee counted from the end, matching the tombstone.
func TestCompleteAfterCloseKeepsGuarantee(t *testing.T) {
	r := NewRegistry(1<<20, 4<<20)
	r.SetResources(nil, nil, 10*time.Minute)
	defer r.CloseAll()
	if st, _, _, _ := r.ReserveCreate("late", "db", protocol.SessionLimits{}, protocol.RecoveryNegotiation{}, "ws"); st != CreateReserved {
		t.Fatal(st)
	}
	r.mu.Lock()
	r.createIdempotency["late"].CreatedAt = time.Now().Add(-9 * time.Minute)
	r.mu.Unlock()
	s := NewSession("p", "db", "i", afSink{}, 1024, 1024, 8, 64, false)
	r.Register(s)
	closed := time.Now()
	s.Close()
	r.CompleteCreate("late", &protocol.CreateSessionResponse{SessionID: s.ID})
	r.mu.Lock()
	until := r.createIdempotency["late"].retainUntil
	r.mu.Unlock()
	if until.Before(closed.Add(10 * time.Minute)) {
		t.Fatalf("record retained %v after close; guarantee 10m", until.Sub(closed))
	}
}

// The timer closes an unattached session on its own
// (no callback needed), and a slot reserved in time cannot be activated by a
// ready that arrives after the absolute deadline.
func TestInitialAttachDeadlineFencesReadyAndCloses(t *testing.T) {
	s, _ := contractSession(t)
	s.mu.Lock()
	s.attachDeadline, s.attachEpoch = time.Now().Add(-time.Millisecond), s.Epoch
	s.mu.Unlock()
	if _, err := s.ReserveStream(1, "ws"); err != ErrSessionClosed {
		t.Fatalf("reserve after deadline: %v", err)
	}

	s2, _ := contractSession(t)
	s2.StartUnattachedTimer(40*time.Millisecond, nil)
	id, err := s2.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	s2.mu.Lock() // hold the lock across the deadline: ready and timer race here
	time.Sleep(60 * time.Millisecond)
	err = s2.activateLocked(1, id)
	s2.mu.Unlock()
	if err == nil {
		t.Fatal("ready after the initial attach deadline activated the session")
	}
	afWait(t, "timer close", func() bool { return s2.GetState() == StateClosed })

	s3, _ := contractSession(t)
	s3.StartUnattachedTimer(30*time.Millisecond, nil)
	id, err = s3.ReserveStream(1, "ws")
	if err == nil {
		err = s3.ActivateDataPlane(id)
	}
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	if s3.GetState() != StateActive {
		t.Fatalf("activated session closed by the unattached timer: %s", s3.GetState())
	}
}
