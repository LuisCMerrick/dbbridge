package session

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"testing"
	"time"

	"dbbridge/internal/protocol"
)

func contractSession(t *testing.T) (*Session, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	s := NewSession("A", "db", "bi", a, 4096, 2048, 8, 64, true)
	s.Configure(2*time.Second, 5*time.Second, "ai", nil)
	s.SetTransportMode("ws")
	t.Cleanup(func() { s.Close(); b.Close() })
	return s, b
}

func TestBarrierTimeoutMustTerminate(t *testing.T) {
	s, _ := contractSession(t)
	id, err := s.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ActivateDataPlane(id); err != nil {
		t.Fatal(err)
	}
	b := []byte("slow")
	d := sha256.Sum256(b)
	if _, _, _, err = s.IngestUpFrame(1, 0, len(b), hex.EncodeToString(d[:]), b); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		writing := s.inflightWriting
		s.mu.Unlock()
		if writing {
			break
		}
		if time.Now().After(until) {
			t.Fatal("fixture: writer did not start")
		}
		time.Sleep(time.Millisecond)
	}
	q := &protocol.ResumeRequest{ResumeRequestID: "test-credential", ExpectedEpoch: "1", IngressInstanceID: "ai", Transport: protocol.TransportMode{Mode: "ws"}, KnownUpAckOffset: "0", UpNextOffset: "4", DownDeliveredOffset: "0"}
	_, err = s.Resume(q, 20*time.Millisecond)
	if s.GetState() != StateClosed || errors.Is(err, ErrResumePending) {
		t.Fatalf("contract violation: after 20ms barrier timeout state=%s error=%v; required CLOSED", s.GetState(), err)
	}
}

func TestNormalTombstoneMustRetainTenMinutes(t *testing.T) {
	s, _ := contractSession(t)
	r := NewRegistry(1<<20, 4<<20)
	r.SetResources(nil, nil, 10*time.Minute)
	defer r.CloseAll()
	if st, _, _, _ := r.ReserveCreate("test-credential", "db", protocol.SessionLimits{}, protocol.RecoveryNegotiation{}, "ws"); st != CreateReserved {
		t.Fatal(st)
	}
	r.Register(s)
	r.CompleteCreate("test-credential", &protocol.CreateSessionResponse{SessionID: s.ID})
	started := time.Now()
	s.CloseWithReason("closed_normal")
	r.mu.Lock()
	expiry := r.tombstoneExpires[s.ID]
	r.mu.Unlock()
	if expiry.Before(started.Add(10 * time.Minute)) {
		t.Fatalf("contract violation: normal tombstone retained for %v; configured guarantee=10m", expiry.Sub(started))
	}
}

// Hold the timer callback after it has observed expiry, emulating descheduling
// between its state check and Close. No production code is changed.
func TestUnattachedExpiryMustFenceActivation(t *testing.T) {
	s, _ := contractSession(t)
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	s.StartUnattachedTimer(20*time.Millisecond, func(x *Session) { close(entered); <-release; x.Close(); close(finished) })
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("fixture: timer did not fire")
	}
	id, err := s.ReserveStream(1, "ws")
	if err == nil {
		err = s.ActivateDataPlane(id)
	}
	state := s.GetState()
	close(release)
	<-finished
	if err == nil {
		t.Fatalf("contract violation: expired initial attach activated as %s before stale timeout callback closed it", state)
	}
}
