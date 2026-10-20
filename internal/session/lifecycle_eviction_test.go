package session

import (
	"crypto/sha256"
	"dbbridge/internal/protocol"
	"encoding/hex"
	"io"
	"testing"
	"time"
)

func TestOldWorkerCannotCloseOrSuspendRecovery(t *testing.T) {
	s, backend := testSession(t, true)
	s.SetTransportMode("ws")
	stream, err := s.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ActivateDataPlane(stream); err != nil {
		t.Fatal(err)
	}
	payload := []byte("original backend socket")
	sum := sha256.Sum256(payload)
	if _, _, _, err = s.IngestUpFrame(1, 0, len(payload), hex.EncodeToString(sum[:]), payload); err != nil {
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
			t.Fatal("backend writer did not start")
		}
		time.Sleep(time.Millisecond)
	}
	req := testReq("barrier-recovery")
	req.UpNextOffset = "23"
	result := make(chan error, 1)
	go func() { _, e := s.Resume(req, time.Second); result <- e }()
	for s.GetState() != StateResuming {
		if time.Now().After(until) {
			t.Fatal("resume barrier did not start")
		}
		time.Sleep(time.Millisecond)
	}
	if s.CloseStream(1, stream) {
		t.Fatal("old protocol worker closed the frozen session")
	}
	s.DropTransport(1)
	if s.GetState() != StateResuming {
		t.Fatal("old close callback changed the recovery state")
	}
	got := make([]byte, len(payload))
	if _, err = io.ReadFull(backend, got); err != nil || string(got) != string(payload) {
		t.Fatalf("backend: %q %v", got, err)
	}
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	newStream, err := s.ReserveStream(2, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ActivateDataPlane(newStream); err != nil {
		t.Fatal(err)
	}
	if s.CloseStream(1, stream) {
		t.Fatal("old worker closed epoch 2")
	}
	s.DropTransport(1)
	if _, err = s.NewHeartbeat(1, stream); err == nil {
		t.Fatal("old worker issued a challenge for epoch 2")
	}
	if s.GetState() != StateActive || s.BackendConn == nil {
		t.Fatal("new generation lost original backend")
	}
}

func TestRecoveryRequiresCurrentStreamChallenge(t *testing.T) {
	s, _ := testSession(t, true)
	s.SetTransportMode("ws")
	id, _ := s.ReserveStream(1, "ws")
	s.ActivateDataPlane(id)
	old, err := s.NewHeartbeat(1, id)
	if err != nil {
		t.Fatal(err)
	}
	s.Suspend(time.Second, nil)
	if _, err = s.Resume(testReq("new-challenge"), time.Second); err != nil {
		t.Fatal(err)
	}
	id, _ = s.ReserveStream(2, "ws")
	q := protocol.AckRequest{DownDeliveredOffset: "0", UpConfirmedOffset: "0", ReadyReceived: true, ProbeID: "probe", HeartbeatNonce: old}
	if _, err = s.Ack(2, id, q); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	deadline := s.ResumeDeadline
	s.mu.Unlock()
	if deadline.IsZero() {
		t.Fatal("old proof cleared deadline")
	}
	fresh, err := s.NewHeartbeat(2, id)
	if err != nil || fresh == old {
		t.Fatalf("challenge: %q %v", fresh, err)
	}
	q.ReadyReceived = false
	q.HeartbeatNonce = fresh
	if _, err = s.Ack(2, id, q); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	deadline = s.ResumeDeadline
	s.mu.Unlock()
	if !deadline.IsZero() {
		t.Fatal("new bidirectional proof did not finish recovery")
	}
}
