package session

import (
	"crypto/sha256"
	"dbbridge/internal/protocol"
	"encoding/hex"
	"errors"
	"testing"
	"time"
)

func TestBoundaryResumeAttachingBypassesSuspendedQuota(t *testing.T) {
	s, _ := auditSession(t, true)
	r := NewRegistry(1, 8192)
	r.Register(s)
	s.SetTransportMode("ws")
	id, _ := s.ReserveStream(1, "ws")
	if err := s.ActivateDataPlane(id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendDownBytes([]byte("two bytes")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Resume(auditReq("active-resume"), 50*time.Millisecond); err != nil && s.GetState() != StateClosed {
		t.Fatal(err)
	}
	r.UpdateSuspended()
	t.Logf("state=%s retained=%d suspended_limit=%d tracked=%d", s.GetState(), s.BufferSize(), r.suspendedBufferLimit, r.retention.Used())
	if s.GetState() != StateClosed {
		t.Fatal("recovery ATTACHING retained data above suspended quota without eviction")
	}
}

func TestBoundaryInflightResumeConflictIsChecked(t *testing.T) {
	s, _ := auditSession(t, true)
	s.SetTransportMode("ws")
	id, _ := s.ReserveStream(1, "ws")
	s.ActivateDataPlane(id)
	sum := sha256.Sum256([]byte("x"))
	if _, _, _, err := s.IngestUpFrame(1, 0, 1, hex.EncodeToString(sum[:]), []byte("x")); err != nil {
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
			t.Fatal("writer not started")
		}
		time.Sleep(time.Millisecond)
	}
	req := auditReq("same-id")
	req.UpNextOffset = "1"
	done := make(chan struct{})
	go func() { defer close(done); s.Resume(req, 500*time.Millisecond) }()
	for s.GetState() != StateResuming {
		if time.Now().After(until) {
			t.Fatal("resume not started")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := s.Resume(req, 50*time.Millisecond); !errors.Is(err, ErrResumePending) {
		t.Errorf("same in-flight request must be pending, got %v", err)
	}
	competing := *req
	competing.ResumeRequestID = "other-id"
	if _, err := s.Resume(&competing, 50*time.Millisecond); !errors.Is(err, ErrResumeInProgress) {
		t.Errorf("competing request must be in progress, got %v", err)
	}
	changed := *req
	changed.Transport.Mode = "http-sse"
	_, err := s.Resume(&changed, 50*time.Millisecond)
	s.Close()
	<-done
	t.Logf("same ID, changed transport returned %v", err)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
}

func TestBoundaryTombstoneExpiresWithoutNewCreate(t *testing.T) {
	s, _ := auditSession(t, false)
	r := NewRegistry(1024, 8192)
	r.ttl = 20 * time.Millisecond
	r.Register(s)
	lim := protocol.SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 1024, DownBufferBytes: 1024}
	r.ReserveCreate("key", "db", lim, protocol.RecoveryNegotiation{}, "ws")
	r.CompleteCreate("key", &protocol.CreateSessionResponse{SessionID: s.ID, SessionToken: s.SessionToken, Limits: lim})
	s.Close()
	time.Sleep(60 * time.Millisecond)
	if r.GetAny(s.ID) != nil {
		t.Fatal("expired tombstone is still returned; expiry only runs when another creation is reserved")
	}
}
