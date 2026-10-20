package session_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
)

type dummyConn struct {
	net.Conn
	closed bool
}

func (d *dummyConn) SetWriteDeadline(time.Time) error    { return nil }
func (b *blockingConn) SetWriteDeadline(time.Time) error { return nil }
func (d *dummyConn) Close() error {
	d.closed = true
	return nil
}

func (d *dummyConn) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func (d *dummyConn) Read(b []byte) (n int, err error) {
	return 0, nil
}

func shaString(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestSessionLifecycleAndResume(t *testing.T) {
	conn := &dummyConn{}
	s := session.NewSession(
		"peer-a",
		"mysql-target",
		"instance-1",
		conn,
		1024*1024,
		2*1024*1024,
		8,
		64,
		true,
	)

	if s.GetEpoch() != 1 {
		t.Errorf("expected initial epoch 1, got %d", s.GetEpoch())
	}

	// Reserve SSE stream
	streamID, err := s.ReserveSSEStream(1)
	if err != nil || streamID == "" {
		t.Fatalf("ReserveSSEStream failed: %v", err)
	}

	// Activate data plane
	if err := s.ActivateDataPlane(streamID); err != nil {
		t.Fatalf("ActivateDataPlane failed: %v", err)
	}

	if s.GetState() != session.StateActive {
		t.Errorf("expected state ACTIVE, got %s", s.GetState())
	}

	// Append downstream bytes
	downData := []byte("database handshake")
	offset, err := s.AppendDownBytes(downData)
	if err != nil || offset != 0 {
		t.Fatalf("AppendDownBytes failed: offset=%d, err=%v", offset, err)
	}

	// Ingest upstream frame
	upData := []byte("client auth")
	alreadyWritten, accepted, ackUp, err := s.IngestUpFrame(1, 0, len(upData), shaString(upData), upData)
	if err != nil || !accepted {
		t.Fatalf("IngestUpFrame failed: accepted=%v, err=%v", accepted, err)
	}
	_ = alreadyWritten
	_ = ackUp

	// Give writer goroutine a moment to write to dummyConn
	time.Sleep(50 * time.Millisecond)
	if s.UpWrittenOffset() != uint64(len(upData)) {
		t.Errorf("expected UpWrittenOffset %d, got %d", len(upData), s.UpWrittenOffset())
	}

	// Suspend
	timeoutCalled := false
	suspended := s.Suspend(100*time.Millisecond, func(ts *session.Session) {
		timeoutCalled = true
	})
	if !suspended {
		t.Fatalf("expected suspend to return true when resume is enabled")
	}

	if s.GetState() != session.StateSuspended {
		t.Errorf("expected state SUSPENDED, got %s", s.GetState())
	}

	// Resume before timeout
	resumeReq := &protocol.ResumeRequest{
		Transport:           protocol.TransportMode{Mode: "http-sse"},
		ResumeRequestID:     "rr_test123",
		ExpectedEpoch:       "1",
		IngressInstanceID:   "ingress-1",
		KnownUpAckOffset:    strconv.FormatUint(s.UpAckOffset(), 10),
		UpNextOffset:        strconv.FormatUint(s.UpAckOffset(), 10),
		DownDeliveredOffset: "0",
	}

	resumeResp, err := s.Resume(resumeReq, 5*time.Second)
	if err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
	if resumeResp.Epoch != "2" {
		t.Errorf("expected new epoch 2, got %s", resumeResp.Epoch)
	}
	if s.GetState() != session.StateAttaching {
		t.Errorf("expected state ATTACHING after resume, got %s", s.GetState())
	}

	// Activate data plane for Epoch 2 so activation deadline does not fire
	stID, err := s.ReserveSSEStream(2)
	if err != nil {
		t.Fatalf("ReserveSSEStream failed: %v", err)
	}
	if err := s.ActivateDataPlane(stID); err != nil {
		t.Fatalf("ActivateDataPlane failed: %v", err)
	}

	if _, e := s.Ack(2, stID, protocol.AckRequest{DownDeliveredOffset: "0", UpConfirmedOffset: "0", HeartbeatNonce: s.Heartbeat(), ProbeID: "test-probe"}); e != nil {
		t.Fatal(e)
	}
	time.Sleep(120 * time.Millisecond)
	if timeoutCalled {
		t.Errorf("expected timeout callback not to be called after data plane activation")
	}

	s.Close()
	if s.GetState() != session.StateClosed {
		t.Errorf("expected state CLOSED, got %s", s.GetState())
	}
}

func TestSessionResumeDisabled(t *testing.T) {
	conn := &dummyConn{}
	s := session.NewSession(
		"peer-a",
		"mysql-target",
		"instance-1",
		conn,
		1024*1024,
		2*1024*1024,
		8,
		64,
		false, // resume disabled
	)

	if s.ResumeEnabled {
		t.Errorf("expected ResumeEnabled=false")
	}

	suspended := s.Suspend(10*time.Second, func(ts *session.Session) {})
	if suspended {
		t.Errorf("expected suspend to return false when resume is disabled")
	}
	if s.GetState() != session.StateClosed {
		t.Errorf("expected state CLOSED after suspend when resume disabled, got %s", s.GetState())
	}

	resumeReq := &protocol.ResumeRequest{
		Transport:           protocol.TransportMode{Mode: "http-sse"},
		ResumeRequestID:     "rr_test456",
		ExpectedEpoch:       "1",
		IngressInstanceID:   "ingress-1",
		KnownUpAckOffset:    "0",
		UpNextOffset:        "0",
		DownDeliveredOffset: "0",
	}

	_, err := s.Resume(resumeReq, 5*time.Second)
	if !errors.Is(err, session.ErrResumeDisabled) && !errors.Is(err, session.ErrSessionClosed) {
		t.Errorf("expected ErrResumeDisabled or ErrSessionClosed, got %v", err)
	}
}

func TestRegistrySuspendedLRUEviction(t *testing.T) {
	reg := session.NewRegistry(200, 1024*1024)

	s1 := session.NewSession("peer-a", "t1", "inst-1", &dummyConn{}, 1024*1024, 2*1024*1024, 8, 64, true)
	s2 := session.NewSession("peer-a", "t2", "inst-1", &dummyConn{}, 1024*1024, 2*1024*1024, 8, 64, true)

	reg.Register(s1)
	reg.Register(s2)

	// Append 150 bytes to s1 and s2
	_, _ = s1.AppendDownBytes(make([]byte, 150))
	_, _ = s2.AppendDownBytes(make([]byte, 150))

	// Suspend s1 (occupies 150 bytes <= 200)
	s1.Suspend(1*time.Minute, func(*session.Session) {})
	reg.MarkSuspended(s1)

	if reg.Get(s1.ID) == nil {
		t.Fatalf("s1 should not be evicted yet")
	}

	// Suspend s2 (occupies another 150 bytes -> 300 bytes > 200 bytes limit)
	s2.Suspend(1*time.Minute, func(*session.Session) {})
	reg.MarkSuspended(s2)

	// s1 (oldest in LRU) should be evicted
	if reg.Get(s1.ID) != nil {
		t.Errorf("s1 should have been evicted by LRU limit")
	}
	if s1.GetState() != session.StateClosed {
		t.Errorf("evicted session s1 should be CLOSED, got %s", s1.GetState())
	}
	if reg.Get(s2.ID) == nil {
		t.Errorf("s2 should remain in registry")
	}
}

func TestRegistryIdempotencyConflictAndPending(t *testing.T) {
	reg := session.NewRegistry(1024*1024, 10*1024*1024)
	key := "peer-a:inst-1:cr_123"

	limits := protocol.SessionLimits{
		MaxFrameSize:        32768,
		UpMaxInflightFrames: 8,
		UpWindowBytes:       1024 * 1024,
		DownBufferBytes:     2 * 1024 * 1024,
	}
	rec := protocol.RecoveryNegotiation{
		Enabled: true,
		GraceMs: 30000,
	}

	// First attempt reserves
	status, _, _, _ := reg.ReserveCreate(key, "mysql-primary", limits, rec)
	if status != session.CreateReserved {
		t.Fatalf("expected CreateReserved, got %v", status)
	}

	// Concurrent attempt with same key returns Pending
	status2, _, _, _ := reg.ReserveCreate(key, "mysql-primary", limits, rec)
	if status2 != session.CreatePending {
		t.Fatalf("expected CreatePending, got %v", status2)
	}

	// Concurrent attempt with same key but different target returns Conflict
	status3, _, errCode, _ := reg.ReserveCreate(key, "mysql-other", limits, rec)
	if status3 != session.CreateConflict || errCode != protocol.ErrIdempotencyConflict {
		t.Fatalf("expected CreateConflict with DBB_IDEMPOTENCY_CONFLICT, got %v / %s", status3, errCode)
	}

	// Complete first creation
	resp := &protocol.CreateSessionResponse{
		SessionID: "s_123",
	}
	reg.CompleteCreate(key, resp)

	// Subsequent attempt with identical parameters returns CreateDone with cached response
	status4, cached, _, _ := reg.ReserveCreate(key, "mysql-primary", limits, rec)
	if status4 != session.CreateDone || cached == nil || cached.SessionID != "s_123" {
		t.Fatalf("expected CreateDone with cached response, got %v", status4)
	}
}

func TestResumeIdempotencyConflict(t *testing.T) {
	conn := &dummyConn{}
	s := session.NewSession("peer-a", "t1", "inst-1", conn, 1024*1024, 2*1024*1024, 8, 64, true)
	streamID, _ := s.ReserveSSEStream(1)
	_ = s.ActivateDataPlane(streamID)

	resumeReq := &protocol.ResumeRequest{
		Transport:           protocol.TransportMode{Mode: "http-sse"},
		ResumeRequestID:     "rr_001",
		ExpectedEpoch:       "1",
		DownDeliveredOffset: "0",
		KnownUpAckOffset:    "0",
		UpNextOffset:        "0",
	}

	resp, err := s.Resume(resumeReq, 5*time.Second)
	if err != nil || resp == nil {
		t.Fatalf("initial resume failed: %v", err)
	}

	// Replay exact same request should succeed idempotently
	resp2, err2 := s.Resume(resumeReq, 5*time.Second)
	if err2 != nil || resp2 == nil || resp2.Epoch != resp.Epoch {
		t.Fatalf("replay resume should succeed idempotently: %v", err2)
	}

	// Replay with same ResumeRequestID but different DownDeliveredOffset must return ErrIdempotencyConflict
	conflictReq := &protocol.ResumeRequest{
		Transport:           protocol.TransportMode{Mode: "http-sse"},
		ResumeRequestID:     "rr_001",
		ExpectedEpoch:       "1",
		DownDeliveredOffset: "1024",
		KnownUpAckOffset:    "0",
		UpNextOffset:        "0",
	}
	_, errConflict := s.Resume(conflictReq, 5*time.Second)
	if !errors.Is(errConflict, session.ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict on conflicting resume params, got: %v", errConflict)
	}
}

type blockingConn struct {
	net.Conn
	writeBlockCh chan struct{}
	writeDoneCh  chan struct{}
}

func (b *blockingConn) Close() error {
	return nil
}

func (b *blockingConn) Write(buf []byte) (int, error) {
	if b.writeBlockCh != nil {
		<-b.writeBlockCh
	}
	if b.writeDoneCh != nil {
		select {
		case <-b.writeDoneCh:
		default:
			close(b.writeDoneCh)
		}
	}
	return len(buf), nil
}

func TestAuditResumeFencesInflightWrite(t *testing.T) {
	blockCh := make(chan struct{})
	doneCh := make(chan struct{})
	bConn := &blockingConn{
		writeBlockCh: blockCh,
		writeDoneCh:  doneCh,
	}

	s := session.NewSession("peer-a", "t1", "inst-1", bConn, 1024*1024, 2*1024*1024, 8, 64, true)
	streamID, _ := s.ReserveSSEStream(1)
	_ = s.ActivateDataPlane(streamID)

	data := []byte("hello inflight barrier")
	sha := shaString(data)
	_, accepted, _, err := s.IngestUpFrame(1, 0, len(data), sha, data)
	if err != nil || !accepted {
		t.Fatalf("IngestUpFrame failed: %v", err)
	}

	// Give writer a moment to enter Write() and block
	time.Sleep(20 * time.Millisecond)

	resumeDone := make(chan *protocol.ResumeResponse, 1)
	resumeErr := make(chan error, 1)

	// Call Resume while write is blocked in-flight
	go func() {
		resumeReq := &protocol.ResumeRequest{
			Transport:           protocol.TransportMode{Mode: "http-sse"},
			ResumeRequestID:     "rr_barrier_1",
			ExpectedEpoch:       "1",
			IngressInstanceID:   "ingress-1",
			DownDeliveredOffset: "0",
			KnownUpAckOffset:    "0",
			UpNextOffset:        strconv.Itoa(len(data)),
		}
		resp, rErr := s.Resume(resumeReq, 2*time.Second)
		if rErr != nil {
			resumeErr <- rErr
		} else {
			resumeDone <- resp
		}
	}()

	// Verify Resume does not return while write is still blocked
	select {
	case resp := <-resumeDone:
		t.Fatalf("Resume returned before inflight write finished: epoch=%s", resp.Epoch)
	case <-time.After(50 * time.Millisecond):
		// Expected: Resume is blocked on the write barrier!
	}

	// Unblock writer
	close(blockCh)

	select {
	case resp := <-resumeDone:
		if resp.Epoch != "2" {
			t.Errorf("expected epoch 2, got %s", resp.Epoch)
		}
		if resp.UpWrittenOffset != strconv.Itoa(len(data)) {
			t.Errorf("expected UpWrittenOffset %d, got %s", len(data), resp.UpWrittenOffset)
		}
	case err := <-resumeErr:
		t.Fatalf("Resume failed: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatalf("Resume timed out waiting for barrier")
	}
}

func TestAuditSuspendMustNotRefreshDeadline(t *testing.T) {
	conn := &dummyConn{}
	s := session.NewSession("peer-a", "t1", "inst-1", conn, 1024*1024, 2*1024*1024, 8, 64, true)
	streamID, _ := s.ReserveSSEStream(1)
	_ = s.ActivateDataPlane(streamID)

	s.Suspend(500*time.Millisecond, func(*session.Session) {})
	initialDeadline := s.ResumeDeadline

	time.Sleep(50 * time.Millisecond)
	// Second suspend in same outage must NOT refresh deadline
	s.Suspend(500*time.Millisecond, func(*session.Session) {})
	if !s.ResumeDeadline.Equal(initialDeadline) {
		t.Fatalf("ResumeDeadline was refreshed on repeat Suspend: expected %v, got %v", initialDeadline, s.ResumeDeadline)
	}
}

func TestAuditResumeMustRetainActivationDeadline(t *testing.T) {
	conn := &dummyConn{}
	s := session.NewSession("peer-a", "t1", "inst-1", conn, 1024*1024, 2*1024*1024, 8, 64, true)
	streamID, _ := s.ReserveSSEStream(1)
	_ = s.ActivateDataPlane(streamID)

	var timeoutCalled atomic.Bool
	s.Suspend(100*time.Millisecond, func(ts *session.Session) {
		timeoutCalled.Store(true)
	})

	resumeReq := &protocol.ResumeRequest{
		Transport:           protocol.TransportMode{Mode: "http-sse"},
		ResumeRequestID:     "rr_timeout_test",
		ExpectedEpoch:       "1",
		IngressInstanceID:   "ingress-1",
		DownDeliveredOffset: "0",
		KnownUpAckOffset:    "0",
		UpNextOffset:        "0",
	}
	resp, err := s.Resume(resumeReq, 1*time.Second)
	if err != nil || resp == nil {
		t.Fatalf("Resume failed: %v", err)
	}

	// In StateAttaching, without calling ActivateDataPlane, activation deadline must fire
	time.Sleep(150 * time.Millisecond)
	if !timeoutCalled.Load() {
		t.Errorf("expected activation timeout callback to fire when not activated in ATTACHING state")
	}
}

func TestAuditRejectACKBeyondProduced(t *testing.T) {
	conn := &dummyConn{}
	s := session.NewSession("peer-a", "t1", "inst-1", conn, 1024*1024, 2*1024*1024, 8, 64, true)

	// Produce only 3 bytes
	_, err := s.AppendDownBytes([]byte("abc"))
	if err != nil {
		t.Fatalf("AppendDownBytes failed: %v", err)
	}

	// Attempt to ACK offset 100 > 3
	err = s.AdvanceDownAck(100, false)
	if !errors.Is(err, session.ErrBadAck) {
		t.Errorf("expected ErrBadAck on ACK beyond produced, got: %v", err)
	}
	if s.DownAckOffset() != 0 {
		t.Errorf("expected DownAckOffset 0, got %d", s.DownAckOffset())
	}
}

func TestAuditDuplicateInflightFrameIsQueuedAgain(t *testing.T) {
	blockCh := make(chan struct{})
	bConn := &blockingConn{
		writeBlockCh: blockCh,
	}

	s := session.NewSession("peer-a", "t1", "inst-1", bConn, 1024*1024, 2*1024*1024, 8, 64, true)
	streamID, _ := s.ReserveSSEStream(1)
	_ = s.ActivateDataPlane(streamID)

	data := []byte("frame-1")
	sha := shaString(data)

	_, accepted1, _, err1 := s.IngestUpFrame(1, 0, len(data), sha, data)
	if err1 != nil || !accepted1 {
		t.Fatalf("first frame ingest failed: %v", err1)
	}

	// Wait for writer to pick frame and block on write
	time.Sleep(20 * time.Millisecond)

	// Retry identical frame while write is in flight
	alreadyWritten, accepted2, _, err2 := s.IngestUpFrame(1, 0, len(data), sha, data)
	if err2 != nil {
		t.Fatalf("retry during inflight write failed: %v", err2)
	}
	if !accepted2 || alreadyWritten {
		t.Errorf("expected accepted=true, alreadyWritten=false, got accepted=%v alreadyWritten=%v", accepted2, alreadyWritten)
	}

	// Unblock writer
	close(blockCh)
	time.Sleep(30 * time.Millisecond)

	if s.UpWrittenOffset() != uint64(len(data)) {
		t.Errorf("expected UpWrittenOffset %d, got %d", len(data), s.UpWrittenOffset())
	}
}

func TestAuditRegistryGlobalBudgetMustBeEnforced(t *testing.T) {
	// Global buffer budget of 128 bytes
	reg := session.NewRegistry(1024*1024, 128)

	conn := &dummyConn{}
	s := session.NewSession("peer-a", "t1", "inst-1", conn, 1024*1024, 2*1024*1024, 8, 64, true)
	reg.Register(s)

	// Append 100 bytes <= 128 bytes
	_, err := s.AppendDownBytes(make([]byte, 100))
	if err != nil {
		t.Fatalf("expected 100 bytes append to succeed, got %v", err)
	}

	// Append another 100 bytes -> 200 bytes > 128 budget
	_, err2 := s.AppendDownBytes(make([]byte, 100))
	if !errors.Is(err2, session.ErrBufferExhausted) {
		t.Fatalf("expected ErrBufferExhausted when exceeding global buffer limit, got %v", err2)
	}
}
