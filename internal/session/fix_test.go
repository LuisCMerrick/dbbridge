package session

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"dbbridge/internal/protocol"
)

func fixActive(t *testing.T) (*Session, string) {
	t.Helper()
	s, _ := auditSession(t, true)
	id, err := s.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ActivateDataPlane(id); err != nil {
		t.Fatal(err)
	}
	if _, err = s.AppendDownBytes([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	return s, id
}

// H1: Resume at the end of an in-flight emit must leave the new epoch's ready
// ACK (which repeats down_delivered_offset) valid.
func TestFixResumeAfterInflightEmitReady(t *testing.T) {
	s, _ := fixActive(t)
	if err := s.BeginEmit(1, 0, 3); err != nil {
		t.Fatal(err)
	}
	s.DropTransport(1)
	req := auditReq("r1")
	req.DownDeliveredOffset = "3"
	if _, err := s.Resume(req, 0); err != nil {
		t.Fatalf("resume: %v", err)
	}
	id2, err := s.ReserveStream(2, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Ack(2, id2, protocol.AckRequest{ReadyReceived: true, DownDeliveredOffset: "3", UpConfirmedOffset: "0"}); err != nil {
		t.Fatalf("ready ack after resume: %v", err)
	}
	if !s.IsDataPlaneActive() {
		t.Fatal("data plane not active after ready ACK")
	}
	// The new epoch keeps working: produce, emit and ACK the next frame.
	if _, err = s.AppendDownBytes([]byte("def")); err != nil {
		t.Fatal(err)
	}
	if err = s.BeginEmit(2, 3, 6); err != nil {
		t.Fatal(err)
	}
	s.FinishEmit(2, 6, nil)
	if _, err = s.Ack(2, id2, protocol.AckRequest{DownDeliveredOffset: "6", UpConfirmedOffset: "0"}); err != nil {
		t.Fatalf("ack in new epoch: %v", err)
	}
	if got := s.DownAckOffset(); got != 6 {
		t.Fatalf("down ack offset = %d, want 6", got)
	}
}

// L2 (same fix as H1): bytes emitted in the previous epoch cannot be ACKed in
// the new epoch before they are re-emitted.
func TestFixResumeResetsDownEmitted(t *testing.T) {
	s, _ := fixActive(t)
	if err := s.BeginEmit(1, 0, 3); err != nil {
		t.Fatal(err)
	}
	s.FinishEmit(1, 3, nil)
	s.DropTransport(1)
	if _, err := s.Resume(auditReq("r1"), 0); err != nil {
		t.Fatal(err)
	}
	id2, err := s.ReserveStream(2, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Ack(2, id2, protocol.AckRequest{ReadyReceived: true, DownDeliveredOffset: "0", UpConfirmedOffset: "0"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Ack(2, id2, protocol.AckRequest{DownDeliveredOffset: "3", UpConfirmedOffset: "0"}); !errors.Is(err, ErrBadAck) {
		t.Fatalf("ACK of bytes not re-emitted in this epoch: got %v, want ErrBadAck", err)
	}
}

// M1: a peer ACK of the frame end parked during the emit counts even when the
// local write reports an error, so a later resume at that offset succeeds.
func TestFixPendingAckSurvivesWriteError(t *testing.T) {
	s, id := fixActive(t)
	if err := s.BeginEmit(1, 0, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ack(1, id, protocol.AckRequest{DownDeliveredOffset: "3", UpConfirmedOffset: "0"}); err != nil {
		t.Fatal(err)
	}
	s.FinishEmit(1, 3, errors.New("write failed after delivery"))
	if got := s.DownAckOffset(); got != 3 {
		t.Fatalf("down ack offset = %d, want 3", got)
	}
	s.DropTransport(1)
	req := auditReq("r1")
	req.DownDeliveredOffset = "3"
	if _, err := s.Resume(req, 0); err != nil {
		t.Fatalf("resume: %v (state=%s)", err, s.GetState())
	}
}

// M1 negative: without a peer ACK a failed write still does not count.
func TestFixWriteErrorWithoutAckNotEmitted(t *testing.T) {
	s, _ := fixActive(t)
	if err := s.BeginEmit(1, 0, 3); err != nil {
		t.Fatal(err)
	}
	s.FinishEmit(1, 3, errors.New("write failed"))
	s.mu.Lock()
	emitted := s.downEmitted
	s.mu.Unlock()
	if emitted != 0 {
		t.Fatalf("downEmitted = %d after failed unacknowledged write, want 0", emitted)
	}
}

// M2: a full idempotency table is a retryable quota error, not DBB_DRAINING,
// and expired entries leave the table through the expiry heap.
func TestFixCreateIdempotencyCapQuota(t *testing.T) {
	r := NewRegistry(1<<20, 1<<20)
	started := time.Now()
	for i := 0; i < maxCreateIdempotencyEntries; i++ {
		k := fmt.Sprintf("k%d", i)
		st, _, _, _ := r.ReserveCreate(k, "db", protocol.SessionLimits{}, protocol.RecoveryNegotiation{})
		if st != CreateReserved {
			t.Fatalf("i=%d st=%v", i, st)
		}
		r.FailCreate(k, protocol.ErrDialFailed, "x")
	}
	t.Logf("%d creates took %v", maxCreateIdempotencyEntries, time.Since(started))
	st, _, code, _ := r.ReserveCreate("new", "db", protocol.SessionLimits{}, protocol.RecoveryNegotiation{})
	if st != CreateFailed || code != protocol.ErrQuotaExceeded {
		t.Fatalf("full table: status=%v code=%s, want CreateFailed %s", st, code, protocol.ErrQuotaExceeded)
	}
	// Existing keys still answer from the cache.
	if st, _, code, _ = r.ReserveCreate("k1", "db", protocol.SessionLimits{}, protocol.RecoveryNegotiation{}); st != CreateFailed || code != protocol.ErrDialFailed {
		t.Fatalf("cached key: status=%v code=%s", st, code)
	}
	r.mu.Lock()
	if len(r.expiry) > 2*maxCreateIdempotencyEntries {
		t.Errorf("expiry heap grew to %d", len(r.expiry))
	}
	r.mu.Unlock()
	r.CloseAll()
}

func TestFixCreateIdempotencyExpiresViaHeap(t *testing.T) {
	r := NewRegistry(1<<20, 1<<20)
	r.ttl = 20 * time.Millisecond
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("k%d", i)
		r.ReserveCreate(k, "db", protocol.SessionLimits{}, protocol.RecoveryNegotiation{})
		r.FailCreate(k, protocol.ErrDialFailed, "x")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		r.mu.Lock()
		n, h := len(r.createIdempotency), len(r.expiry)
		r.mu.Unlock()
		if n == 0 && h == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("entries=%d heap=%d not expired", n, h)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if st, _, _, _ := r.ReserveCreate("k1", "db", protocol.SessionLimits{}, protocol.RecoveryNegotiation{}); st != CreateReserved {
		t.Fatalf("expired key not reusable: %v", st)
	}
}

func fixBlockedWriter(t *testing.T) (*Session, io.Reader) {
	t.Helper()
	s, backend := auditSession(t, true)
	id, err := s.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ActivateDataPlane(id); err != nil {
		t.Fatal(err)
	}
	p := []byte("slow")
	sum := sha256.Sum256(p)
	if _, _, _, err = s.IngestUpFrame(1, 0, len(p), hex.EncodeToString(sum[:]), p); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		writing := s.inflightWriting
		s.mu.Unlock()
		if writing {
			return s, backend
		}
		if time.Now().After(until) {
			t.Fatal("writer not started")
		}
		time.Sleep(time.Millisecond)
	}
}

// M3 (corrected for DBB-AUD-04; 5.8 B(8), 10.2.2, FD-07): a backend write
// that outlives barrier_timeout fails the barrier. The session and its backend
// socket close at once and the resume returns a terminal error; a retry with
// the same request ID never commits a new epoch after the write unblocks.
func TestFixResumeBarrierSlowWriteTerminates(t *testing.T) {
	s, backend := fixBlockedWriter(t)
	req := auditReq("slow")
	req.UpNextOffset = "4"
	started := time.Now()
	_, err := s.Resume(req, 30*time.Millisecond)
	if !errors.Is(err, ErrBarrierTimeout) || !errors.Is(err, ErrSessionClosed) || errors.Is(err, ErrResumePending) {
		t.Fatalf("barrier timeout: got %v, want terminal ErrBarrierTimeout", err)
	}
	if el := time.Since(started); el > time.Second {
		t.Fatalf("barrier failure took %v", el)
	}
	if st := s.GetState(); st != StateClosed {
		t.Fatalf("state=%s after barrier timeout, want CLOSED", st)
	}
	select {
	case <-s.Done():
	default:
		t.Fatal("session done not signalled")
	}
	// The original backend socket is closed: the peer sees EOF/closed pipe.
	_, _ = io.ReadAll(backend)
	time.Sleep(20 * time.Millisecond)
	if _, err := s.Resume(req, 30*time.Millisecond); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("retry after barrier failure: got %v, want ErrSessionClosed", err)
	}
	if st, ep := s.GetState(), s.GetEpoch(); st != StateClosed || ep != 1 {
		t.Fatalf("state=%s epoch=%d after retry, want CLOSED/1", st, ep)
	}
}

// M3/L8 (corrected for DBB-AUD-04): when the barrier is clamped by the
// remaining grace and the write still does not finish, the resume fails at
// the grace deadline with DBB_RESUME_EXPIRED, not RESUME_PENDING.
func TestFixResumeBarrierGraceExpiryReason(t *testing.T) {
	s, _ := fixBlockedWriter(t)
	s.Configure(150*time.Millisecond, 0, "", nil)
	req := auditReq("slow")
	req.UpNextOffset = "4"
	if _, err := s.Resume(req, time.Second); !errors.Is(err, ErrResumeExpired) {
		t.Fatalf("got %v, want ErrResumeExpired", err)
	}
	select {
	case <-s.Done():
	default:
		t.Fatal("session not closed at grace deadline")
	}
	if code := s.TerminalCode(); code != protocol.ErrResumeExpired {
		t.Fatalf("terminal code %q, want %s", code, protocol.ErrResumeExpired)
	}
}

// L8: a resume rejected for bad offsets records DBB_BAD_ACK.
func TestFixResumeBadOffsetRecordsReason(t *testing.T) {
	s, _ := fixActive(t)
	s.DropTransport(1)
	req := auditReq("bad")
	req.DownDeliveredOffset = "2" // not a frame boundary
	if _, err := s.Resume(req, 0); !errors.Is(err, ErrBadAck) {
		t.Fatalf("got %v, want ErrBadAck", err)
	}
	if code := s.TerminalCode(); code != protocol.ErrBadAck {
		t.Fatalf("terminal code %q, want %s", code, protocol.ErrBadAck)
	}
}
