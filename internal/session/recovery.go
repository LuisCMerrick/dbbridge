package session

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
)

// LastResumeRequestID returns pointer to last verified resume request ID, or nil if none.
func (s *Session) LastResumeRequestID() *string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastResumeRequestID == "" {
		return nil
	}
	id := s.lastResumeRequestID
	return &id
}

func (s *Session) Suspend(grace time.Duration, cb func(*Session)) bool {
	return s.suspendEpoch(0, grace, cb)
}

func (s *Session) suspendEpoch(epoch uint64, grace time.Duration, cb func(*Session)) bool {
	s.mu.Lock()
	if epoch != 0 && (epoch != s.Epoch || s.resumeBusy || s.barrierPending) {
		s.mu.Unlock()
		return false
	}
	if grace == 0 {
		grace = s.recoveryGrace
	}
	if s.State == StateClosed {
		s.mu.Unlock()
		s.finalize()
		return false
	}
	if !s.ResumeEnabled || grace <= 0 {
		s.closeLocked()
		s.mu.Unlock()
		s.finalize()
		return false
	}
	if s.ResumeDeadline.IsZero() {
		s.beginRecoveryLocked(grace)
	}
	if !time.Now().Before(s.ResumeDeadline) {
		s.closeLocked()
		s.mu.Unlock()
		s.finalize()
		return false
	}
	s.markSuspendedLocked()
	s.State = StateSuspended
	s.dataPlaneActive = false
	s.onTimeout = cb
	s.writerCond.Broadcast()
	r := s.registry
	s.mu.Unlock()
	if r != nil {
		r.MarkSuspended(s)
	}
	return true
}

func (s *Session) Resume(req *protocol.ResumeRequest, barrier time.Duration) (*protocol.ResumeResponse, error) {
	if req == nil || req.ResumeRequestID == "" || len(req.ResumeRequestID) > 128 {
		return nil, ErrInvalidToken
	}
	started := time.Now()
	defer func() { observability.ObserveDuration("egress", "barrier", time.Since(started)) }()
	fingerprint, _ := json.Marshal(req)
	s.mu.Lock()
	if s.State == StateClosed {
		s.mu.Unlock()
		return nil, ErrSessionClosed
	}
	if !s.ResumeDeadline.IsZero() && !time.Now().Before(s.ResumeDeadline) {
		s.closeReason = protocol.ErrResumeExpired
		s.closeLocked()
		s.mu.Unlock()
		s.finalize()
		return nil, ErrResumeExpired
	}
	if !s.ResumeEnabled {
		s.mu.Unlock()
		return nil, ErrResumeDisabled
	}
	if s.lastResumeResponse != nil && s.lastResumeRequestID == req.ResumeRequestID {
		old, _ := json.Marshal(s.lastResumeRequest)
		resp := s.lastResumeResponse
		busy := s.resumeBusy
		s.mu.Unlock()
		if !bytes.Equal(old, fingerprint) {
			return nil, ErrIdempotencyConflict
		}
		if busy {
			// a newer resume is inside its barrier; the cached
			// response names an epoch that is about to become stale.
			return nil, ErrResumeInProgress
		}
		return resp, nil
	}
	if s.resumeBusy {
		err := ErrResumeInProgress
		if req.ResumeRequestID == s.resumePendingID {
			err = ErrResumePending
			if !bytes.Equal(s.resumePendingBytes, fingerprint) {
				err = ErrIdempotencyConflict
			}
		}
		s.mu.Unlock()
		return nil, err
	}
	expected, e := protocol.Offset(req.ExpectedEpoch)
	if e != nil || expected != s.Epoch {
		s.mu.Unlock()
		return nil, ErrStaleEpoch
	}
	if req.ResumeRequestID == "" || s.ingressInstance != "" && req.IngressInstanceID != s.ingressInstance {
		s.mu.Unlock()
		return nil, ErrInvalidToken
	}
	if req.Transport.Mode != "ws" && req.Transport.Mode != "http-sse" {
		s.mu.Unlock()
		return nil, ErrTransportMismatch
	}
	if s.ResumeDeadline.IsZero() {
		s.beginRecoveryLocked(s.recoveryGrace)
	}
	if !time.Now().Before(s.ResumeDeadline) {
		s.closeReason = protocol.ErrResumeExpired
		s.closeLocked()
		s.mu.Unlock()
		s.finalize()
		return nil, ErrResumeExpired
	}
	s.markSuspendedLocked()
	s.resumeBusy = true
	s.resumePendingID = req.ResumeRequestID
	s.resumePendingBytes = append([]byte(nil), fingerprint...)
	s.State = StateResuming
	s.barrierPending = true
	s.dataPlaneActive = false
	cancel := s.activeSSECancel
	s.activeSSECancel = nil
	done := s.inflightDoneCh
	emit := barrierEmitState{end: s.emittingEnd, data: s.emitting}
	deadline := s.ResumeDeadline
	if barrier > time.Until(deadline) {
		barrier = time.Until(deadline)
	}
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if s.registry != nil {
		s.registry.MarkSuspended(s)
	}
	if s.GetState() == StateClosed {
		return nil, ErrSessionClosed
	}
	if done != nil {
		timer := time.NewTimer(barrier)
		select {
		case <-done:
			timer.Stop()
		case <-timer.C:
			if e := s.failBarrier(done); e != nil {
				return nil, e
			}
		}
	}
	return s.finishResume(req, fingerprint, expected, deadline, emit)
}

// barrierEmitState records the downstream data emit in progress when a resume
// closes the data plane. Bytes that were being written at that moment may have
// reached the peer even if the write later fails, so the resume request may
// legitimately report them as delivered.
type barrierEmitState struct {
	end  uint64 // end offset of the data frame being emitted
	data bool   // a data frame emit was in progress
}

// ErrBarrierTimeout is returned by Resume when the started backend write did
// not finish within barrier_timeout (bounded by the remaining grace). It wraps
// ErrSessionClosed: the session is already terminal when it is returned.
var ErrBarrierTimeout = fmt.Errorf("%w: egress write barrier timeout", ErrSessionClosed)

// failBarrier is the barrier-expiry decision point. Under the session state latch it either sees that the write
// did finish at the deadline (nil: commit normally) or closes the session and
// its original backend socket and returns a terminal error. No background
// task is left to commit this resume later, and the same request ID can only
// read the terminal state from then on.
func (s *Session) failBarrier(done <-chan struct{}) error {
	s.mu.Lock()
	if s.State == StateClosed {
		// Another terminal path (e.g. the grace timer) won the latch.
		err := ErrSessionClosed
		if s.closeReason == protocol.ErrResumeExpired {
			err = ErrResumeExpired
		}
		s.mu.Unlock()
		s.finalize()
		return err
	}
	select {
	case <-done:
		s.mu.Unlock()
		return nil
	default:
	}
	err := ErrBarrierTimeout
	s.closeReason = protocol.ErrSessionClosed
	if !s.ResumeDeadline.IsZero() && !time.Now().Before(s.ResumeDeadline) {
		// The barrier was clamped to the remaining grace and that ran out.
		err = ErrResumeExpired
		s.closeReason = protocol.ErrResumeExpired
	}
	s.closeLocked()
	s.mu.Unlock()
	s.finalize()
	return err
}

func (s *Session) finishResume(req *protocol.ResumeRequest, fingerprint []byte, expected uint64, deadline time.Time, emit barrierEmitState) (*protocol.ResumeResponse, error) {
	s.mu.Lock()
	defer func() {
		s.resumeBusy = false
		s.resumePendingID = ""
		s.resumePendingBytes = nil
		s.barrierPending = false
		s.writerCond.Broadcast()
		s.mu.Unlock()
		s.finalize()
	}()
	if s.State == StateClosed {
		return nil, ErrSessionClosed
	}
	if s.Epoch != expected {
		return nil, ErrStaleEpoch
	}
	if !time.Now().Before(deadline) {
		s.closeReason = protocol.ErrResumeExpired
		s.closeLocked()
		return nil, ErrResumeExpired
	}
	known, e := protocol.Offset(req.KnownUpAckOffset)
	if e != nil {
		s.closeReason = protocol.ErrBadAck
		s.closeLocked()
		return nil, ErrBadAck
	}
	next, e := protocol.Offset(req.UpNextOffset)
	if e != nil {
		s.closeReason = protocol.ErrBadAck
		s.closeLocked()
		return nil, ErrBadAck
	}
	down, e := protocol.Offset(req.DownDeliveredOffset)
	if e != nil || known > s.upAckOffset || next < s.upAckOffset || known > next || down < s.downAckOffset || down > s.downProducedOffset || down > s.downEmitted && !(emit.data && down == emit.end) || !s.downBoundaryLocked(down) {
		s.closeReason = protocol.ErrBadAck
		s.closeLocked()
		return nil, ErrBadAck
	}
	if e = s.checkFINLocked(req); e != nil {
		s.closeReason = protocol.ErrFrameConflict
		s.closeLocked()
		return nil, e
	}
	if s.Epoch == math.MaxUint64 {
		s.closeReason = protocol.ErrStaleEpoch
		s.closeLocked()
		return nil, ErrStaleEpoch
	}
	for _, f := range s.upPendingFrames {
		s.retainedBytes.Add(-int64(f.Length))
		if s.registry != nil {
			s.registry.ReleaseGlobalBuffer(int64(f.Length))
		}
	}
	s.upPendingFrames = map[uint64]*InflightUpFrame{}
	s.releaseDownLocked(down)
	// the create-time unattached timer must not outlive a resume; the
	// recovery grace timer bounds the new attach from here on.
	if s.unattachedTimer != nil {
		s.unattachedTimer.Stop()
		s.unattachedTimer = nil
	}
	if req.DownFIN.Applied {
		if !s.DownFIN.Acked {
			atomic.AddInt64(&observability.DefaultMetrics.DownFINAckedTotal, 1)
		}
		s.DownFIN.Applied = true
		s.DownFIN.Acked = true
	}
	// both directions already finished (only the down-FIN ACK was lost).
	// Close normally instead of carrying a finished session into a new epoch.
	if s.UpFIN.Applied && s.DownFIN.Acked && s.downAckOffset == s.downProducedOffset {
		s.closeReason = "closed_normal"
		s.closeLocked()
		return nil, ErrSessionClosed
	}
	// Only bytes emitted in the new epoch count as emitted. This also moves
	// downEmitted forward when down is the end of a frame that was still in
	// flight, so the new epoch's ready ACK (which repeats down) is valid.
	s.downEmitted = down
	s.Epoch++
	s.State = StateAttaching
	s.transportMode = req.Transport.Mode
	s.streamSubscribed = false
	s.currentStreamID = ""
	s.heartbeatNonce = ""
	s.heartbeatEpoch = 0
	s.heartbeatStream = ""
	s.transportWriting = false
	s.transportRxBlocked = false
	s.dataPlaneActive = false
	s.emitting = false
	s.finEmitting = false
	// The FIN, like the data bytes, must be emitted again on the new stream
	// before a FIN ACK of the new epoch is accepted.
	s.finEmitted = false
	s.pendingAck = 0
	s.pendingFinAck = false
	if req.UpFIN.Seen && !s.UpFIN.Seen && req.UpFIN.FinalOffset != nil {
		v, _ := protocol.Offset(*req.UpFIN.FinalOffset)
		s.UpFIN = FINState{Seen: true, FinalOffset: v}
	}
	upFIN := s.directionLocked(s.UpFIN)
	resp := &protocol.ResumeResponse{SessionID: s.ID, InstanceID: s.InstanceID, ResumeRequestID: req.ResumeRequestID, Epoch: strconv.FormatUint(s.Epoch, 10), Transport: protocol.TransportMode{Mode: s.transportMode}, UpWrittenOffset: strconv.FormatUint(s.upWrittenOffset, 10), AckUpOffset: strconv.FormatUint(s.upAckOffset, 10), DownReplayFrom: strconv.FormatUint(down, 10), DownProducedOffset: strconv.FormatUint(s.downProducedOffset, 10), DownRetainedFrom: strconv.FormatUint(s.downBuffer.StartOffset(), 10), UpFIN: protocol.ResumeUpFIN{Received: upFIN.Seen, FinalOffset: upFIN.FinalOffset, Applied: upFIN.Applied}, DownFIN: s.directionLocked(s.DownFIN), ResumeRemainingMs: time.Until(deadline).Milliseconds()}
	s.lastResumeRequestID = req.ResumeRequestID
	var copyReq protocol.ResumeRequest
	_ = json.Unmarshal(fingerprint, &copyReq)
	s.lastResumeRequest = &copyReq
	s.lastResumeResponse = resp
	return resp, nil
}

func formatOffsetPtr(v uint64) *string {
	s := strconv.FormatUint(v, 10)
	return &s
}

func (s *Session) closeRecovery(lease *resource.RetentionLease) {
	s.mu.Lock()
	if s.recoveryLease != lease || !lease.Active() || s.State == StateClosed {
		s.mu.Unlock()
		return
	}
	s.closeReason = protocol.ErrBufferExhausted
	s.closeLocked()
	s.mu.Unlock()
	s.finalize()
}

func (s *Session) beginRecoveryLocked(grace time.Duration) {
	if !s.ResumeDeadline.IsZero() {
		return
	}
	if grace <= 0 {
		grace = 30 * time.Second
	}
	started := time.Now()
	s.ResumeDeadline = started.Add(grace)
	var lease *resource.RetentionLease
	lease = resource.NewRetentionLease(started, s.retainedBytes.Load, func() { s.closeRecovery(lease) })
	s.recoveryLease = lease
	s.graceTimer = time.AfterFunc(grace, func() {
		s.mu.Lock()
		expired := !s.ResumeDeadline.IsZero() && !time.Now().Before(s.ResumeDeadline)
		cb := s.onTimeout
		if expired {
			s.closeReason = protocol.ErrResumeExpired
			s.closeLocked()
		}
		s.mu.Unlock()
		if expired {
			s.finalize()
			if cb != nil {
				cb(s)
			}
		}
	})
}

// checkFINLocked validates the FIN fields of a resume request. A down FIN
// reported as applied must have entered emit settlement in some epoch
// (downFINSent); a FIN that was mid-emit when the resume started counts, since
// it may have reached the peer before the write failed.
func (s *Session) checkFINLocked(r *protocol.ResumeRequest) error {
	if r.UpFIN.Applied && !r.UpFIN.Seen || r.UpFIN.Acked && !r.UpFIN.Applied || r.DownFIN.Applied && !r.DownFIN.Seen || r.DownFIN.Acked && !r.DownFIN.Applied {
		return ErrFrameConflict
	}
	if !r.UpFIN.Seen && r.UpFIN.FinalOffset != nil || !r.DownFIN.Seen && r.DownFIN.FinalOffset != nil || s.UpFIN.Seen && !r.UpFIN.Seen {
		return ErrFrameConflict
	}
	if r.UpFIN.Seen {
		if r.UpFIN.FinalOffset == nil {
			return ErrFrameConflict
		}
		n, e := protocol.Offset(*r.UpFIN.FinalOffset)
		next, ne := protocol.Offset(r.UpNextOffset)
		if e != nil || ne != nil || n != next || n < s.upAckOffset || s.UpFIN.Seen && n != s.UpFIN.FinalOffset {
			return ErrFrameConflict
		}
	}
	if r.UpFIN.Applied && !s.UpFIN.Applied {
		return ErrFrameConflict
	}
	if r.DownFIN.Seen {
		if r.DownFIN.FinalOffset == nil || !s.DownFIN.Seen {
			return ErrFrameConflict
		}
		n, e := protocol.Offset(*r.DownFIN.FinalOffset)
		if e != nil || n != s.DownFIN.FinalOffset {
			return ErrFrameConflict
		}
	}
	if r.DownFIN.Applied && (!s.DownFIN.Seen || r.DownDeliveredOffset != strconv.FormatUint(s.DownFIN.FinalOffset, 10) || !s.downFINSent) {
		return ErrFrameConflict
	}
	return nil
}

func (s *Session) DropTransport(ep uint64) {
	s.suspendEpoch(ep, 0, nil)
}

func (s *Session) markSuspendedLocked() {
	if !s.metricSuspended {
		s.metricSuspended = true
		atomic.AddInt64(&observability.DefaultMetrics.ActiveSessions, -1)
		atomic.AddInt64(&observability.DefaultMetrics.SuspendedSessions, 1)
	}
}
