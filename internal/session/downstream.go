package session

import (
	"math"
	"sync/atomic"
	"time"

	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
)

// maxDownFrames bounds retained downstream frames per session.
const maxDownFrames = 512

// MaxDownFrames is maxDownFrames for transports that advertise it to the
// peer (protocol.SessionLimits.DownMaxFrames).
const MaxDownFrames = maxDownFrames

// WaitDownCapacity blocks until at least needed bytes of buffer capacity are available, or session is closed.
// Returns false if session is closed.
//
// The backpressure timeout only counts time the session spends attached: while
// it is suspended or resuming, recovery.grace and the retention budget bound
// the stall instead. The timer only flags expiry; the decision to close
// is taken here under s.mu, so a waiter that just got capacity is never closed
// by a timer that fired concurrently.
func (s *Session) WaitDownCapacity(needed int64) bool {
	s.mu.Lock()
	expired := false
	timeout := s.backpressureTimeout
	timer := time.AfterFunc(timeout, func() {
		s.mu.Lock()
		expired = true
		s.downCond.Broadcast()
		s.mu.Unlock()
	})
	defer timer.Stop()
	blocked, killed := false, false
	for s.State != StateClosed && (s.downBuffer.AvailableCapacity() < needed || len(s.downFrames) >= maxDownFrames) {
		if expired {
			if !s.recoveringLocked() {
				s.closeReason = protocol.ErrBufferExhausted
				s.closeLocked()
				killed = true
				break
			}
			// Recovery time is not backpressure: restart the attached budget.
			expired = false
			timer.Reset(timeout)
		}
		if !blocked {
			atomic.AddInt64(&observability.DefaultMetrics.BackpressureTotal, 1)
			blocked = true
		}
		s.downCond.Wait()
	}
	ok := s.State != StateClosed
	s.mu.Unlock()
	if killed {
		s.finalize()
	}
	return ok
}

// recoveringLocked reports whether the session is between a transport fault and
// a completed recovery (suspended, resuming, or re-attaching after Resume).
func (s *Session) recoveringLocked() bool {
	return s.State == StateSuspended || s.State == StateResuming || s.resumeBusy || s.barrierPending ||
		!s.ResumeDeadline.IsZero() && !s.dataPlaneActive
}

// AdvanceDownAck advances acknowledged delivered downstream bytes and frees buffers.
func (s *Session) AdvanceDownAck(off uint64, fin bool) error {
	s.mu.Lock()
	defer func() { s.mu.Unlock(); s.finalize() }()
	return s.advanceDownLocked(off, fin)
}

func (s *Session) AdvanceDownAckAtomic(epoch uint64, off uint64, fin bool) error {
	s.mu.Lock()
	defer func() { s.mu.Unlock(); s.finalize() }()
	if epoch != s.Epoch {
		return ErrStaleEpoch
	}
	return s.advanceDownLocked(off, fin)
}

func (s *Session) AppendDownBytes(data []byte) (uint64, error) {
	s.mu.Lock()
	if s.State == StateClosed {
		s.mu.Unlock()
		return 0, ErrSessionClosed
	}
	if len(data) == 0 || s.downProducedOffset > math.MaxUint64-uint64(len(data)) || len(s.downFrames) >= maxDownFrames {
		s.mu.Unlock()
		return 0, ErrBufferExhausted
	}
	r := s.registry
	if r != nil && !r.AcquireGlobalBuffer(int64(len(data))) {
		s.mu.Unlock()
		return 0, ErrBufferExhausted
	}
	off := s.downProducedOffset
	if e := s.downBuffer.Append(off, data); e != nil {
		if r != nil {
			r.ReleaseGlobalBuffer(int64(len(data)))
		}
		s.mu.Unlock()
		return 0, e
	}
	s.downProducedOffset += uint64(len(data))
	s.downFrames = append(s.downFrames, downFrame{off, s.downProducedOffset})
	s.retainedBytes.Add(int64(len(data)))
	s.signalLocked()
	s.mu.Unlock()
	if r != nil {
		r.UpdateSuspended()
	}
	return off, nil
}

func (s *Session) MarkDownFinal(finalOffset uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.DownFIN.Seen = true
	s.DownFIN.FinalOffset = finalOffset
	s.downBuffer.SetFinalOffset(finalOffset)
	if s.State == StateActive {
		s.State = StateDraining
	}
}

func (s *Session) ReadDownRange(off uint64, max int) ([]byte, uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.State == StateClosed {
		return nil, off, false
	}
	for _, f := range s.downFrames {
		if f.start == off {
			if f.end-f.start > uint64(max) || max <= 0 {
				return nil, off, false
			}
			data, next, _ := s.downBuffer.ReadRange(off, int(f.end-f.start))
			return data, next, s.DownFIN.Seen && next == s.DownFIN.FinalOffset
		}
	}
	return nil, off, s.DownFIN.Seen && off == s.DownFIN.FinalOffset
}

func (s *Session) downBoundaryLocked(off uint64) bool {
	if off == s.downAckOffset {
		return true
	}
	for _, f := range s.downFrames {
		if f.end == off {
			return true
		}
	}
	return false
}

func (s *Session) releaseDownLocked(off uint64) {
	if off <= s.downAckOffset {
		return
	}
	s.downAckOffset = off
	before := s.downBuffer.AllocatedBytes()
	s.downBuffer.Acknowledge(off)
	n := before - s.downBuffer.AllocatedBytes()
	s.retainedBytes.Add(-n)
	if s.registry != nil {
		s.registry.ReleaseGlobalBuffer(n)
	}
	i := 0
	for i < len(s.downFrames) && s.downFrames[i].end <= off {
		i++
	}
	s.downFrames = append([]downFrame(nil), s.downFrames[i:]...)
	s.downCond.Broadcast()
}

func (s *Session) validateDownLocked(off uint64, fin bool) error {
	if s.State == StateClosed {
		return ErrSessionClosed
	}
	// An ACK at or below the released boundary is a harmless duplicate (for
	// example the ready ACK after Resume, which repeats down_delivered_offset).
	// A FIN ACK at the boundary still goes through the FIN checks below.
	if off < s.downAckOffset || off == s.downAckOffset && !fin {
		return nil
	}
	if off > s.downProducedOffset || !s.downBoundaryLocked(off) {
		return ErrBadAck
	}
	if off > s.downEmitted && !(s.emitting && off == s.emittingEnd) {
		return ErrBadAck
	}
	if fin && (!s.DownFIN.Seen || off != s.DownFIN.FinalOffset) {
		return ErrBadAck
	}
	// A FIN ACK must follow a FIN emit in this epoch. Once the FIN is
	// acknowledged, a repeated FIN ACK at the final offset stays valid: the
	// peer keeps reporting its applied FIN until it sees down_fin_acked.
	if fin && !s.DownFIN.Acked && !s.finEmitted && !s.finEmitting {
		return ErrBadAck
	}
	return nil
}

func (s *Session) advanceDownLocked(off uint64, fin bool) error {
	if e := s.validateDownLocked(off, fin); e != nil {
		return e
	}
	if off < s.downAckOffset {
		return nil
	}
	if s.emitting && off > s.downEmitted {
		s.pendingAck = off
		if fin {
			s.pendingFinAck = true
		}
		return nil
	}
	s.releaseDownLocked(off)
	if fin {
		if s.finEmitting && !s.finEmitted && !s.DownFIN.Acked {
			s.pendingFinAck = true
		} else {
			if !s.DownFIN.Acked {
				atomic.AddInt64(&observability.DefaultMetrics.DownFINAckedTotal, 1)
			}
			s.DownFIN.Applied = true
			s.DownFIN.Acked = true
			s.maybeNormalCloseLocked()
		}
	}
	return nil
}

// ReserveDownRead charges the one backend reader against raw payload, not Codec.
// The lease covers the socket read and is transferred to the retained window,
// so an idle backend cannot hold the codec needed to deliver another session.
func (s *Session) ReserveDownRead(n int64) bool {
	s.mu.Lock()
	if n <= 0 || s.State == StateClosed || s.downReadReserved != 0 || s.downBuffer.AvailableCapacity() < n {
		s.mu.Unlock()
		return false
	}
	r := s.registry
	if r != nil && !r.AcquireGlobalBuffer(n) {
		s.mu.Unlock()
		return false
	}
	s.downReadReserved = n
	s.retainedBytes.Add(n)
	s.mu.Unlock()
	if r != nil {
		r.UpdateSuspended()
	}
	return true
}

// FinishDownRead consumes the reservation even on failure. No copy or second
// resource acquisition is needed while holding the raw read allocation.
func (s *Session) FinishDownRead(data []byte) (uint64, error) {
	s.mu.Lock()
	r := s.registry
	reserved := s.downReadReserved
	s.downReadReserved = 0
	release := func() {
		s.retainedBytes.Add(-reserved)
		if r != nil {
			r.ReleaseGlobalBuffer(reserved)
		}
	}
	var err error
	off := s.downProducedOffset
	switch {
	case s.State == StateClosed:
		err = ErrSessionClosed
	case len(data) == 0:
	case reserved <= 0 || int64(cap(data)) != reserved || s.downProducedOffset > ^uint64(0)-uint64(len(data)) || len(s.downFrames) >= maxDownFrames:
		err = ErrBufferExhausted
	default:
		owned := data
		if int64(len(data))*4 <= reserved {
			// a short read would pin the whole read allocation in the
			// direction window (a 1-byte frame charged as 32 KiB). Keep a
			// compact copy and return the unused part of the reservation.
			owned = make([]byte, len(data))
			copy(owned, data)
		}
		err = s.downBuffer.AppendOwned(off, owned)
		if err == nil {
			s.downProducedOffset += uint64(len(owned))
			s.downFrames = append(s.downFrames, downFrame{off, s.downProducedOffset})
			// The window now owns cap(owned); release the remainder below.
			reserved -= int64(cap(owned))
			s.signalLocked()
		}
	}
	release()
	s.mu.Unlock()
	if r != nil {
		r.UpdateSuspended()
	}
	return off, err
}

func (s *Session) NextDownFrameBytes(off uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.downFrames {
		if f.start == off {
			return int(f.end - f.start)
		}
	}
	return 0
}

// Recovering reports whether the session is between a transport fault and a
// completed recovery. Callers with their own stall timers (for example the
// backend reader's read-reservation loop) use it to exempt recovery time from
// backpressure, as WaitDownCapacity does.
func (s *Session) Recovering() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.State != StateClosed && s.recoveringLocked()
}
