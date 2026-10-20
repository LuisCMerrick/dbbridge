package session

import (
	"sync/atomic"

	"dbbridge/internal/observability"
)

func (s *Session) BeginEmit(ep, from, end uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ep != s.Epoch || !s.dataPlaneActive || s.State == StateClosed || s.barrierPending || end < from || !s.downBoundaryLocked(end) {
		return ErrNotActive
	}
	s.emitting = true
	s.emittingEnd = end
	return nil
}

func (s *Session) FinishEmit(ep, end uint64, writeErr error) {
	s.mu.Lock()
	defer func() { s.mu.Unlock(); s.finalize() }()
	if ep != s.Epoch || s.State == StateClosed {
		return
	}
	s.emitting = false
	// A peer ACK of the frame end parked during the emit proves delivery even
	// if the local write later reported an error (for example a failed flush).
	delivered := writeErr == nil || s.pendingAck >= end
	if delivered && end > s.downEmitted {
		s.downEmitted = end
	}
	if delivered && s.pendingAck <= s.downEmitted && s.pendingAck > s.downAckOffset {
		s.releaseDownLocked(s.pendingAck)
	}
	s.pendingAck = 0
}

func (s *Session) BeginFinEmit(ep uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ep != s.Epoch || !s.DownFIN.Seen || s.downEmitted < s.DownFIN.FinalOffset || !s.dataPlaneActive {
		return ErrNotActive
	}
	s.finEmitting = true
	s.downFINSent = true
	return nil
}

// FinishFinEmit settles a FIN emit started by BeginFinEmit. A down-FIN ACK
// parked during the emit proves delivery even if the local write later
// reported an error, the same rule FinishEmit applies to data frames. Without
// such an ACK a failed write leaves the FIN unemitted for this epoch.
func (s *Session) FinishFinEmit(ep uint64, e error) {
	s.mu.Lock()
	defer func() { s.mu.Unlock(); s.finalize() }()
	if ep != s.Epoch || s.State == StateClosed {
		return
	}
	s.finEmitting = false
	if e == nil || s.pendingFinAck {
		s.finEmitted = true
	}
	if s.pendingFinAck {
		if !s.DownFIN.Acked {
			atomic.AddInt64(&observability.DefaultMetrics.DownFINAckedTotal, 1)
		}
		s.DownFIN.Applied = true
		s.DownFIN.Acked = true
		s.pendingFinAck = false
		s.maybeNormalCloseLocked()
	}
}
