package session

import (
	"sync/atomic"
	"time"

	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
)

func (s *Session) finalize() {
	if !s.closed.Load() || s.finalized.Swap(true) {
		return
	}
	s.mu.Lock()
	conn := s.BackendConn
	cancel := s.activeSSECancel
	s.activeSSECancel = nil
	fn := s.onFinalize
	r := s.registry
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.Close()
	}
	if r != nil {
		r.Unregister(s.ID)
	}
	if fn != nil {
		fn(s)
	}
}

func (s *Session) maybeNormalCloseLocked() {
	if s.UpFIN.Applied && s.DownFIN.Acked && s.downAckOffset == s.downProducedOffset {
		s.closeReason = "closed_normal"
		s.closeLocked()
	}
}

func (s *Session) CloseWithEpoch(ep uint64) error {
	s.mu.Lock()
	if s.State != StateClosed && ep != s.Epoch {
		s.mu.Unlock()
		return ErrStaleEpoch
	}
	s.closeLocked()
	s.mu.Unlock()
	s.finalize()
	return nil
}

func (s *Session) Close() {
	s.mu.Lock()
	s.closeLocked()
	s.mu.Unlock()
	s.finalize()
}

func (s *Session) closeLocked() {
	if s.closed.Swap(true) {
		return
	}
	if s.metricSuspended {
		atomic.AddInt64(&observability.DefaultMetrics.SuspendedSessions, -1)
	} else {
		atomic.AddInt64(&observability.DefaultMetrics.ActiveSessions, -1)
	}
	if s.closeReason == "" {
		s.closeReason = protocol.ErrSessionClosed
	}
	observability.RecordEvent("egress", "terminate", terminalCategory(s.closeReason), s.transportMode, "none")
	s.State = StateClosed
	if s.done != nil {
		close(s.done)
	}
	s.recoveryLease.End()
	s.writingClosed = true
	s.dataPlaneActive = false
	s.writerCond.Broadcast()
	s.downCond.Broadcast()
	for _, t := range []*time.Timer{s.graceTimer, s.activationTimer, s.unattachedTimer} {
		if t != nil {
			t.Stop()
		}
	}
	for _, f := range s.upPendingFrames {
		s.retainedBytes.Add(-int64(f.Length))
		if s.registry != nil {
			s.registry.ReleaseGlobalBuffer(int64(f.Length))
		}
	}
	s.upPendingFrames = nil
	n := s.downBuffer.AllocatedBytes()
	s.retainedBytes.Add(-n)
	if s.registry != nil {
		s.registry.ReleaseGlobalBuffer(n)
	}
	s.downBuffer.Reset()
	s.downFrames = nil
	s.receiptTable.Clear()
	s.lastResumeRequest = nil
	s.lastResumeResponse = nil
	s.resumePendingBytes = nil
	s.resumePendingID = ""
	s.resumeBusy = false
	s.barrierPending = false
	s.signalLocked()
	atomic.AddInt64(&observability.DefaultMetrics.ClosedSessions, 1)
}

func (s *Session) CloseWithReason(code string) {
	s.mu.Lock()
	if s.State != StateClosed {
		s.closeReason = code
		s.closeLocked()
	}
	s.mu.Unlock()
	s.finalize()
}

func (s *Session) TerminalCode() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closeReason == "closed_normal" {
		return protocol.ErrSessionClosed
	}
	return s.closeReason
}

func terminalCategory(code string) string {
	switch code {
	case "closed_normal":
		return "normal"
	case protocol.ErrBackendLost:
		return "backend_lost"
	case protocol.ErrResumeExpired:
		return "timeout"
	case protocol.ErrTargetForbidden:
		return "forbidden"
	case protocol.ErrBufferExhausted:
		return "buffer"
	case protocol.ErrFrameConflict, protocol.ErrFrameTooLarge:
		return "protocol"
	}
	return "failed"
}

func (s *Session) signalLocked() {
	if s.notify != nil {
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}
}

// TerminalCategory returns the terminal category of a closed session ("normal",
// "timeout", "buffer", ...), or "" while it is open. Unlike TerminalCode it
// distinguishes a completed normal close from other closes.
func (s *Session) TerminalCategory() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.State != StateClosed {
		return ""
	}
	return terminalCategory(s.closeReason)
}
