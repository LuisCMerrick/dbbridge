package session

import (
	"context"
	"time"

	"dbbridge/internal/protocol"
)

// ReserveStream reserves the single stream subscription slot (WS or SSE) for the given epoch and transport mode.
func (s *Session) ReserveStream(epoch uint64, transport string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.State == StateClosed {
		return "", ErrSessionClosed
	}
	if !s.ResumeDeadline.IsZero() && !time.Now().Before(s.ResumeDeadline) {
		return "", ErrResumeExpired
	}
	// the initial attach deadline is decided under the same lock
	// as the slot; the unattached timer closes the session.
	if s.initialAttachExpiredLocked(time.Now()) {
		return "", ErrSessionClosed
	}
	// no stream slot while a resume barrier is open; the old epoch is
	// about to be replaced and finishResume would reset the reservation.
	if s.State != StateAttaching || s.resumeBusy || s.barrierPending {
		return "", ErrNotActive
	}
	if epoch != s.Epoch {
		return "", ErrStaleEpoch
	}
	if s.transportMode != "" && s.transportMode != transport {
		return "", ErrTransportMismatch
	}
	if s.streamSubscribed {
		return "", ErrStreamExists
	}

	s.transportMode = transport
	s.streamSubscribed = true
	s.currentStreamID = "st_" + generateID("", 12)
	return s.currentStreamID, nil
}

// ReserveSSEStream reserves the single SSE subscription slot for the given epoch.
func (s *Session) ReserveSSEStream(epoch uint64) (string, error) {
	return s.ReserveStream(epoch, "http-sse")
}

// CurrentStreamID returns the currently bound stream ID.
func (s *Session) CurrentStreamID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentStreamID
}

// GetTransportMode returns the currently bound transport mode ("ws" or "http-sse").
func (s *Session) GetTransportMode() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transportMode
}

// SetTransportMode sets the transport mode for current epoch.
func (s *Session) SetTransportMode(mode string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transportMode = mode
}

// ActivateDataPlane marks the data plane active upon receiving client ready ACK.
func (s *Session) ActivateDataPlane(streamID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activateLocked(s.Epoch, streamID)
}

func (s *Session) activateLocked(epoch uint64, id string) error {
	if s.State == StateClosed {
		return ErrSessionClosed
	}
	if epoch != s.Epoch {
		return ErrStaleEpoch
	}
	if id == "" || id != s.currentStreamID || !s.streamSubscribed || s.State == StateSuspended || s.resumeBusy {
		return ErrNotActive
	}
	if !s.ResumeDeadline.IsZero() && !time.Now().Before(s.ResumeDeadline) {
		return ErrResumeExpired
	}
	if s.initialAttachExpiredLocked(time.Now()) {
		return ErrSessionClosed
	}
	s.dataPlaneActive = true
	s.attachDeadline = time.Time{}
	if s.unattachedTimer != nil {
		s.unattachedTimer.Stop()
		s.unattachedTimer = nil
	}
	if s.UpFIN.Seen || s.DownFIN.Seen {
		s.State = StateDraining
	} else {
		s.State = StateActive
	}
	s.writerCond.Broadcast()
	return nil
}

// StartUnattachedTimer fixes the absolute initial attach deadline
// (unattached_timeout) for the current epoch. ReserveStream and ready
// activation refuse once it has passed, and the timer itself closes the session
// under the same lock if the initial attach is still pending, so expiry and
// activation have one decision point. onTimeout, if set, runs after
// the session was closed by the timer.
func (s *Session) StartUnattachedTimer(timeout time.Duration, onTimeout func(*Session)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.State == StateClosed {
		return
	}
	if s.unattachedTimer != nil {
		s.unattachedTimer.Stop()
	}
	deadline := time.Now().Add(timeout)
	s.attachDeadline, s.attachEpoch = deadline, s.Epoch
	s.unattachedTimer = time.AfterFunc(timeout, func() {
		s.mu.Lock()
		// Still applicable: same deadline, still the initial epoch (a resume
		// replaced it with the recovery deadline) and never activated.
		if s.State == StateClosed || !s.attachDeadline.Equal(deadline) || !s.initialAttachPendingLocked() {
			s.mu.Unlock()
			return
		}
		s.closeLocked()
		s.mu.Unlock()
		s.finalize()
		if onTimeout != nil {
			onTimeout(s)
		}
	})
}

// initialAttachPendingLocked reports whether the initial attach deadline still
// governs the session: it was set, the epoch is the one it was set for, and the
// data plane has not been activated yet.
func (s *Session) initialAttachPendingLocked() bool {
	return !s.attachDeadline.IsZero() && s.Epoch == s.attachEpoch && !s.dataPlaneActive
}

func (s *Session) initialAttachExpiredLocked(now time.Time) bool {
	return s.initialAttachPendingLocked() && !now.Before(s.attachDeadline)
}

// SetStreamCancel binds cancellation only while this handler still owns the
// reserved stream. A handler delayed across Resume cannot replace its successor.
func (s *Session) SetStreamCancel(epoch uint64, streamID string, cancel context.CancelFunc) error {
	s.mu.Lock()
	if err := s.checkStreamLocked(epoch, streamID, false); err != nil {
		s.mu.Unlock()
		return err
	}
	previous := s.activeSSECancel
	s.activeSSECancel = cancel
	s.mu.Unlock()
	if previous != nil {
		previous()
	}
	return nil
}

func (s *Session) CheckStream(ep uint64, id string, active bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkStreamLocked(ep, id, active)
}

func (s *Session) checkStreamLocked(ep uint64, id string, active bool) error {
	if s.State == StateClosed {
		return ErrSessionClosed
	}
	if ep != s.Epoch {
		return ErrStaleEpoch
	}
	if id == "" || id != s.currentStreamID || !s.streamSubscribed || s.resumeBusy || s.State == StateSuspended {
		return ErrNotActive
	}
	if active && !s.dataPlaneActive {
		return ErrNotActive
	}
	if !s.ResumeDeadline.IsZero() && !time.Now().Before(s.ResumeDeadline) {
		return ErrResumeExpired
	}
	return nil
}

// CloseStream makes protocol termination conditional on ownership at the final
// state-change point. A stale or frozen worker cannot close another generation.
func (s *Session) CloseStream(ep uint64, id string) bool {
	return s.CloseStreamWithReason(ep, id, protocol.ErrFrameConflict)
}

func (s *Session) CloseStreamWithReason(ep uint64, id, reason string) bool {
	s.mu.Lock()
	if ep != s.Epoch || id != s.currentStreamID || s.resumeBusy || s.barrierPending || s.State == StateSuspended || s.State == StateClosed {
		s.mu.Unlock()
		return false
	}
	s.closeReason = reason
	s.closeLocked()
	s.mu.Unlock()
	s.finalize()
	return true
}

func (s *Session) SetTransportWriting(ep uint64, id string, writing bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ep == s.Epoch && id == s.currentStreamID && s.State != StateClosed {
		s.transportWriting = writing
	}
}

func (s *Session) SetTransportRxBlocked(ep uint64, id string, blocked bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ep == s.Epoch && id == s.currentStreamID && s.State != StateClosed {
		s.transportRxBlocked = blocked
	}
}
