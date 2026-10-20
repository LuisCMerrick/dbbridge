package session

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"sync/atomic"
	"time"

	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
)

func (s *Session) ControlDeadline(detect time.Duration) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastControl.Add(detect)
}

func (s *Session) Ack(ep uint64, id string, req protocol.AckRequest) (*protocol.AckResponse, error) {
	s.mu.Lock()
	defer func() { s.mu.Unlock(); s.finalize() }()
	if s.State == StateClosed {
		if ep == s.Epoch && id == s.currentStreamID && id != "" && s.UpFIN.Applied && s.DownFIN.Acked {
			return s.ackLocked(req.ProbeID), nil
		}
		return nil, ErrSessionClosed
	}
	if e := s.checkStreamLocked(ep, id, false); e != nil {
		return nil, e
	}
	down, e := protocol.Offset(req.DownDeliveredOffset)
	if e != nil {
		return nil, ErrBadAck
	}
	up, e := protocol.Offset(req.UpConfirmedOffset)
	if e != nil {
		return nil, ErrBadAck
	}
	if e = s.validateDownLocked(down, req.DownFinApplied); e != nil {
		return nil, e
	}
	if up > s.upAckOffset || up > s.receiptTable.ReceiptFloor() && !s.receiptTable.Boundary(up) {
		return nil, ErrBadAck
	}
	if req.ReadyReceived {
		if e = s.activateLocked(ep, id); e != nil {
			return nil, e
		}
	}
	// confirm upstream before advancing downstream. The down-FIN ACK can
	// complete the normal close, and closeLocked clears the receipt table that
	// confirmUpLocked validates against.
	if e = s.confirmUpLocked(up); e != nil {
		return nil, e
	}
	if e = s.advanceDownLocked(down, req.DownFinApplied); e != nil {
		return nil, e
	}
	if req.ProbeID != "" {
		s.lastControl = time.Now()
	}
	// ingress repeats the latest ping nonce on every ACK. Only the first
	// match after a fault completes recovery; a healthy session skips the
	// branch (and the global retention sweep it triggers).
	recovering := !s.ResumeDeadline.IsZero() || s.metricSuspended
	if recovering && s.dataPlaneActive && s.heartbeatNonce != "" && s.heartbeatEpoch == ep && s.heartbeatStream == id && req.HeartbeatNonce == s.heartbeatNonce && req.ProbeID != "" {
		if s.graceTimer != nil {
			s.graceTimer.Stop()
		}
		if !s.ResumeDeadline.IsZero() {
			atomic.AddInt64(&observability.DefaultMetrics.ResumeSuccess, 1)
		}
		if s.metricSuspended {
			atomic.AddInt64(&observability.DefaultMetrics.SuspendedSessions, -1)
			atomic.AddInt64(&observability.DefaultMetrics.ActiveSessions, 1)
			s.metricSuspended = false
		}
		s.ResumeDeadline = time.Time{}
		s.recoveryLease.End()
		if s.registry != nil {
			go s.registry.UnmarkSuspended(s)
		}
	}
	s.signalLocked()
	return s.ackLocked(req.ProbeID), nil
}

func (s *Session) ackLocked(probe string) *protocol.AckResponse {
	rx := s.rxBlockedLocked()
	return &protocol.AckResponse{SessionID: s.ID, InstanceID: s.InstanceID, Epoch: strconv.FormatUint(s.Epoch, 10), StreamID: s.currentStreamID, Transport: &protocol.TransportMode{Mode: s.transportMode}, AckUpOffset: strconv.FormatUint(s.upAckOffset, 10), DownAckOffset: strconv.FormatUint(s.downAckOffset, 10), UpFinApplied: s.UpFIN.Applied, DownFinAcked: s.DownFIN.Acked, ProbeID: probe, DataTxBlocked: s.transportWriting, DataRxBlocked: rx, BackpressureReason: s.backpressureReasonLocked(rx)}
}

// backpressureReasonLocked names the blockage behind data_tx_blocked and
// data_rx_blocked. Receive blockage takes precedence: "global_buffer" when
// the transport stopped reading for lack of buffer, "local_socket_write" when
// the in-flight queue is full behind a backend socket write. Otherwise
// "data_channel_write" while a downstream transport write is in progress.
func (s *Session) backpressureReasonLocked(rx bool) string {
	switch {
	case s.transportRxBlocked:
		return "global_buffer"
	case rx:
		return "local_socket_write"
	case s.transportWriting:
		return "data_channel_write"
	}
	return "none"
}

func (s *Session) Snapshot() *protocol.AckResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ackLocked("")
}

func (s *Session) FINs() (FINState, FINState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.UpFIN, s.DownFIN
}

func (s *Session) directionLocked(f FINState) protocol.DirectionFIN {
	d := protocol.DirectionFIN{Seen: f.Seen, Applied: f.Applied, Acked: f.Acked}
	if f.Seen {
		d.FinalOffset = formatOffsetPtr(f.FinalOffset)
	}
	return d
}

func (s *Session) Heartbeat() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.heartbeatNonce == "" {
		var b [16]byte
		_, _ = rand.Read(b[:])
		s.heartbeatNonce = hex.EncodeToString(b[:])
		s.heartbeatEpoch, s.heartbeatStream = s.Epoch, s.currentStreamID
	}
	return s.heartbeatNonce
}

func (s *Session) ControlExpired(timeout time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dataPlaneActive && time.Since(s.lastControl) > timeout
}

func (s *Session) NewHeartbeat(ep uint64, id string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.checkStreamLocked(ep, id, false); e != nil {
		return "", e
	}
	var b [16]byte
	_, _ = rand.Read(b[:])
	s.heartbeatNonce = hex.EncodeToString(b[:])
	s.heartbeatEpoch, s.heartbeatStream = ep, id
	return s.heartbeatNonce, nil
}

func (s *Session) ApplyPiggyback(ep uint64, id string, down, up uint64) error {
	_, e := s.Ack(ep, id, protocol.AckRequest{DownDeliveredOffset: strconv.FormatUint(down, 10), UpConfirmedOffset: strconv.FormatUint(up, 10)})
	return e
}

// rxBlockedLocked reports upstream receive backpressure. A backend
// write in progress is normal operation; it only blocks the peer when the
// in-flight queue is full behind it.
func (s *Session) rxBlockedLocked() bool {
	if s.transportRxBlocked {
		return true
	}
	return s.inflightWriting && s.maxInflight > 0 && len(s.upPendingFrames)+1 >= s.maxInflight
}

// UpReceiptCapacity returns the per-session upstream receipt limit, the number
// of written-but-unconfirmed frames the session accepts before it answers
// WINDOW_EXCEEDED. Transports can advertise it to the peer.
func (s *Session) UpReceiptCapacity() int { return s.receiptTable.Capacity() }

// AckWanted reports whether an explicit control ACK from the peer would unblock
// this session soon: unconfirmed upstream receipts or retained downstream frames
// have reached half of their limits. Transports can piggyback it (for example as
// an "ack requested" hint on ack_up) so a peer that batches ACKs by bytes does
// not stall small-frame traffic until its ACK timer fires.
func (s *Session) AckWanted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.State == StateClosed {
		return false
	}
	used := s.receiptTable.Count() + len(s.upPendingFrames) + boolInt(s.inflightWriting)
	return 2*used >= s.receiptTable.Capacity() || len(s.downFrames) >= maxDownFrames/2
}
