package session

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
)

// IngestUpFrame verifies window, de-duplicates, and enqueues frame for single backend writer.
// Returns:
// alreadyWritten: true if already written to DB socket (idempotent 200)
// accepted: true if enqueued for writing (202)
// currentAckUp: current stable written boundary
func (s *Session) IngestUpFrame(epoch uint64, offset uint64, length int, sha string, data []byte) (bool, bool, uint64, error) {
	written, accepted, ack, _, err := s.ingestUpFrame(epoch, offset, length, sha, data, false, false)
	return written, accepted, ack, err
}

// IngestUpFrameReserved consumes the existing length-byte payload reservation
// only when a new frame is enqueued. Duplicates and errors leave it to the caller.
func (s *Session) IngestUpFrameReserved(epoch uint64, offset uint64, length int, sha string, data []byte) (bool, bool, uint64, bool, error) {
	return s.ingestUpFrame(epoch, offset, length, sha, data, true, false)
}

// IngestUpFrameOwned is the transport fast path. The caller has already
// verified that sha is the lowercase hex SHA-256 of data, and hands data
// over: a frame that is enqueued keeps the buffer instead of a copy, so the
// caller must not reuse it after the call. Duplicates and errors leave data
// untouched. reserved has the meaning of IngestUpFrameReserved.
func (s *Session) IngestUpFrameOwned(epoch uint64, offset uint64, sha string, data []byte, reserved bool) (bool, bool, uint64, bool, error) {
	return s.ingestUpFrame(epoch, offset, len(data), sha, data, reserved, true)
}

func (s *Session) ingestUpFrame(epoch uint64, offset uint64, length int, sha string, data []byte, reserved, verified bool) (alreadyWritten bool, accepted bool, currentAckUp uint64, consumed bool, err error) {
	// The digest is a pure function of data: verify it before taking the
	// session lock, which ACKs, the backend writer and the downstream path
	// all contend on.
	digestOK := verified
	if !verified && length == len(data) {
		sum := sha256.Sum256(data)
		digestOK = hex.EncodeToString(sum[:]) == sha
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.State == StateClosed {
		return false, false, s.upAckOffset, false, ErrSessionClosed
	}
	if epoch != s.Epoch {
		return false, false, s.upAckOffset, false, ErrStaleEpoch
	}

	if !s.dataPlaneActive || s.barrierPending || (s.State != StateActive && s.State != StateDraining) {
		return false, false, s.upAckOffset, false, ErrNotActive
	}
	if length <= 0 || length != len(data) || offset > math.MaxUint64-uint64(length) {
		return false, false, s.upAckOffset, false, ErrFrameConflict
	}
	if !digestOK {
		return false, false, s.upAckOffset, false, ErrFrameConflict
	}
	frameEnd := offset + uint64(length)

	// Disallow frames beyond FIN final offset
	if s.UpFIN.Seen && frameEnd > s.UpFIN.FinalOffset {
		return false, false, s.upAckOffset, false, ErrFrameConflict
	}

	// Frame size check against negotiated limit if set
	if s.negotiatedMaxFrameSize > 0 && length > s.negotiatedMaxFrameSize {
		return false, false, s.upAckOffset, false, ErrFrameTooLarge
	}

	// 1. Check receipt table for already written duplicate
	exists, conflict, tooOld := s.receiptTable.CheckFrame(offset, length, sha)
	if tooOld {
		return false, false, s.upAckOffset, false, ErrReplayTooOld
	}
	if conflict {
		return false, false, s.upAckOffset, false, ErrFrameConflict
	}
	if exists {
		// Frame is verified duplicate and already written
		return true, true, s.upAckOffset, false, nil
	}

	// If frame was already written (offset+len <= upAckOffset) but dropped from receipts
	if frameEnd <= s.upAckOffset {
		return false, false, s.upAckOffset, false, ErrReplayTooOld
	}
	// a frame straddling upAckOffset re-sends an already written prefix
	// that no receipt can verify. No retry can make it acceptable, so it is a
	// conflict rather than a retryable window error.
	if offset < s.upAckOffset {
		return false, false, s.upAckOffset, false, ErrFrameConflict
	}

	// 2. Check if frame matches currently inflight writing frame
	if s.inflightWriting && s.inflightFrame != nil && s.inflightFrame.Offset == offset {
		if s.inflightFrame.Length == length && s.inflightFrame.SHA256 == sha {
			// Idempotent retry while writing
			return false, true, s.upAckOffset, false, nil
		}
		return false, false, s.upAckOffset, false, ErrFrameConflict
	}

	if s.inflightFrame != nil {
		f := s.inflightFrame
		if offset < f.Offset+uint64(f.Length) && frameEnd > f.Offset {
			return false, false, s.upAckOffset, false, ErrFrameConflict
		}
	}
	if existing, ok := s.upPendingFrames[offset]; ok && existing.Length == length && existing.SHA256 == sha {
		return false, true, s.upAckOffset, false, nil
	}
	if s.receiptTable.Count()+len(s.upPendingFrames)+boolInt(s.inflightWriting) >= s.receiptTable.Capacity() {
		return false, false, s.upAckOffset, false, ErrWindowExceeded
	}
	// 3. Window check: frame end must not exceed upAckOffset + upReceiveWindow
	if s.upAckOffset > math.MaxUint64-s.upReceiveWindow || frameEnd > s.upAckOffset+s.upReceiveWindow {
		return false, false, s.upAckOffset, false, ErrWindowExceeded
	}
	if len(s.upPendingFrames)+boolInt(s.inflightWriting) >= s.maxInflight {
		return false, false, s.upAckOffset, false, ErrWindowExceeded
	}

	// 4. Check already pending frames
	if existing, ok := s.upPendingFrames[offset]; ok {
		if existing.Length == length && existing.SHA256 == sha {
			// Idempotent retry while pending
			return false, true, s.upAckOffset, false, nil
		}
		return false, false, s.upAckOffset, false, ErrFrameConflict
	}

	// Check partial overlaps with pending frames
	for pOff, pFrame := range s.upPendingFrames {
		pEnd := pOff + uint64(pFrame.Length)
		if !(frameEnd <= pOff || offset >= pEnd) {
			return false, false, s.upAckOffset, false, ErrFrameConflict
		}
	}

	if !reserved && s.registry != nil && !s.registry.AcquireGlobalBuffer(int64(length)) {
		return false, false, s.upAckOffset, false, ErrBufferExhausted
	}
	// Enqueue the frame: a verified transport buffer is adopted, anything
	// else is copied because the caller may reuse it.
	frameCopy := data
	if !verified {
		frameCopy = make([]byte, length)
		copy(frameCopy, data)
	}
	s.upPendingFrames[offset] = &InflightUpFrame{
		Offset: offset,
		Length: length,
		SHA256: sha,
		Data:   frameCopy,
	}

	s.retainedBytes.Add(int64(length))
	// Wake up writer
	s.writerCond.Signal()
	return false, true, s.upAckOffset, reserved, nil
}

// runBackendWriter runs the single writer loop writing sequentially to backend DB socket.
func (s *Session) runBackendWriter() {
	for {
		s.mu.Lock()
		for !s.writingClosed && (s.barrierPending || !s.dataPlaneActive || (!s.canFINLocked() && s.upPendingFrames[s.upWrittenOffset] == nil)) {
			s.writerCond.Wait()
		}
		if s.writingClosed {
			s.mu.Unlock()
			return
		}
		f := s.upPendingFrames[s.upWrittenOffset]
		fin := f == nil && s.canFINLocked()
		if f != nil {
			delete(s.upPendingFrames, f.Offset)
		}
		s.inflightWriting = true
		s.inflightFrame = f
		s.inflightDoneCh = make(chan struct{})
		conn := s.BackendConn
		timeout := s.socketWriteTimeout
		s.mu.Unlock()
		var writeErr error
		if fin {
			if c, ok := conn.(interface{ CloseWrite() error }); ok {
				writeErr = c.CloseWrite()
			}
		} else {
			nTotal := 0
			for nTotal < len(f.Data) {
				if timeout > 0 {
					_ = conn.SetWriteDeadline(time.Now().Add(timeout))
				}
				n, e := conn.Write(f.Data[nTotal:])
				if n > 0 {
					nTotal += n
					s.mu.Lock()
					s.upWrittenOffset += uint64(n)
					s.mu.Unlock()
					atomic.AddInt64(&observability.DefaultMetrics.UpBytesTotal, int64(n))
				}
				if e != nil {
					writeErr = e
					break
				}
				if n == 0 {
					writeErr = io.ErrNoProgress
					break
				}
			}
		}
		s.mu.Lock()
		s.inflightWriting = false
		s.inflightFrame = nil
		close(s.inflightDoneCh)
		s.inflightDoneCh = nil
		if f != nil {
			s.retainedBytes.Add(-int64(f.Length))
			if s.registry != nil {
				s.registry.ReleaseGlobalBuffer(int64(f.Length))
			}
		}
		if writeErr != nil {
			if s.State != StateClosed {
				s.closeReason = protocol.ErrBackendLost
			}
			s.closeLocked()
		} else if fin {
			s.UpFIN.Applied = true
			atomic.AddInt64(&observability.DefaultMetrics.UpFINAppliedTotal, 1)
			s.maybeNormalCloseLocked()
		} else {
			s.upAckOffset = s.upWrittenOffset
			if !s.receiptTable.RecordChecked(f.Offset, f.Length, f.SHA256) {
				// admission keeps receipts within capacity; an
				// overflow is an accounting bug. Fail loudly instead of
				// silently dropping a receipt and answering later
				// confirmations with BAD_ACK.
				slog.Error("upstream receipt table overflow", "session", s.ID, "capacity", s.receiptTable.Capacity())
				s.closeReason = protocol.ErrFrameConflict
				s.closeLocked()
			}
		}
		s.signalLocked()
		closed := s.State == StateClosed
		s.mu.Unlock()
		if closed {
			s.finalize()
			return
		}
	}
}

func (s *Session) canFINLocked() bool {
	return s.UpFIN.Seen && !s.UpFIN.Applied && s.upWrittenOffset == s.UpFIN.FinalOffset
}

func (s *Session) IngestUpFIN(epoch uint64, finalOffset uint64) (applied bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.State == StateClosed {
		return false, ErrSessionClosed
	}
	if epoch != s.Epoch {
		return false, ErrStaleEpoch
	}

	if s.UpFIN.Seen {
		if s.UpFIN.FinalOffset != finalOffset {
			return false, ErrIdempotencyConflict
		}
		return s.UpFIN.Applied, nil
	}

	if !s.dataPlaneActive || s.barrierPending {
		return false, ErrNotActive
	}
	if s.upAckOffset > math.MaxUint64-s.upReceiveWindow || finalOffset > s.upAckOffset+s.upReceiveWindow {
		return false, ErrWindowExceeded
	}
	for _, f := range s.upPendingFrames {
		if f.Offset+uint64(f.Length) > finalOffset {
			return false, ErrFrameConflict
		}
	}
	if s.inflightFrame != nil && s.inflightFrame.Offset+uint64(s.inflightFrame.Length) > finalOffset {
		return false, ErrFrameConflict
	}
	if finalOffset < s.upWrittenOffset {
		return false, fmt.Errorf("%w: final offset %d is below written offset %d", ErrFrameConflict, finalOffset, s.upWrittenOffset)
	}

	s.UpFIN.Seen = true
	s.UpFIN.FinalOffset = finalOffset
	if s.State == StateActive {
		s.State = StateDraining
	}

	// Signal writer
	s.writerCond.Signal()
	return s.UpFIN.Applied, nil
}

func (s *Session) ConfirmUpAdvancesReceiptFloor(off uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.confirmUpLocked(off)
}

func (s *Session) confirmUpLocked(off uint64) error {
	floor := s.receiptTable.ReceiptFloor()
	if off <= floor {
		return nil
	}
	if off > s.upAckOffset || !s.receiptTable.Boundary(off) {
		return ErrBadAck
	}
	s.receiptTable.ConfirmAdvancesReceiptFloor(off)
	s.writerCond.Broadcast()
	return nil
}
