package egress

import (
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"errors"
	"io"
	"time"
)

// reservePoll is the retry period of a reader blocked on a payload budget
// that cannot notify; reserveSafetyPoll backs up the release notification.
const (
	reservePoll       = 10 * time.Millisecond
	reserveSafetyPoll = 250 * time.Millisecond
)

func (s *Server) runBackendReader(sess *session.Session, notify chan struct{}) {
	stall := s.cfg.Transport.BackpressureTimeout
	if stall <= 0 {
		stall = 120 * time.Second
	}
	sess.SetBackpressureTimeout(stall)
	base := s.cfg.Transport.FrameSize
	if base <= 0 {
		base = 32 << 10
	}
	maxFrame := sess.NegotiatedMaxFrameSize()
	if maxFrame <= 0 {
		maxFrame = base
	}
	base = min(base, maxFrame)
	size := base
	// A reader blocked on the global payload budget parks on that budget's
	// release notification instead of polling. The ticker is a slow safety
	// net (and the only wake-up when no budget can notify); it is created
	// once per reader, not per wait.
	var retry *time.Ticker
	defer func() {
		if retry != nil {
			retry.Stop()
		}
	}()
	reserve := func(n int64) bool {
		if sess.ReserveDownRead(n) {
			return true
		}
		until := time.NewTimer(stall)
		defer until.Stop()
		for {
			ch, pch, stopWatch, notifies := sess.WatchDownReserve()
			if sess.ReserveDownRead(n) {
				stopWatch()
				return true
			}
			period := reserveSafetyPoll
			if !notifies {
				period = reservePoll
			}
			if retry == nil {
				retry = time.NewTicker(period)
			} else {
				retry.Reset(period)
			}
			select {
			case <-sess.Done():
				stopWatch()
				return false
			case <-until.C:
				stopWatch()
				// A suspended or resuming session is bounded by the
				// recovery grace and Retention, not by backpressure: re-arm
				// instead of killing it before the grace ends.
				if sess.Recovering() {
					until.Reset(stall)
					continue
				}
				sess.CloseWithReason(protocol.ErrBufferExhausted)
				return false
			case <-ch:
			case <-pch:
			case <-retry.C:
			}
			stopWatch()
		}
	}
	for {
		if !sess.WaitDownCapacity(int64(size)) {
			return
		}
		if !reserve(int64(size)) {
			return
		}
		buf := make([]byte, size)
		n, e := sess.BackendConn.Read(buf)
		if _, ae := sess.FinishDownRead(buf[:n]); ae != nil {
			sess.Close()
			return
		}
		if e != nil {
			if errors.Is(e, io.EOF) {
				sess.MarkDownFinal(sess.DownProducedOffset())
				select {
				case notify <- struct{}{}:
				default:
				}
			} else {
				s.loseBackend(sess)
			}
			return
		}
		if n == 0 {
			sess.CloseWithReason(protocol.ErrBackendLost)
			return
		}
		if n == size {
			size = min(size*2, maxFrame)
		} else {
			size = base
		}
	}
}

// loseBackend is the single decision point for a fatal (non-EOF) backend read
// error such as a reset. The
// caller has already settled the n bytes returned with the error through
// FinishDownRead, so the produced offset is exact. The original database
// socket is no longer usable: the session is marked BACKEND_LOST, closed and
// its backend socket released under the session state latch, so a concurrent
// or later /resume sees a terminal state and can never commit a new epoch.
// Only a normal EOF drains (FIN semantics); this path sends no FIN and does
// not wait for downstream ACKs or socket_write_timeout.
func (s *Server) loseBackend(sess *session.Session) {
	sess.CloseWithReason(protocol.ErrBackendLost)
}
