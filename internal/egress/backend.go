package egress

import (
	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
	"errors"
	"io"
	"time"
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
	// One reusable retry ticker per reader instead of a time.After timer
	// allocation every 10 ms while the payload budget is exhausted.
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
		if retry == nil {
			retry = time.NewTicker(10 * time.Millisecond)
		} else {
			retry.Reset(10 * time.Millisecond)
		}
		for !sess.ReserveDownRead(n) {
			select {
			case <-sess.Done():
				return false
			case <-until.C:
				// A suspended or resuming session is bounded by the
				// recovery grace and Retention, not by backpressure: re-arm
				// instead of killing it before the grace ends.
				if sess.Recovering() {
					until.Reset(stall)
					continue
				}
				sess.CloseWithReason(protocol.ErrBufferExhausted)
				return false
			case <-retry.C:
			}
		}
		return true
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
