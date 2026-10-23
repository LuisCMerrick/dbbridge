package ingress

import (
	"context"
	"io"
	"math"
	"sync/atomic"
	"time"

	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
)

type retainedChunk struct {
	offset uint64
	data   []byte
	cost   int64
	refs   int
	acked  bool
}

func (s *ingressSession) advanceUpLocked(off uint64) error {
	if off <= s.upAck {
		return nil
	}
	if off > s.upNext {
		return transport.ErrProtocol
	}
	boundary := false
	for _, f := range s.frames {
		if f.offset+uint64(len(f.data)) == off {
			boundary = true
		}
	}
	if !boundary {
		return transport.ErrProtocol
	}
	s.upAck = off
	if g := s.g; g != nil {
		if g.upSentOffset > off {
			g.upPendingSince = time.Now()
		} else {
			g.upPendingSince = time.Time{}
		}
	}
	keep := s.frames[:0]
	for _, f := range s.frames {
		if f.offset+uint64(len(f.data)) <= off {
			f.acked = true
			for _, g := range s.all {
				delete(g.sent, f)
			}
			if f.refs == 0 {
				s.releaseFrameLocked(f)
			}
		} else {
			keep = append(keep, f)
		}
	}
	clear(s.frames[len(keep):])
	s.frames = keep
	return nil
}
func (s *ingressSession) releaseFrameLocked(f *retainedChunk) {
	if f.cost > 0 {
		s.m.cfg.Resources.Payload.Put(f.cost)
		s.used -= f.cost
		s.retainedBytes.Add(-f.cost)
		f.cost = 0
	}
	f.data = nil
}
func (s *ingressSession) readLocal() {
	defer s.readerWG.Done()
	baseFrame := min(s.m.cfg.Transport.FrameSize, s.limits.MaxFrameSize)
	maxFrame := baseFrame
	if maxFrame <= 0 {
		maxFrame = s.limits.MaxFrameSize
	}
	var blockedSince time.Time
	for {
		s.mu.Lock()
		ch := s.changed
		// While a recovery is in progress there is no active generation; that
		// wait is bounded by recovery.grace and the deadline timer, not by
		// backpressure_timeout.
		recovering := !s.closed.Load() && (s.g == nil || !s.g.active) && !s.deadline.IsZero()
		if maxFrame > baseFrame && baseFrame > 0 {
			winRem := int64(s.limits.UpWindowBytes) - int64(s.upNext-s.upAck)
			bufRem := s.m.cfg.Transport.BufferPerDirection - s.used
			avail := min(winRem, bufRem)
			if avail >= int64(baseFrame) && avail < int64(maxFrame) {
				maxFrame = int(avail)
			}
		}
		can := !s.closed.Load() && s.g != nil && s.g.active && len(s.frames) < s.limits.UpMaxInflightFrames && s.upNext-s.upAck+uint64(maxFrame) <= uint64(s.limits.UpWindowBytes) && s.used+int64(maxFrame) <= s.m.cfg.Transport.BufferPerDirection
		s.mu.Unlock()
		if recovering {
			blockedSince = time.Time{}
			select {
			case <-s.ctx.Done():
				return
			case <-ch:
				continue
			}
		}
		if !can {
			if blockedSince.IsZero() {
				blockedSince = time.Now()
				atomic.AddInt64(&observability.DefaultMetrics.BackpressureTotal, 1)
			}
			remaining := s.m.cfg.Transport.BackpressureTimeout - time.Since(blockedSince)
			if remaining <= 0 {
				s.fault(nil, observability.FaultUpstream, context.DeadlineExceeded, true)
				return
			}
			timer := time.NewTimer(remaining)
			select {
			case <-s.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				s.fault(nil, observability.FaultUpstream, context.DeadlineExceeded, true)
				return
			case <-ch:
				timer.Stop()
				continue
			}
		}
		blockedSince = time.Time{}
		ctx, cancel := context.WithTimeout(s.ctx, s.m.cfg.Transport.BackpressureTimeout)
		e := s.m.cfg.Resources.Payload.Take(ctx, int64(maxFrame))
		cancel()
		if e != nil {
			s.fault(nil, observability.FaultUpstream, e, true)
			return
		}
		// The payload budget is reserved before the read (backpressure), but
		// only bytes actually read count toward the recovery lease.
		buf := make([]byte, maxFrame)
		n, e := s.conn.Read(buf)
		capacity := maxFrame
		s.mu.Lock()
		if n > 0 && !s.closed.Load() {
			if s.upNext > math.MaxUint64-uint64(n) {
				s.mu.Unlock()
				s.m.cfg.Resources.Payload.Put(int64(maxFrame))
				s.fault(nil, observability.FaultUpstream, transport.ErrProtocol, true)
				return
			}
			data := buf[:n]
			cost := int64(capacity)
			if n*4 <= capacity {
				// As in egress FinishDownRead, compact short reads and return
				// the unused reservation so small queries do not pin full-frame
				// allocations in the window and recovery lease.
				data = make([]byte, n)
				copy(data, buf[:n])
				s.m.cfg.Resources.Payload.Put(int64(capacity - n))
				cost = int64(n)
			}
			f := &retainedChunk{offset: s.upNext, data: data, cost: cost}
			s.frames = append(s.frames, f)
			s.used += f.cost
			s.retainedBytes.Add(f.cost)
			s.upNext += uint64(n)
		} else {
			s.m.cfg.Resources.Payload.Put(int64(maxFrame))
		}
		if e == io.EOF {
			s.upEOF = true
		}
		s.wake()
		s.mu.Unlock()
		s.m.cfg.Resources.Suspended.Enforce()
		if n == capacity {
			maxFrame = min(capacity*2, s.limits.MaxFrameSize)
		} else {
			maxFrame = baseFrame
		}
		if e != nil {
			if e != io.EOF {
				s.fault(nil, observability.FaultUpstream, localSocketError(e), true)
			}
			return
		}
	}
}
func (s *ingressSession) send(g *generation) {
	finSent := false
	for {
		s.mu.Lock()
		if s.g != g || !g.active || s.closed.Load() {
			s.mu.Unlock()
			return
		}
		ch := s.changed
		var pending []*retainedChunk
		for _, f := range s.frames {
			if !g.sent[f] {
				g.sent[f] = true
				f.refs++
				pending = append(pending, f)
			}
		}
		eof, final, down, confirmed := s.upEOF, s.upNext, s.downStable, s.upAck
		s.mu.Unlock()
		for index, f := range pending {
			if g.mode == "ws" {
				end := f.offset + uint64(len(f.data))
				cost := int64(len(f.data) + protocol.WSBinaryHeaderLen)
				e := s.m.cfg.Resources.Codec.Take(g.ctx, cost)
				if e == nil {
					e = g.ws.WriteBinaryFrame(g.ep, f.offset, f.data)
					s.m.cfg.Resources.Codec.Put(cost)
				}
				s.mu.Lock()
				if e == nil && s.g == g && g.active && !s.closed.Load() && end > s.upAck {
					g.upSentOffset = max(g.upSentOffset, end)
					if g.upPendingSince.IsZero() {
						g.upPendingSince = time.Now()
					}
				}
				f.refs--
				if f.acked && f.refs == 0 {
					s.releaseFrameLocked(f)
				}
				s.mu.Unlock()
				if e != nil {
					s.mu.Lock()
					for _, tail := range pending[index+1:] {
						tail.refs--
						if tail.acked && tail.refs == 0 {
							s.releaseFrameLocked(tail)
						}
					}
					s.mu.Unlock()
					s.fault(g, observability.FaultUpstream, e, false)
					return
				}
			} else {
				g.wg.Add(1)
				go func(f *retainedChunk) {
					defer g.wg.Done()
					defer func() {
						s.mu.Lock()
						f.refs--
						if f.acked && f.refs == 0 {
							s.releaseFrameLocked(f)
						}
						s.wake()
						s.mu.Unlock()
					}()
					end := f.offset + uint64(len(f.data))
					markPending := func() {
						if g.active && !s.closed.Load() && end > s.upAck {
							g.upSentOffset = max(g.upSentOffset, end)
							if g.upPendingSince.IsZero() {
								g.upPendingSince = time.Now()
							}
						}
					}
					for attempt := 0; g.ctx.Err() == nil; attempt++ {
						r, e := g.c.SendUpFrame(g.ctx, s.sid, s.token, s.owner, g.ep, f.offset, f.data, down, confirmed)
						if e == nil {
							off, pe := protocol.Offset(r.AckUpOffset)
							s.mu.Lock()
							if s.g == g && pe == nil {
								pe = s.advanceUpLocked(off)
								markPending()
								s.wake()
							}
							s.mu.Unlock()
							if pe != nil {
								s.fault(g, observability.FaultUpstream, pe, true)
							}
							return
						}
						// A retryable answer, or a local connection/request budget
						// that ran out before the request left this node, is
						// retried on the same carrier; the carrier did not fail.
						// failure_detect_timeout still bounds the wait for the ACK.
						if safeRetry(e) || localResourceError(e) {
							s.mu.Lock()
							if s.g == g {
								markPending()
							}
							s.mu.Unlock()
							if pause(g.ctx, attempt, e) == nil {
								continue
							}
						}
						s.fault(g, observability.FaultUpstream, e, false)
						return
					}
				}(f)
			}
		}
		if eof && !finSent {
			ctx, cancel := context.WithTimeout(g.ctx, s.m.cfg.Transport.ControlRequestTimeout)
			var e error
			for attempt := 0; ; attempt++ {
				_, e = g.c.SendFin(ctx, s.sid, s.token, s.owner, g.ep, final)
				if !safeRetry(e) && !localResourceError(e) || pause(ctx, attempt, e) != nil {
					break
				}
			}
			cancel()
			if e != nil {
				s.fault(g, observability.FaultUpstream, e, false)
				return
			}
			finSent = true
			s.mu.Lock()
			s.notifyAckLocked(true)
			s.mu.Unlock()
		}
		select {
		case <-g.ctx.Done():
			return
		case <-ch:
		}
	}
}
