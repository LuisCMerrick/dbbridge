package ingress

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
)

func (s *ingressSession) receive(g *generation) {
	var release func()
	defer func() {
		if release != nil {
			release()
		}
		// Early messages buffered during attach that were never processed
		// still hold codec budget.
		for _, p := range g.pending {
			if p.release != nil {
				p.release()
			}
		}
		g.pending = nil
	}()
	for {
		if release != nil {
			release()
			release = nil
		}
		var msg wireMessage
		var e error
		if len(g.pending) > 0 {
			msg = g.pending[0]
			g.pending = g.pending[1:]
		} else {
			msg, e = s.readWire(g, time.Time{})
		}
		if e != nil {
			s.fault(g, e, false)
			return
		}
		release = msg.release
		switch msg.kind {
		case "ping":
			if msg.ctrl.Nonce == "" {
				s.fault(g, transport.ErrProtocol, true)
				return
			}
			s.mu.Lock()
			if s.g == g && s.heartbeatNonce != msg.ctrl.Nonce {
				s.heartbeatNonce = msg.ctrl.Nonce
				// Arms the control writer's ack_max_delay timer for the reply.
				s.wake()
			}
			s.mu.Unlock()
		case "ack_up":
			off, e := protocol.Offset(msg.ctrl.AckUpOffset)
			s.mu.Lock()
			if s.g == g && e == nil {
				before := len(s.frames)
				e = s.advanceUpLocked(off)
				s.upFramesUnconfirmed += before - len(s.frames)
				s.upFINApplied = s.upFINApplied || msg.ctrl.UpFinApplied
				s.notifyAckLocked(msg.ctrl.UpFinApplied || msg.ctrl.AckRequested || s.upNext-s.upAck >= uint64(s.limits.UpWindowBytes) || s.upFramesUnconfirmed >= upFramesAckThreshold(s.limits))
				s.wake()
			}
			s.mu.Unlock()
			if e != nil {
				s.fault(g, e, true)
				return
			}
		case "data", "fin":
			select {
			case <-g.ctx.Done():
				return
			case <-s.gate:
			}
			s.mu.Lock()
			valid := s.g == g && g.active && !s.closed.Load()
			want := s.downActual
			alreadyFin := s.downFIN
			s.mu.Unlock()
			if !valid {
				s.gate <- struct{}{}
				return
			}
			if alreadyFin {
				s.gate <- struct{}{}
				s.fault(g, transport.ErrProtocol, true)
				return
			}
			if msg.kind == "data" {
				if msg.offset != want {
					s.gate <- struct{}{}
					s.fault(g, transport.ErrProtocol, true)
					return
				}
				s.mu.Lock()
				s.rxBlockedAt = time.Now()
				s.rxBlockReason = "local_socket_write"
				s.mu.Unlock()
				written := 0
				for written < len(msg.data) {
					s.conn.SetWriteDeadline(time.Now().Add(s.m.cfg.Transport.SocketWriteTimeout))
					n, we := s.conn.Write(msg.data[written:])
					if n > 0 {
						written += n
						s.mu.Lock()
						s.downActual += uint64(n)
						s.rxBlockedAt = time.Now()
						atomic.AddInt64(&observability.DefaultMetrics.DownBytesTotal, int64(n))
						s.mu.Unlock()
					}
					if we != nil || n == 0 {
						if we == nil {
							we = io.ErrNoProgress
						}
						s.gate <- struct{}{}
						s.fault(g, localSocketError(we), true)
						return
					}
				}
				s.mu.Lock()
				s.downStable = s.downActual
				s.rxBlockedAt = time.Time{}
				s.rxBlockReason = ""
				s.downFramesUnacked++
				s.notifyAckLocked(s.downStable-s.ackProgress >= downBytesAckThreshold(int64(s.m.cfg.Transport.AckBatchBytes), s.limits) || s.downFramesUnacked >= downFramesAckThreshold(s.limits))
				s.wake()
				s.mu.Unlock()
			} else {
				off, e := protocol.Offset(msg.ctrl.FinalOffset)
				if e != nil || off != want {
					s.gate <- struct{}{}
					s.fault(g, transport.ErrProtocol, true)
					return
				}
				if c, ok := s.conn.(interface{ CloseWrite() error }); ok {
					if e = c.CloseWrite(); e != nil {
						s.gate <- struct{}{}
						s.fault(g, localSocketError(e), true)
						return
					}
				}
				s.mu.Lock()
				s.downFIN = true
				s.notifyAckLocked(true)
				s.wake()
				s.mu.Unlock()
			}
			s.gate <- struct{}{}
		case "error":
			s.fault(g, fmt.Errorf("remote %s", msg.ctrl.Code), true)
			return
		default:
			s.fault(g, transport.ErrProtocol, true)
			return
		}
	}
}
