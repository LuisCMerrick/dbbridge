package ingress

import (
	"context"
	"strconv"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/transport"
)

func (s *ingressSession) ack(g *generation, ready bool, nonce string, deadline time.Time) (err error) {
	start := time.Now()
	defer func() { s.m.observePhase(s.p.ID, g.c.URL(), "control", start, err == nil) }()
	s.mu.Lock()
	down, up, fin := s.downStable, s.upAck, s.downFIN
	sentDownFrames, sentUpFrames := s.downFramesUnacked, s.upFramesUnconfirmed
	blocked := !s.rxBlockedAt.IsZero()
	blockReason := s.rxBlockReason
	if nonce == "" {
		nonce = s.heartbeatNonce
	}
	s.mu.Unlock()
	ctx, cancel := context.WithDeadline(g.ctx, deadline)
	defer cancel()
	var resp *protocol.AckResponse
	var e error
	for attempt := 0; ; attempt++ {
		reason := "none"
		if blocked {
			reason = blockReason
			if reason == "" {
				reason = "local_socket_write"
			}
		}
		resp, e = g.c.SendAck(ctx, s.sid, s.token, s.owner, g.ep, protocol.AckRequest{StreamID: g.stream, ReadyReceived: ready, DownDeliveredOffset: strconv.FormatUint(down, 10), UpConfirmedOffset: strconv.FormatUint(up, 10), DownFinApplied: fin, HeartbeatNonce: nonce, ProbeID: generateID(16), DataRxBlocked: blocked, BackpressureReason: reason})
		if !safeRetry(e) || pause(ctx, attempt, e) != nil {
			break
		}
	}
	if e != nil {
		return e
	}
	ack, e := protocol.Offset(resp.AckUpOffset)
	if e != nil {
		return transport.ErrProtocol
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() || s.g != g {
		return context.Canceled
	}
	if e = s.advanceUpLocked(ack); e != nil {
		return e
	}
	s.upFINApplied = s.upFINApplied || resp.UpFinApplied
	s.downFINAcked = s.downFINAcked || resp.DownFinAcked
	s.lastControl = time.Now()
	if resp.DataTxBlocked {
		if s.remoteBlockedAt.IsZero() {
			s.remoteBlockedAt = time.Now()
		}
	} else {
		s.remoteBlockedAt = time.Time{}
	}
	s.remoteTxBlocked = resp.DataTxBlocked
	s.remoteRxBlocked = resp.DataRxBlocked
	if down > s.ackProgress {
		s.ackProgress = down
	}
	s.downFramesUnacked -= min(sentDownFrames, s.downFramesUnacked)
	s.upFramesUnconfirmed -= min(sentUpFrames, s.upFramesUnconfirmed)
	s.wake()
	return nil
}
func (s *ingressSession) control(g *generation) {
	interval := controlInterval(s.m.cfg.Transport)
	idle := controlIdleInterval(s.m.cfg.Transport)
	t := time.NewTimer(interval)
	defer t.Stop()
	for {
		s.mu.Lock()
		nonce := s.heartbeatNonce
		s.mu.Unlock()
		if e := s.ack(g, false, nonce, time.Now().Add(s.m.cfg.Transport.ControlRequestTimeout)); e != nil {
			s.fault(g, e, false)
			return
		}
		s.mu.Lock()
		normal := s.upEOF && s.upFINApplied && s.downFIN && s.downFINAcked
		s.mu.Unlock()
		if normal {
			s.fault(g, errClosedNormal, true)
			return
		}
		// ack_max_delay bounds how long something unacknowledged may wait. With
		// nothing outstanding, only a keepalive runs until state changes.
		deadline := time.Now().Add(idle)
		armed := false
	wait:
		for {
			s.mu.Lock()
			if !armed && s.ackOutstandingLocked(nonce) {
				armed = true
				if d := time.Now().Add(interval); d.Before(deadline) {
					deadline = d
				}
			}
			var changed chan struct{}
			if !armed {
				changed = s.changed
			}
			s.mu.Unlock()
			t.Reset(time.Until(deadline))
			select {
			case <-g.ctx.Done():
				return
			case <-t.C:
				break wait
			case <-s.ackNotify:
				break wait
			case <-changed:
			}
		}
	}
}

// ackOutstandingLocked reports whether the control channel has something to
// tell the peer (or learn from it) since the last ACK that carried sentNonce.
func (s *ingressSession) ackOutstandingLocked(sentNonce string) bool {
	return s.downStable > s.ackProgress ||
		s.downFIN && !s.downFINAcked ||
		s.upEOF && !s.upFINApplied ||
		s.upNext > s.upAck ||
		s.heartbeatNonce != sentNonce ||
		!s.rxBlockedAt.IsZero() || s.remoteTxBlocked || s.remoteRxBlocked
}

// defaultDownFramesAckThreshold makes the control writer acknowledge
// downstream data after this many frames even when ack_batch_bytes is not
// reached. Egress caps retained downstream frames per session, so small
// frames exhaust it long before the byte threshold. 32 stays far
// below the cap of every egress that does not advertise one (512).
const defaultDownFramesAckThreshold = 32

// downFramesAckThreshold uses the egress-advertised frame cap when the
// flow-limits extension was negotiated: a quarter of it, and never more than
// the default.
func downFramesAckThreshold(l protocol.SessionLimits) int {
	if l.DownMaxFrames > 0 {
		return max(1, min(defaultDownFramesAckThreshold, l.DownMaxFrames/4))
	}
	return defaultDownFramesAckThreshold
}

// downBytesAckThreshold is ack_batch_bytes, lowered to half the negotiated
// downstream window so a window smaller than the batch never waits for the
// ACK timer.
func downBytesAckThreshold(batch int64, l protocol.SessionLimits) uint64 {
	if batch <= 0 {
		return 0 // acknowledge every frame, as before
	}
	if l.DownBufferBytes > 1 {
		batch = min(batch, l.DownBufferBytes/2)
	}
	return uint64(batch)
}

// upFramesAckThreshold returns how many egress-acknowledged but unconfirmed
// upstream frames trigger an immediate control ACK. Over WS only the control
// ACK trims the egress receipt table. Without an advertised limit it relies
// on the egress config invariant receipt_limit >= 2*inflight+8, so
// inflight+4 is below it; with the flow-limits extension it also stays at
// or below half of the advertised limit.
func upFramesAckThreshold(l protocol.SessionLimits) int {
	t := max(l.UpMaxInflightFrames, 1) + 4
	if l.UpReceiptLimit > 0 {
		t = max(1, min(t, l.UpReceiptLimit/2))
	}
	return t
}

func controlInterval(c config.TransportConfig) time.Duration {
	ack, hb := c.AckMaxDelay, c.Heartbeat
	if ack <= 0 {
		ack = 200 * time.Millisecond
	}
	if hb <= 0 {
		hb = 15 * time.Second
	}
	return min(ack, hb)
}

// controlIdleInterval is the keepalive period of the control channel while
// nothing is outstanding; heartbeat pings re-arm it as they arrive.
func controlIdleInterval(c config.TransportConfig) time.Duration {
	hb, detect := c.Heartbeat, c.FailureDetectTimeout
	if hb <= 0 {
		hb = 15 * time.Second
	}
	if detect <= 0 {
		detect = 45 * time.Second
	}
	return max(controlInterval(c), min(hb, detect/3))
}
