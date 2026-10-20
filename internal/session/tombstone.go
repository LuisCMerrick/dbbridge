package session

import (
	"sync"
	"time"

	"dbbridge/internal/stream"
)

// createRecordCost is the metadata reserved for one create idempotency record
// together with the compact tombstone its session leaves behind. The
// record holds the cached create response; the tombstone holds the identifiers,
// offsets, FIN state and the bounded (<=128 byte) last resume request ID.
// Neither keeps the closed *Session, its buffers or its timers alive.
const createRecordCost = 1536

// tombstone is the immutable terminal view of a closed session. GetAny builds a
// throwaway closed *Session from it so callers keep their existing API.
type tombstone struct {
	id, token, instance, owner, target string
	closeReason, transport, streamID   string
	lastResumeID                       string
	epoch                              uint64
	upWritten, upAck                   uint64
	downProduced, downAck              uint64
	upFIN, downFIN                     FINState
	resumeEnabled                      bool
	createdAt                          time.Time
}

func (s *Session) tombstoneSnapshot() *tombstone {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &tombstone{
		id: s.ID, token: s.SessionToken, instance: s.InstanceID, owner: s.OwnerPeer, target: s.TargetID,
		closeReason: s.closeReason, transport: s.transportMode, streamID: s.currentStreamID,
		lastResumeID: s.lastResumeRequestID, epoch: s.Epoch,
		upWritten: s.upWrittenOffset, upAck: s.upAckOffset,
		downProduced: s.downProducedOffset, downAck: s.downAckOffset,
		upFIN: s.UpFIN, downFIN: s.DownFIN, resumeEnabled: s.ResumeEnabled, createdAt: s.CreatedAt,
	}
}

// view returns a closed, already finalized Session that answers every request
// path the same way the original closed session did (ACK on a normally closed
// stream, DELETE, terminal codes) without retaining its resources.
func (t *tombstone) view() *Session {
	done := make(chan struct{})
	close(done)
	v := &Session{
		ID: t.id, SessionToken: t.token, InstanceID: t.instance, OwnerPeer: t.owner, TargetID: t.target,
		closeReason: t.closeReason, State: StateClosed, Epoch: t.epoch, CreatedAt: t.createdAt,
		ResumeEnabled: t.resumeEnabled, transportMode: t.transport, currentStreamID: t.streamID,
		lastResumeRequestID: t.lastResumeID,
		upWrittenOffset:     t.upWritten, upAckOffset: t.upAck,
		downProducedOffset: t.downProduced, downAckOffset: t.downAck,
		UpFIN: t.upFIN, DownFIN: t.downFIN,
		receiptTable:  stream.NewReceiptTable(1),
		downBuffer:    stream.NewByteWindow(t.downAck, 0),
		done:          done,
		writingClosed: true,
	}
	v.writerCond = sync.NewCond(&v.mu)
	v.downCond = sync.NewCond(&v.mu)
	v.closed.Store(true)
	v.finalized.Store(true)
	return v
}

// createEntryLimitLocked bounds the create idempotency table. With a metadata
// budget the budget is the real bound, so the table may hold as many records as
// it can pay for; without one the historical fixed cap applies.
//
// The configured idempotency_tombstone_ttl (>= 10min) is the single retention
// guarantee for every terminal outcome: normal close, abnormal close and create
// failure (5.3(7), 5.10). There is deliberately no shorter normal-close TTL:
// when metadata or the table cap is exhausted ReserveCreate
// rejects the NEW key with DBB_QUOTA_EXCEEDED; records inside the guarantee are
// never evicted early.
func (r *Registry) createEntryLimitLocked() int {
	if r.metadata == nil || r.metadata.Limit() <= 0 {
		return maxCreateIdempotencyEntries
	}
	return int(min(max(r.metadata.Limit()/createRecordCost, maxCreateIdempotencyEntries), 1<<20))
}
