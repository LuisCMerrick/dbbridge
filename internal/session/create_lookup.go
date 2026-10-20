package session

import (
	"time"

	"dbbridge/internal/protocol"
)

// LookupCreate answers a create from the idempotency table without reserving
// anything. found is false when the table holds no live record for key; the
// caller then reserves resources and calls ReserveCreate. Otherwise status is
// what ReserveCreate would answer for the existing record (pending, failed,
// done or a parameter conflict), so a retry is answered from the table before
// any quota or budget is consulted.
func (r *Registry) LookupCreate(key, targetID string, limits protocol.SessionLimits, recovery protocol.RecoveryNegotiation, transportMode string) (status CreateReserveStatus, cachedResp *protocol.CreateSessionResponse, errCode, errMsg string, found bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expireLocked(time.Now())
	entry, ok := r.createIdempotency[key]
	if !ok {
		return CreateReserved, nil, "", "", false
	}
	switch {
	case entry.TargetID != targetID ||
		entry.Transport != "" && transportMode != "" && entry.Transport != transportMode ||
		entry.Recovery.Enabled != recovery.Enabled ||
		recovery.Enabled && entry.Recovery.GraceMs != recovery.GraceMs ||
		entry.Limits.MaxFrameSize != limits.MaxFrameSize ||
		entry.Limits.UpMaxInflightFrames != limits.UpMaxInflightFrames ||
		entry.Limits.UpWindowBytes != limits.UpWindowBytes ||
		entry.Limits.DownBufferBytes != limits.DownBufferBytes:
		return CreateConflict, nil, protocol.ErrIdempotencyConflict, "idempotency key conflict: parameters differ", true
	case entry.InFlight:
		return CreatePending, nil, protocol.ErrCreatePending, "session creation pending", true
	case entry.ErrCode != "":
		return CreateFailed, nil, entry.ErrCode, entry.ErrMsg, true
	}
	return CreateDone, entry.Response, "", "", true
}
