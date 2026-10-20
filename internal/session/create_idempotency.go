package session

import (
	"time"

	"dbbridge/internal/protocol"
)

type CreateReserveStatus int

const (
	CreateReserved CreateReserveStatus = iota
	CreatePending
	CreateConflict
	CreateDone
	CreateFailed
)

type CreateIdempEntry struct {
	metadataCost int64
	InFlight     bool
	TargetID     string
	Limits       protocol.SessionLimits
	Recovery     protocol.RecoveryNegotiation
	Transport    string
	Response     *protocol.CreateSessionResponse
	ErrCode      string
	ErrMsg       string
	CreatedAt    time.Time
	retainUntil  time.Time // overrides CreatedAt+ttl when set (normal close)
}

func (r *Registry) ReserveCreate(
	key string,
	targetID string,
	limits protocol.SessionLimits,
	recovery protocol.RecoveryNegotiation,
	transportMode ...string,
) (status CreateReserveStatus, cachedResp *protocol.CreateSessionResponse, errCode string, errMsg string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var trMode string
	if len(transportMode) > 0 {
		trMode = transportMode[0]
	}

	now := time.Now()
	r.expireLocked(now)

	// DBB-A11: Check idempotency table FIRST to allow safe retries without rejecting on quota
	entry, ok := r.createIdempotency[key]
	if ok {
		// Conflict check: parameters must match normalized request
		if entry.TargetID != targetID ||
			(entry.Transport != "" && trMode != "" && entry.Transport != trMode) ||
			entry.Recovery.Enabled != recovery.Enabled ||
			(recovery.Enabled && entry.Recovery.GraceMs != recovery.GraceMs) ||
			entry.Limits.MaxFrameSize != limits.MaxFrameSize ||
			entry.Limits.UpMaxInflightFrames != limits.UpMaxInflightFrames ||
			entry.Limits.UpWindowBytes != limits.UpWindowBytes ||
			entry.Limits.DownBufferBytes != limits.DownBufferBytes {
			return CreateConflict, nil, protocol.ErrIdempotencyConflict, "idempotency key conflict: parameters differ"
		}

		if entry.InFlight {
			return CreatePending, nil, protocol.ErrCreatePending, "session creation pending"
		}

		if entry.ErrCode != "" {
			return CreateFailed, nil, entry.ErrCode, entry.ErrMsg
		}

		return CreateDone, entry.Response, "", ""
	}

	// Global max_sessions check (DBB-A11: including both registered and in-flight creates)
	if r.maxSessions > 0 && (len(r.sessions)+r.pendingCreates) >= r.maxSessions {
		return CreateFailed, nil, protocol.ErrQuotaExceeded, "maximum concurrent sessions exceeded"
	}

	if r.draining {
		return CreateFailed, nil, protocol.ErrDraining, "new sessions unavailable"
	}
	if len(r.createIdempotency) >= r.createEntryLimitLocked() {
		return CreateFailed, nil, protocol.ErrQuotaExceeded, "create idempotency table full"
	}
	// Reserve the create record and the compact tombstone (bounded last-resume ID) up front.
	// This lease survives active-session finalize until the tombstone expires.
	cost := int64(createRecordCost + len(key) + len(targetID) + len(trMode))
	if r.metadata != nil && !r.metadata.Try(cost) {
		return CreateFailed, nil, protocol.ErrQuotaExceeded, "idempotency budget exhausted"
	}
	r.pendingCreates++
	r.createIdempotency[key] = &CreateIdempEntry{
		metadataCost: cost,
		InFlight:     true,
		TargetID:     targetID,
		Limits:       limits,
		Recovery:     recovery,
		Transport:    trMode,
		CreatedAt:    now,
	}
	return CreateReserved, nil, "", ""
}

func (r *Registry) CompleteCreate(key string, resp *protocol.CreateSessionResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.scheduleExpiryLocked()
	if entry, ok := r.createIdempotency[key]; ok {
		if entry.InFlight {
			entry.InFlight = false
			if r.pendingCreates > 0 {
				r.pendingCreates--
			}
		}
		entry.Response = resp
		r.createBySession[resp.SessionID] = key
		deadline := entry.CreatedAt.Add(r.ttl)
		// A session that already closed keeps its record as long as its
		// tombstone: the guarantee runs from the end, not the reservation.
		if until, ok := r.tombstoneExpires[resp.SessionID]; ok && r.sessions[resp.SessionID] == nil && until.After(deadline) {
			entry.retainUntil, deadline = until, until
		}
		r.pushExpiryLocked(expireCreate, key, deadline)
	}
}

func (r *Registry) FailCreate(key string, errCode, errMsg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.scheduleExpiryLocked()
	if entry, ok := r.createIdempotency[key]; ok {
		if entry.InFlight {
			entry.InFlight = false
			if r.pendingCreates > 0 {
				r.pendingCreates--
			}
		}
		entry.CreatedAt = time.Now()
		entry.ErrCode = errCode
		entry.ErrMsg = errMsg
		r.pushExpiryLocked(expireCreate, key, entry.CreatedAt.Add(r.ttl))
	}
}

func (r *Registry) CacheCreate(key string, resp *protocol.CreateSessionResponse) {
	r.CompleteCreate(key, resp)
}

func (r *Registry) GetCachedCreate(key string) *protocol.CreateSessionResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.createIdempotency[key]; ok {
		return entry.Response
	}
	return nil
}
