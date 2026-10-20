package egress

import (
	"dbbridge/internal/auth"
	"dbbridge/internal/protocol"
	"errors"
	"net/http"
	"time"
)

// createLease owns everything a create takes before it registers the
// session, and gives it all back unless commit is called.
type createLease struct {
	s                 *Server
	peer, target, key string
	metadataCost      int64
	tracked           bool // holds a reference on key in the per-peer idempotency tracker
	quota, slot, meta bool
	reserved, failed  bool // this request reserved the table record; it failed
	final             bool // the record's outcome was handed to the tracker
}

// acquire takes the per-peer idempotency allowance, the peer session quota,
// a session slot and the session metadata. It returns a zero status on
// success, or the status, code and message of the refusal.
func (l *createLease) acquire() (int, string, string) {
	s := l.s
	if !l.tracked {
		if _, ok := s.idem.admit(l.peer, l.key, s.idemLimits(l.peer), time.Now()); !ok {
			return 429, protocol.ErrQuotaExceeded, "peer idempotency quota"
		}
		l.tracked = true
	}
	if e := s.auth.AuthorizeAndReserveTarget(l.peer, l.target); e != nil {
		if errors.Is(e, auth.ErrMaxSessions) {
			return 429, protocol.ErrQuotaExceeded, "peer session quota"
		}
		return 403, protocol.ErrTargetForbidden, "creation refused"
	}
	l.quota = true
	if !s.cfg.Resources.TryEgressSession() {
		return 429, protocol.ErrQuotaExceeded, "session quota"
	}
	l.slot = true
	if !s.cfg.Resources.TryEgressMetadata(l.metadataCost) {
		return 429, protocol.ErrQuotaExceeded, "session metadata budget"
	}
	l.meta = true
	return 0, "", ""
}

// reserve records that this request owns the table record for its key.
func (l *createLease) reserve() {
	l.reserved = true
	if l.tracked {
		l.s.idem.claim(l.peer, l.key)
	}
}

// fail caches a refusal discovered after the table reservation and answers.
func (l *createLease) fail(w http.ResponseWriter, st int, code string) {
	l.s.registry.FailCreate(l.key, code, "creation refused")
	l.failed = true
	writeJSONError(w, st, code, "creation refused", false, st == 429 || st == 503)
}

// commit hands the resources to the registered session. The record stays
// charged to the peer while the session lives and, through the session's
// finalize hook, for the tombstone TTL afterwards.
func (l *createLease) commit() {
	l.quota, l.slot, l.meta, l.final = false, false, false, true
	if l.tracked {
		l.s.idem.live(l.peer, l.key)
	}
}

func (l *createLease) rollback() {
	s := l.s
	if l.quota {
		s.auth.DecrSession(l.peer)
	}
	if l.slot {
		s.cfg.Resources.Sessions.Put(1)
	}
	if l.meta {
		s.cfg.Resources.Metadata.Put(l.metadataCost)
	}
	l.quota, l.slot, l.meta = false, false, false
	if !l.tracked {
		return
	}
	if l.reserved && l.failed && !l.final {
		// The registry keeps the failed record for its TTL; keep it counted.
		s.idem.failed(l.peer, l.key, time.Now().Add(s.idemTTL()))
		l.final = true
	}
	l.tracked = false
	s.idem.release(l.peer, l.key)
}

func (s *Server) idemTTL() time.Duration {
	if s.cfg.Transport.IdempotencyTombstoneTTL > 0 {
		return s.cfg.Transport.IdempotencyTombstoneTTL
	}
	return 10 * time.Minute
}

// recordCostEstimate is the create record budget one record is assumed to
// take when the budget is turned into a record count: the registry's fixed
// record and tombstone cost plus room for the key and target identifiers.
const recordCostEstimate = 2 << 10

// idemLimits returns peer's bounds on retained create records: its share of
// the create record budget, and the in-flight/failed allowance.
func (s *Server) idemLimits(peer string) idemLimits {
	capacity := s.records.Limit() / recordCostEstimate
	return idemLimits{
		total:   int(max(s.auth.PeerShareOf(peer, capacity), 1)),
		pending: peerIdemLimit(s.auth.PeerMaxSessions(peer)),
	}
}
