package egress

import (
	"sync"
	"sync/atomic"

	"dbbridge/internal/auth"
	"dbbridge/internal/observability"
	"dbbridge/internal/resource"
)

// reverseShares counts the requests each reverse peer holds per budget, so
// one reverse peer cannot take every request slot (reverse peers are not
// inbound identities and have no share in the authenticator).
type reverseShares struct {
	mu     sync.Mutex
	active map[string]int64
}

func (r *reverseShares) take(key string, n, limit int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active == nil {
		r.active = map[string]int64{}
	}
	if r.active[key]+n > limit {
		return false
	}
	r.active[key] += n
	return true
}

func (r *reverseShares) put(key string, n int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active[key] -= n; r.active[key] <= 0 {
		delete(r.active, key)
	}
}

// admitReverse admits one authenticated request of reverse peer on the same
// budgets as the inbound API: a data or control request slot and the
// request's metadata, each within the peer's fair share among the reverse
// peers. Every refusal is retryable; the database side retries the request.
func (s *Server) admitReverse(peer string, control bool, meta int64) (func(), bool) {
	res := s.cfg.Resources
	if res == nil {
		return func() {}, true
	}
	peers := 0
	for _, p := range s.cfg.Peers {
		if p.IsReverse() {
			peers++
		}
	}
	requests, kind := res.DataRequests, "data"
	if control {
		requests, kind = res.ControlRequests, "control"
	}
	var releases []func()
	release := func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}
	take := func(key string, b *resource.Budget, limit int64, n int64, try func(int64) bool) bool {
		key = peer + "|" + key
		if !s.reverseShares.take(key, n, auth.PeerShare(limit, peers)) {
			return false
		}
		if !try(n) {
			s.reverseShares.put(key, n)
			return false
		}
		releases = append(releases, func() { b.Put(n); s.reverseShares.put(key, n) })
		return true
	}
	if !take(kind, requests, requests.Limit(), 1, requests.Try) {
		atomic.AddInt64(&observability.DefaultMetrics.AdmissionRejected, 1)
		return nil, false
	}
	meta = max(meta, 1)
	if !take("metadata", res.Metadata, res.EgressMetadataLimit(), meta, res.TryEgressMetadata) {
		release()
		return nil, false
	}
	var once sync.Once
	return func() { once.Do(release) }, true
}
