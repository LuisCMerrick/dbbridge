package ingress

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"dbbridge/internal/observability"
	"dbbridge/internal/transport"
)

type wsCacheEntry struct {
	expires time.Time
	err     error
}

// precheckCall is one shared, in-flight WS precheck. It runs under a context
// owned by the manager, so no single caller's cancellation can decide (and
// poison) the outcome that every waiter receives.
type precheckCall struct {
	done chan struct{}
	err  error
}

// precheckCacheTTL is how long a definitive precheck answer is reused.
const precheckCacheTTL = 30 * time.Second

// precheck answers whether WS works on an endpoint, sharing one in-flight
// check and reusing definitive answers. Only real checks enter the per-
// endpoint phase statistics: cache hits and local contention do not.
func (m *IngressManager) precheck(ctx context.Context, peer string, c *transport.TripleTransportClient, node, inst string) error {
	key := m.precheckKey(peer, c, inst)
	m.preMu.Lock()
	if v, ok := m.cache[key]; ok && time.Now().Before(v.expires) {
		m.preMu.Unlock()
		observability.RecordEvent("ingress", "cache_hit", "success", "ws", "ws")
		return v.err
	}
	call := m.prechecks[key]
	if call == nil {
		observability.RecordEvent("ingress", "cache_miss", "success", "ws", "ws")
		call = &precheckCall{done: make(chan struct{})}
		m.prechecks[key] = call
		m.wg.Add(1)
		go m.runPrecheck(key, call, peer, c, node, inst)
	}
	m.preMu.Unlock()
	// Every caller, the one that started the check included, waits only with
	// its own context; the shared check itself is not bound to any caller.
	select {
	case <-call.done:
		return call.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runPrecheck performs one shared precheck under a context detached from the
// sessions waiting for it, and caches only definitive answers.
func (m *IngressManager) runPrecheck(key string, call *precheckCall, peer string, c *transport.TripleTransportClient, node, inst string) {
	defer m.wg.Done()
	timeout := m.cfg.Transport.ControlRequestTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(m.ctx, timeout)
	defer cancel()
	release, e := m.acquireProbeSlot(ctx, peer)
	if e == nil {
		start := time.Now()
		e = c.PrecheckWS(ctx, node, inst)
		release()
		if m.ctx.Err() == nil {
			m.observePhase(peer, c.URL(), "precheck", start, e == nil)
		}
	}
	m.preMu.Lock()
	for k, v := range m.cache {
		if time.Now().After(v.expires) {
			delete(m.cache, k)
		}
	}
	if ctx.Err() == nil && precheckCacheable(e) {
		m.cache[key] = wsCacheEntry{time.Now().Add(precheckCacheTTL), e}
	}
	call.err = e
	delete(m.prechecks, key)
	close(call.done)
	m.preMu.Unlock()
}

// precheckCacheable reports whether a precheck outcome is a definitive answer
// from the endpoint. Transport-level failures (timeouts, resets, refused dials,
// cancellation, gateway 502/503/504) and local contention are transient and are
// never cached. transport.wsUnavailable may wrap such failures as
// ErrWSUpgradeRejected, so a rejection is only cached when it is not one.
func precheckCacheable(e error) bool {
	if e == nil {
		return true
	}
	// A quota response is transient and must be retried after Retry-After,
	// rather than pinning an otherwise healthy endpoint to a 30-second failure.
	if safeRetry(e) || precheckTransient(e) {
		return false
	}
	// A refusal that declares itself transient (408, 425, 429, 5xx answers
	// of an intermediary) says nothing lasting about the WS path.
	var transient interface{ Transient() bool }
	if errors.As(e, &transient) && transient.Transient() {
		return false
	}
	var remote *transport.RemoteError
	if errors.As(e, &remote) {
		return remote.Trusted && remote.Status < 500
	}
	return true
}

// precheckTransient reports precheck failures that say nothing definitive
// about the endpoint's WS support: local contention, cancellation, or a
// network-level failure (also when wrapped as ErrWSUpgradeRejected).
func precheckTransient(e error) bool {
	if errors.Is(e, errLocalBusy) || errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(e, &ne) && ne.Timeout() {
		return true
	}
	msg := strings.ToLower(e.Error())
	for _, status := range []string{"502", "503", "504"} {
		if strings.HasSuffix(msg, ": status "+status) {
			return true
		}
	}
	for _, marker := range []string{"timeout", "context canceled", "deadline exceeded", "connection refused", "connection reset", "no such host", "unreachable", "broken pipe"} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}
func (m *IngressManager) precheckKey(peer string, c *transport.TripleTransportClient, inst string) string {
	revision := uint64(0)
	if m.auth != nil {
		revision = m.auth.Revision()
	}
	token := sha256.Sum256([]byte(c.GetToken()))
	return fmt.Sprintf("%s|%s|%s|%d|%x", peer, c.URL(), inst, revision, token)
}
func (m *IngressManager) invalidatePrecheck(peer, url, inst string) {
	m.preMu.Lock()
	removed := false
	prefix := peer + "|" + url + "|" + inst + "|"
	for key := range m.cache {
		if strings.HasPrefix(key, prefix) {
			delete(m.cache, key)
			removed = true
		}
	}
	m.preMu.Unlock()
	if removed {
		observability.RecordEvent("ingress", "cache_invalidate", "transport", "ws", "ws")
	}
}
