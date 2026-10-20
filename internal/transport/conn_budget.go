package transport

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"dbbridge/internal/resource"
)

// ErrLocalConnBudget reports that a request could not get a local request or
// connection slot (per-peer or process-wide budget) before its own deadline.
// It says nothing about the peer or the carrier: callers should treat it as
// local backpressure, not as a transport fault of the endpoint. When the
// request deadline ran out at the same moment the error also matches
// context.DeadlineExceeded; check ErrLocalConnBudget first.
var ErrLocalConnBudget = errors.New("local connection budget exhausted")

// budgetEvictInterval is how often a dial still waiting for a connection slot
// closes idle connections again, so slots parked in idle pools are reclaimed
// without closing them on every miss.
const budgetEvictInterval = 250 * time.Millisecond

type dialWaitKey struct{}

// dialWait travels as a context value from a request to the dial net/http
// runs for it. net/http dials on a context detached from the request's
// cancellation and deadline but keeping its values, so the dial learns the
// request's deadline from here and records whether it was stuck waiting for
// a connection slot.
type dialWait struct {
	deadline time.Time
	blocked  atomic.Bool
}

func withDialWait(ctx context.Context, fallback time.Duration) (context.Context, *dialWait) {
	w := &dialWait{}
	if dl, ok := ctx.Deadline(); ok {
		w.deadline = dl
	} else if fallback > 0 {
		w.deadline = time.Now().Add(fallback)
	}
	return context.WithValue(ctx, dialWaitKey{}, w), w
}

func dialWaitFrom(ctx context.Context) *dialWait {
	w, _ := ctx.Value(dialWaitKey{}).(*dialWait)
	return w
}

// localBudgetError marks err as ErrLocalConnBudget when it came from a wait
// for a local slot (ctx is the request context). A caller's own cancellation
// stays a plain cancellation.
func localBudgetError(ctx context.Context, err error, w *dialWait) error {
	if err == nil || errors.Is(err, ErrLocalConnBudget) || errors.Is(ctx.Err(), context.Canceled) {
		return err
	}
	if w != nil && !w.blocked.Load() {
		return err
	}
	return fmt.Errorf("%w: %w", ErrLocalConnBudget, err)
}

// budgetWaitContext bounds a dial's wait for connection slots by the
// requesting call's own deadline: the dial context's deadline (WebSocket
// dials), else the deadline recorded by withDialWait (net/http requests),
// else peer_dial_timeout.
func (c *TripleTransportClient) budgetWaitContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	if w := dialWaitFrom(ctx); w != nil && !w.deadline.IsZero() {
		return context.WithDeadline(ctx, w.deadline)
	}
	if dt := c.tCfg.PeerDial(); dt > 0 {
		return context.WithTimeout(ctx, dt)
	}
	return ctx, func() {}
}

// takeSlot takes one unit of b, waiting up to ctx. While it waits it calls
// evict at most every budgetEvictInterval to return idle connections that
// hold slots. A failed wait wraps ErrLocalConnBudget and is recorded in the
// request's dialWait.
func takeSlot(ctx context.Context, b *resource.Budget, what string, evict func()) error {
	if b.Try(1) {
		return nil
	}
	w := dialWaitFrom(ctx)
	if w != nil {
		w.blocked.Store(true)
	}
	for {
		evict()
		if ce := ctx.Err(); ce != nil {
			return fmt.Errorf("%w: %s: %w", ErrLocalConnBudget, what, ce)
		}
		slice, cancel := context.WithTimeout(ctx, budgetEvictInterval)
		err := b.Take(slice, 1)
		cancel()
		if err == nil {
			if w != nil {
				w.blocked.Store(false)
			}
			return nil
		}
		if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			if ce := ctx.Err(); ce != nil {
				err = ce
			}
			return fmt.Errorf("%w: %s: %w", ErrLocalConnBudget, what, err)
		}
	}
}

// closeIdleExcept closes idle connections of every pool of this peer except
// self, the pool that is dialing (its own idle connections would have been
// reused instead of dialing). control selects the control pools.
func (c *TripleTransportClient) closeIdleExcept(control bool, self http.RoundTripper) {
	c.shared.mu.Lock()
	endpoints := make([]*TripleTransportClient, 0, len(c.shared.endpoints))
	for _, ep := range c.shared.endpoints {
		endpoints = append(endpoints, ep)
	}
	c.shared.mu.Unlock()
	for _, ep := range endpoints {
		var clients []*http.Client
		if control {
			clients = []*http.Client{ep.controlClient}
		} else {
			ep.mu.Lock()
			clients = append([]*http.Client{ep.dataClient, ep.sseClient}, ep.sseShards...)
			ep.mu.Unlock()
		}
		for _, cl := range clients {
			if cl != nil && (self == nil || cl.Transport != self) {
				cl.CloseIdleConnections()
			}
		}
	}
}
