package transport

import (
	"bufio"
	"context"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type SSESubscriber struct {
	ctx                     context.Context
	codec                   *resource.Budget
	client                  *TripleTransportClient
	sessionID, sessionToken string
	epoch                   uint64
	maxFrameSize            int
	body                    io.ReadCloser
	reader                  *bufio.Reader
	mu                      sync.Mutex
	closeOnce               sync.Once
	releaseRequest          func()
	failTimeout             time.Duration
	pendingLF               bool // previous line ended in CR; skip one LF
	lineHint                int  // capacity for the next buffered line (a declared frame line)
	implicitReady           bool // the egress advertised protocol.HeaderImplicitReady
}

// ImplicitReady reports whether the egress activates the initial attach on
// the first upstream frame (protocol.HeaderImplicitReady).
func (s *SSESubscriber) ImplicitReady() bool { return s.implicitReady }

type SSEEvent struct {
	Event, ID, Data string
	release         func()
	shrink          func(int64)
}

// Release returns the event's codec charge. It is idempotent and may be
// called from a goroutine other than the reader's; it never touches the
// subscriber.
func (e *SSEEvent) Release() {
	if e.release != nil {
		e.release()
	}
}

// Shrink lowers the event's codec charge to n bytes (never raises it), for a
// caller that has decoded the event and keeps only n bytes of it resident
// (and has dropped its references to Data). It must not race with Release.
func (e *SSEEvent) Shrink(n int64) {
	if e.shrink != nil {
		e.shrink(n)
	}
}

func (c *TripleTransportClient) SubscribeEvents(ctx context.Context, sid, token, inst string, ep uint64, maxFrame int) (*SSESubscriber, error) {
	release := func() {}
	waitCtx, wait := withDialWait(ctx, c.tCfg.ControlRequestTimeout)
	if c.resources != nil {
		if err := c.resources.DataRequests.Take(ctx, 1); err != nil {
			return nil, localBudgetError(ctx, fmt.Errorf("request budget: %w", err), nil)
		}
		release = func() { c.resources.DataRequests.Put(1) }
	}
	owned := true
	defer func() {
		if owned {
			release()
		}
	}()
	r, e := http.NewRequestWithContext(waitCtx, "GET", c.peerURL+sessionPath(sid, "/events", ep, true), nil)
	if e != nil {
		return nil, e
	}
	SetUserAgent(r.Header, c.tCfg)
	r.Header.Set("Accept", "text/event-stream")
	if t := c.GetToken(); t != "" {
		r.Header.Set("Authorization", "Bearer "+t)
	}
	if token != "" {
		r.Header.Set(protocol.HeaderSessionToken, token)
	}
	if inst != "" {
		r.Header.Set(protocol.HeaderInstanceID, inst)
	}
	resp, e := c.nextSSEShard().Do(r)
	if e != nil {
		return nil, localBudgetError(ctx, e, wait)
	}
	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		return nil, ReadError(resp, inst)
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		resp.Body.Close()
		return nil, ErrProtocol
	}
	owned = false
	var codec *resource.Budget
	if c.resources != nil {
		codec = c.resources.Codec
	}
	return &SSESubscriber{client: c, codec: codec, sessionID: sid, sessionToken: token, epoch: ep, maxFrameSize: maxFrame, body: resp.Body, reader: bufio.NewReaderSize(resp.Body, 4096), ctx: ctx, failTimeout: c.tCfg.FailureDetectTimeout, releaseRequest: release, implicitReady: resp.Header.Get(protocol.HeaderImplicitReady) == "1"}, nil
}
func (s *SSESubscriber) NextEvent() (*SSEEvent, error) {
	timeout := s.failTimeout
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	return s.NextEventWithProgress(s.ctx, timeout, nil)
}

func (s *SSESubscriber) Close() error {
	var e error
	s.closeOnce.Do(func() {
		if s.body != nil {
			e = s.body.Close()
		}
		if s.releaseRequest != nil {
			s.releaseRequest()
		}
	})
	return e
}
