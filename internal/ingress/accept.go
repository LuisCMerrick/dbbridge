package ingress

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"syscall"
	"time"

	"dbbridge/internal/config"
)

// Accept retry backoff, as in net/http.Server.Serve.
const acceptBackoffMin = 5 * time.Millisecond

// acceptBackoffMax caps the retry backoff (a variable for tests).
var acceptBackoffMax = time.Second

const (
	// acceptFatalAfter is the number of consecutive non-transient Accept
	// errors after which the listener is reported as dead to the fatal
	// handler (if one is registered). Without a handler it keeps retrying.
	acceptFatalAfter = 20
)

// acceptRejected counts local connections refused for lack of local budget.
var acceptRejected atomic.Int64

// acceptTransient reports Accept errors that are expected to clear on their
// own (descriptor exhaustion, aborted handshakes, memory pressure, timeouts).
func acceptTransient(e error) bool {
	for _, errno := range []syscall.Errno{syscall.EMFILE, syscall.ENFILE, syscall.ECONNABORTED, syscall.ECONNRESET, syscall.ENOBUFS, syscall.ENOMEM, syscall.EINTR, syscall.EAGAIN} {
		if errors.Is(e, errno) {
			return true
		}
	}
	var ne net.Error
	return errors.As(e, &ne) && ne.Timeout()
}

func (m *IngressManager) acceptLoop(ln net.Listener, l config.ListenerConfig) {
	defer m.wg.Done()
	var backoff time.Duration
	var lastReject time.Time
	permanent := 0
	for {
		c, e := ln.Accept()
		if e != nil {
			if errors.Is(e, net.ErrClosed) || m.stopping() {
				return
			}
			if acceptTransient(e) {
				permanent = 0
			} else {
				permanent++
			}
			backoff = min(max(backoff*2, acceptBackoffMin), acceptBackoffMax)
			slog.Warn("ingress accept error; retrying", "listener", l.ID, "err", e, "backoff", backoff, "transient", permanent == 0)
			if permanent >= acceptFatalAfter && m.reportFatal(fmt.Errorf("ingress listener %s: accept keeps failing: %w", l.ID, e)) {
				return
			}
			t := time.NewTimer(backoff)
			select {
			case <-t.C:
			case <-m.stopCh:
				t.Stop()
				return
			}
			continue
		}
		backoff, permanent = 0, 0
		acceptedAt := time.Now()
		m.mu.Lock()
		if m.closed || m.draining || !m.cfg.Resources.Sessions.Try(1) {
			busy := !m.closed && !m.draining
			m.mu.Unlock()
			c.Close()
			if busy {
				lastReject = m.noteRejected(l, "sessions", lastReject)
			}
			continue
		}
		metadataCost := int64(16<<10) + int64(m.cfg.Transport.UpMaxInflightFrames)*192
		if !m.cfg.Resources.Metadata.Try(metadataCost) {
			m.cfg.Resources.Sessions.Put(1)
			m.mu.Unlock()
			c.Close()
			lastReject = m.noteRejected(l, "metadata", lastReject)
			continue
		}
		m.wg.Add(1)
		ctx, cancel := context.WithCancel(m.ctx)
		id := generateID(16)
		m.activeConns.Store(id, &activeConnEntry{cancel: cancel, closeFn: func() { _ = c.Close() }})
		m.mu.Unlock()
		go func() {
			defer m.wg.Done()
			defer m.cfg.Resources.Metadata.Put(metadataCost)
			defer m.cfg.Resources.Sessions.Put(1)
			m.handleConn(ctx, cancel, c, l, id, acceptedAt)
		}()
	}
}

// noteRejected counts a connection refused for local budget and logs at most
// once per second per listener loop.
func (m *IngressManager) noteRejected(l config.ListenerConfig, budget string, last time.Time) time.Time {
	total := acceptRejected.Add(1)
	if time.Since(last) < time.Second {
		return last
	}
	slog.Warn("ingress connection rejected: local budget exhausted", "listener", l.ID, "budget", budget, "rejected_total", total)
	return time.Now()
}
