package tunnel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"
)

// pipe is one side of a tunnel carried over plain HTTP requests (SSE or long
// polling plus POST). It is a reliable, ordered byte stream addressed by
// offset in each direction, wrapped as the net.Conn smux runs on:
//
//   - Write appends to the outbound buffer, which keeps every byte until the
//     other side acknowledges it, so a lost request is simply sent again
//     from the acknowledged offset. Write blocks while outCap bytes wait
//     unacknowledged.
//   - receive accepts the bytes the other side sent at an offset; bytes
//     already received are skipped (retransmissions), a gap is a protocol
//     error. It waits while inCap bytes wait unread, which pushes back on
//     the carrier.
//
// smux keepalive runs on top: when the carrier stops moving bytes in either
// direction, the session times out and closes the pipe.
type pipe struct {
	mu sync.Mutex
	// changed is closed and replaced on every state change.
	changed chan struct{}

	out     []byte
	outBase int64 // offset of out[0]; everything before is acknowledged
	outCap  int

	in     []byte
	inNext int64 // offset of the next byte expected from the other side
	inCap  int

	closed   bool
	closeErr error
	onClose  func()

	local, remote net.Addr
}

var errPipeGap = errors.New("tunnel: offset gap on the HTTP carrier")
var errPipeAck = errors.New("tunnel: acknowledgement beyond the data sent")

func newPipe(capacity int, local, remote net.Addr) *pipe {
	return &pipe{changed: make(chan struct{}), outCap: capacity, inCap: capacity, local: local, remote: remote}
}

// signal wakes every waiter. Callers hold p.mu.
func (p *pipe) signal() {
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *pipe) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.in) == 0 {
		if p.closed {
			return 0, io.EOF
		}
		ch := p.changed
		p.mu.Unlock()
		<-ch
		p.mu.Lock()
	}
	n := copy(b, p.in)
	p.in = p.in[n:]
	if len(p.in) == 0 {
		p.in = nil
	}
	p.signal()
	return n, nil
}

func (p *pipe) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	written := 0
	for written < len(b) {
		if p.closed {
			return written, net.ErrClosed
		}
		room := p.outCap - len(p.out)
		if room <= 0 {
			ch := p.changed
			p.mu.Unlock()
			<-ch
			p.mu.Lock()
			continue
		}
		n := min(room, len(b)-written)
		p.out = append(p.out, b[written:written+n]...)
		written += n
		p.signal()
	}
	return written, nil
}

// ack drops the outbound bytes before off. An ack at or before the current
// base is a stale duplicate; one beyond the data written is a violation.
func (p *pipe) ack(off int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if off <= p.outBase {
		return nil
	}
	end := p.outBase + int64(len(p.out))
	if off > end {
		return errPipeAck
	}
	drop := int(off - p.outBase)
	p.out = append([]byte(nil), p.out[drop:]...)
	p.outBase = off
	p.signal()
	return nil
}

// pending returns a copy of at most max outbound bytes from offset from
// (raised to the acknowledged base) and the offset of the first one. It
// waits until there is such a byte, the pipe closes or ctx ends; then it
// returns no bytes.
func (p *pipe) pending(ctx context.Context, from int64, max int) ([]byte, int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if from < p.outBase {
			from = p.outBase
		}
		end := p.outBase + int64(len(p.out))
		if from < end {
			start := int(from - p.outBase)
			n := min(max, len(p.out)-start)
			return append([]byte(nil), p.out[start:start+n]...), from
		}
		if p.closed || ctx.Err() != nil {
			return nil, from
		}
		ch := p.changed
		p.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
		}
		p.mu.Lock()
	}
}

// unacked returns the outbound bytes waiting for an acknowledgement.
func (p *pipe) unacked() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.out)
}

// receive accepts data that starts at offset off and returns the offset of
// the next byte expected. It waits while the unread buffer is full; when
// ctx ends first, it returns what it accepted so far with ctx's error.
// Concurrent receives of overlapping data (a retransmission racing the
// original) are safe: each pass re-derives what is still missing from the
// current receive offset.
func (p *pipe) receive(ctx context.Context, off int64, data []byte) (int64, error) {
	if off < 0 || off > math.MaxInt64-int64(len(data)) {
		// Not a position on the byte stream: refuse it like a gap, never as
		// an empty retransmission.
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.inNext, fmt.Errorf("%w: offset %d out of range", errPipeGap, off)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	end := off + int64(len(data))
	for p.inNext < end {
		if off > p.inNext {
			return p.inNext, errPipeGap
		}
		if p.closed {
			return p.inNext, net.ErrClosed
		}
		room := p.inCap - len(p.in)
		if room <= 0 {
			ch := p.changed
			p.mu.Unlock()
			select {
			case <-ch:
			case <-ctx.Done():
			}
			p.mu.Lock()
			if ctx.Err() != nil {
				return p.inNext, ctx.Err()
			}
			continue
		}
		missing := data[p.inNext-off:]
		n := min(room, len(missing))
		p.in = append(p.in, missing[:n]...)
		p.inNext += int64(n)
		p.signal()
	}
	return p.inNext, nil
}

// received returns the offset of the next byte expected (what this side
// acknowledges).
func (p *pipe) received() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.inNext
}

// wait returns a channel closed on the next state change.
func (p *pipe) wait() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.changed
}

func (p *pipe) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// closeWith closes the pipe, recording why. Unread inbound bytes stay
// readable; Read reports EOF after them.
func (p *pipe) closeWith(err error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.closeErr = err
	fn := p.onClose
	p.signal()
	p.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func (p *pipe) err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closeErr
}

func (p *pipe) Close() error {
	p.closeWith(nil)
	return nil
}

func (p *pipe) LocalAddr() net.Addr  { return p.local }
func (p *pipe) RemoteAddr() net.Addr { return p.remote }

// Deadlines are not supported: smux never sets them on its connection, and
// its keepalive bounds a stalled carrier.
func (p *pipe) SetDeadline(time.Time) error      { return nil }
func (p *pipe) SetReadDeadline(time.Time) error  { return nil }
func (p *pipe) SetWriteDeadline(time.Time) error { return nil }

var _ net.Conn = (*pipe)(nil)
