package transport

import (
	"context"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"encoding/binary"
	"errors"
	"github.com/gorilla/websocket"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ReadWSMessage reserves the reassembled payload before allocating it. The caller
// keeps the lease until the verified frame has been delivered or discarded.
func ReadWSMessage(ctx context.Context, c *websocket.Conn, budget *resource.Budget, max int) (int, []byte, func(), error) {
	return ReadWSMessageWithProgress(ctx, c, budget, max, 0, nil)
}

func ReadWSMessageWithProgress(parent context.Context, c *websocket.Conn, budget *resource.Budget, max int, stall time.Duration, progress func()) (kind int, payload []byte, release func(), err error) {
	kind, payload, release, _, err = ReadWSMessageWithPayload(parent, c, budget, nil, max, stall, progress, nil)
	return
}

// Cancellation semantics: if parent is cancelled, or the message
// stalls for longer than stall while data is expected, the WebSocket
// connection itself is closed, not just this read. parent must therefore be
// the carrier's lifetime (or a deadline at which the carrier is to be
// declared dead).
//
// Payload and codec leases are separate: the session may adopt the former after
// verification, while reassembly is always released by the transport reader.
func ReadWSMessageWithPayload(parent context.Context, c *websocket.Conn, budget, raw *resource.Budget, max int, stall time.Duration, progress func(), blocked func(bool)) (kind int, payload []byte, release, releasePayload func(), err error) {
	var expired, waiting atomic.Bool
	defer func() {
		if err != nil && expired.Load() && parent.Err() == nil {
			err = ErrBackpressure
		} else {
			err = classifyWSReadError(err)
		}
	}()
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = closeWSHard(c); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
		cancel()
	}()
	typ, r, e := c.NextReader()
	if e != nil {
		return 0, nil, nil, nil, e
	}
	var timer *time.Timer
	if stall > 0 {
		timer = time.AfterFunc(stall, func() { expired.Store(waiting.Load()); cancel() })
		defer timer.Stop()
	}
	r = &progressReader{Reader: r, onProgress: func() {
		if timer != nil {
			timer.Reset(stall)
		}
		if progress != nil {
			progress()
		}
	}}
	// take reports blocked only while it actually has to wait for budget.
	take := func(b *resource.Budget, n int64) error {
		if b.Try(n) {
			return nil
		}
		if blocked != nil {
			blocked(true)
			defer blocked(false)
		}
		waiting.Store(true)
		defer waiting.Store(false)
		return b.Take(ctx, n)
	}
	if typ == websocket.TextMessage {
		cost := int64(16 << 10)
		if e = take(budget, cost); e != nil {
			return 0, nil, nil, nil, e
		}
		b, e := io.ReadAll(io.LimitReader(r, cost+1))
		release := func() { budget.Put(cost) }
		if e != nil {
			release()
			return 0, nil, nil, nil, e
		}
		if len(b) > int(cost) {
			release()
			return 0, nil, nil, nil, ErrProtocol
		}
		return typ, b, release, nil, nil
	}
	if typ != websocket.BinaryMessage {
		return 0, nil, nil, nil, ErrProtocol
	}
	var h [protocol.WSBinaryHeaderLen]byte
	if _, e = io.ReadFull(r, h[:]); e != nil {
		return 0, nil, nil, nil, e
	}
	n := int(binary.BigEndian.Uint32(h[20:24]))
	if n <= 0 || n > max || h[0] != 1 || h[1] != 0 || h[2] != 0 || h[3] != 0 {
		return 0, nil, nil, nil, ErrProtocol
	}
	if raw != nil {
		if e = take(raw, int64(n)); e != nil {
			return 0, nil, nil, nil, e
		}
		var once sync.Once
		lease := func() { once.Do(func() { raw.Put(int64(n)) }) }
		releasePayload = lease
		defer func() {
			if err != nil {
				lease()
			}
		}()
	}
	cost := int64(len(h) + n)
	if e = take(budget, cost); e != nil {
		return 0, nil, nil, nil, e
	}
	b := make([]byte, int(cost))
	copy(b, h[:])
	if _, e = io.ReadFull(r, b[len(h):]); e == nil {
		var extra [1]byte
		var k int
		k, e = r.Read(extra[:])
		if k != 0 {
			e = ErrProtocol
		} else if e == io.EOF {
			e = nil
		}
	}
	if e != nil {
		budget.Put(cost)
		return 0, nil, nil, nil, e
	}
	return typ, b, func() { budget.Put(cost) }, releasePayload, nil
}

// Gorilla uses CloseError for a lost socket, including a truncated wire frame.
// EOF from its message reader instead marks a complete WebSocket message whose
// DBBridge payload ended early. Its local framing errors have no exported type.
func classifyWSReadError(err error) error {
	if err == nil {
		return nil
	}
	var closed *websocket.CloseError
	if errors.As(err, &closed) {
		if closed.Code == websocket.CloseProtocolError || closed.Code == websocket.CloseInvalidFramePayloadData || closed.Code == websocket.CloseMessageTooBig {
			return ErrProtocol
		}
		return err
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, websocket.ErrReadLimit) || strings.HasPrefix(err.Error(), "websocket: ") {
		return ErrProtocol
	}
	return err
}

type progressReader struct {
	io.Reader
	onProgress func()
}

func (r *progressReader) Read(p []byte) (int, error) {
	n, e := r.Reader.Read(p)
	if n > 0 {
		r.onProgress()
	}
	return n, e
}
