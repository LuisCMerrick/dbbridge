package transport

import (
	"bytes"
	"context"
	"dbbridge/internal/protocol"
	"encoding/base64"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// NextEventWithProgress reads the next dispatched SSE event.
//
// Cancellation semantics: parent is the lifetime of the whole
// subscription, not of one read. If parent is cancelled, or no byte arrives
// for timeout, the subscriber is closed for good (its body and DataRequests
// lease are released) and every later call fails. Callers wanting a
// per-read deadline must accept that the stream ends with it.
//
// Codec accounting: a control event (event: other than "data") is read under
// a small reservation and may not exceed sseControlLimit bytes. A data line
// of a data event reserves for the frame size its "<offset>:<length>:"
// prefix declares (and may not be longer than that); only a data line
// without such a prefix reserves the worst-case frame size. The reader never holds part of a reservation while
// it waits for the rest. Once the event is complete the charge is lowered to
// what stays resident (the Data text plus its decoded payload) and that
// remainder is returned by SSEEvent.Release.
func (s *SSESubscriber) NextEventWithProgress(parent context.Context, timeout time.Duration, progress func(), onBlocked ...func(bool)) (event *SSEEvent, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if parent == nil {
		parent = context.Background()
	}
	var waiting, exhausted atomic.Bool
	defer func() {
		if err != nil && exhausted.Load() && parent.Err() == nil {
			err = ErrBackpressure
		}
	}()
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { s.Close(); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
		cancel()
	}()
	var timer *time.Timer
	if timeout > 0 {
		timer = time.AfterFunc(timeout, func() { exhausted.Store(waiting.Load()); cancel() })
		defer timer.Stop()
	}
	limit := s.maxFrameSize*4/3 + 4096
	if limit < 16<<10 {
		limit = 16 << 10
	}
	small := min(sseControlLimit, limit)
	if s.codec == nil && s.client != nil && s.client.resources != nil {
		s.codec = s.client.resources.Codec
	}
	codec := s.codec
	charge := func(n int) int64 {
		w := int64(n)*4 + 64*(sseMaxLines+1)
		if codec != nil {
			w = min(w, codec.Limit())
		}
		return w
	}
	reserved := int64(0)
	refund := func() {
		if codec != nil && reserved > 0 {
			codec.Put(reserved)
		}
		reserved = 0
	}
	reserve := func(want int64) error {
		if codec == nil || reserved >= want {
			return nil
		}
		if codec.Try(want - reserved) {
			reserved = want
			return nil
		}
		// Never wait while holding part of the budget: give the small
		// reservation back and wait for the whole amount in one Take.
		refund()
		if !codec.Try(want) {
			if len(onBlocked) > 0 && onBlocked[0] != nil {
				onBlocked[0](true)
				defer onBlocked[0](false)
			}
			waiting.Store(true)
			e := codec.Take(ctx, want)
			waiting.Store(false)
			if e != nil {
				return e
			}
		}
		reserved = want
		return nil
	}
	defer func() {
		if err != nil {
			refund()
		}
	}()
	// onData marks progress of an event: it resets the read timer and
	// reports progress to the caller. Comments, blank separators and fields
	// the parser ignores are not progress, so a proxy that only writes SSE
	// comments cannot keep a dead stream alive.
	onData := func() {
		if timer != nil {
			timer.Reset(timeout)
		}
		if progress != nil {
			progress()
		}
	}
	idle := func() {}
	var ev SSEEvent
	var data []string
	seen := map[string]bool{}
	total, lines, capBytes := 0, 0, small
	reset := func() {
		ev, data, seen, total, lines, capBytes = SSEEvent{}, nil, map[string]bool{}, 0, 0, small
		refund()
	}
	for {
		first, e := s.peekLineStart()
		if e != nil {
			return nil, e
		}
		if first == '\r' || first == '\n' {
			if _, e := s.readSSELine(nil, 0, idle); e != nil {
				return nil, e
			}
			// WHATWG: an event whose data buffer is empty is not dispatched;
			// comment-only and ignored-field-only blocks reset all accounting.
			if !seen["data"] {
				reset()
				continue
			}
			if !seen["event"] {
				return nil, ErrProtocol
			}
			ev.Data = strings.Join(data, "\n")
			data = nil
			if codec != nil {
				cost := min(resident(len(ev.Data)), reserved)
				codec.Put(reserved - cost)
				reserved = 0
				var mu sync.Mutex
				held := cost
				ev.release = func() {
					mu.Lock()
					n := held
					held = 0
					mu.Unlock()
					if n > 0 {
						codec.Put(n)
					}
				}
				ev.shrink = func(n int64) {
					mu.Lock()
					give := int64(0)
					if n >= 0 && n < held {
						give, held = held-n, n
					}
					mu.Unlock()
					if give > 0 {
						codec.Put(give)
					}
				}
			}
			return &ev, nil
		}
		if first == ':' {
			// Comments are discarded as they stream in: never buffered, never
			// charged and not counted toward the event size, but still bounded.
			if _, e := s.readSSELine(nil, limit, idle); e != nil {
				return nil, e
			}
			continue
		}
		isData, e := s.peekDataField()
		if e != nil {
			return nil, e
		}
		// A data line may carry a frame unless the event already declared
		// itself a control event. A frame line declares its own size in its
		// "<offset>:<length>:" prefix; only a line without a usable prefix
		// reserves for the largest frame allowed.
		if isData && (!seen["event"] || ev.Event == "data") {
			n, ok, e := s.peekDeclaredDataLine()
			if e != nil {
				return nil, e
			}
			switch {
			case !ok:
				capBytes = limit
			case capBytes == small:
				capBytes = min(limit, total+n+sseLineSlack)
			default:
				capBytes = max(capBytes, min(limit, total+n+sseLineSlack))
			}
		}
		if e := reserve(charge(capBytes)); e != nil {
			return nil, e
		}
		// Data lines may be large and arrive slowly, so they report progress
		// as they stream in; other fields are short and count once parsed.
		lineProgress := idle
		if isData {
			lineProgress = onData
		}
		line, e := s.readSSELine(&total, capBytes, lineProgress)
		if e != nil {
			return nil, e
		}
		lines++
		if lines > sseMaxLines || !utf8.Valid(line) {
			return nil, ErrProtocol
		}
		text := string(line)
		key, value, found := strings.Cut(text, ":")
		if found {
			value = strings.TrimPrefix(value, " ")
		}
		switch key {
		case "data":
			seen[key] = true
			data = append(data, value)
		case "event", "id":
			// Repeated event/id lines stay strict: they would make the frame
			// binding ambiguous, and egress never emits them.
			if seen[key] {
				return nil, ErrProtocol
			}
			seen[key] = true
			onData()
			if key == "event" {
				ev.Event = value
			} else {
				ev.ID = value
			}
		default:
			// WHATWG: retry and unknown fields are ignored.
		}
	}
}

// sseControlLimit bounds the field bytes of an event that is not a data frame
// (ready/ping/ack_up/fin/error JSON is decoded with a 16 KiB limit).
const sseControlLimit = 16<<10 + 4096

// resident is the codec charge kept by a completed event: its Data text plus
// the payload the caller decodes from it (ParseSSEData), plus a small fixed
// overhead.
func resident(n int) int64 {
	return int64(n) + int64(base64.StdEncoding.DecodedLen(n)) + 64
}

// peekDataField reports, without consuming anything, whether the next line's
// field name is exactly "data". It only waits for bytes the line must contain
// anyway.
func (s *SSESubscriber) peekDataField() (bool, error) {
	const name = "data"
	for n := 1; n <= len(name)+1; n++ {
		b, e := s.reader.Peek(n)
		if len(b) < n {
			return false, e
		}
		c := b[n-1]
		if n <= len(name) {
			if c != name[n-1] {
				return false, nil
			}
			continue
		}
		return c == ':' || c == '\r' || c == '\n', nil
	}
	return false, nil
}

const sseMaxLines = 64

// sseLineSlack is the room left after a declared frame line for the rest of
// the event (id and event lines that follow the data line).
const sseLineSlack = 4096

// maxDataPrefix bounds the peeked "data: <offset>:<length>:" prefix: two
// 20-digit decimal fields.
const maxDataPrefix = len("data: ") + 20 + 1 + 20 + 1

// peekDeclaredDataLine reads, without consuming anything, the
// "data:[ ]<offset>:<length>:" prefix of the next line and returns the line
// length a frame of that length has (prefix, 64 hex digest, ':' and the
// base64 payload). ok is false when the line does not start with a
// well-formed prefix; the caller then reserves for the largest frame and
// ParseSSEData rejects the line if it is a malformed frame. Like
// peekDataField it only waits for bytes the line must contain anyway.
func (s *SSESubscriber) peekDeclaredDataLine() (n int, ok bool, err error) {
	start := len("data:")
	colons := 0
	for i := start + 1; i <= maxDataPrefix; i++ {
		b, e := s.reader.Peek(i)
		if len(b) < i {
			return 0, false, e
		}
		c := b[i-1]
		switch {
		case c == ' ' && i == len("data:")+1:
			start++
		case c == ':':
			colons++
			if colons < 2 {
				continue
			}
			off, length, found := strings.Cut(string(b[start:i-1]), ":")
			if !found {
				return 0, false, nil
			}
			if _, e := protocol.Offset(off); e != nil {
				return 0, false, nil
			}
			l, e := protocol.Offset(length)
			if e != nil || l == 0 || l > protocol.MaxFramePayload || s.maxFrameSize > 0 && int(l) > s.maxFrameSize {
				return 0, false, nil
			}
			return i + 64 + 1 + base64.StdEncoding.EncodedLen(int(l)), true, nil
		case c < '0' || c > '9':
			return 0, false, nil
		}
	}
	return 0, false, nil
}

// peekLineStart returns the first byte of the next line without consuming it,
// skipping the LF of a CRLF pair whose CR ended the previous line.
func (s *SSESubscriber) peekLineStart() (byte, error) {
	for {
		b, e := s.reader.Peek(1)
		if len(b) == 0 {
			if e == nil {
				e = io.ErrNoProgress
			}
			return 0, e
		}
		if s.pendingLF {
			s.pendingLF = false
			if b[0] == '\n' {
				_, _ = s.reader.Discard(1)
				continue
			}
		}
		return b[0], nil
	}
}

// readSSELine consumes one line terminated by CR, LF or CRLF. When total is
// nil the content is discarded; otherwise it is returned and added to *total.
// The line (plus *total) may not exceed limit bytes; limit 0 accepts only an
// empty line.
func (s *SSESubscriber) readSSELine(total *int, limit int, onData func()) ([]byte, error) {
	var line []byte
	n := 0
	if total != nil {
		n = *total
	}
	for {
		if _, e := s.reader.Peek(1); e != nil {
			return nil, e
		}
		buf, _ := s.reader.Peek(s.reader.Buffered())
		i := bytes.IndexAny(buf, "\r\n")
		chunk := buf
		if i >= 0 {
			chunk = buf[:i]
		}
		if n+len(chunk) > limit {
			return nil, ErrProtocol
		}
		n += len(chunk)
		if total != nil {
			line = append(line, chunk...)
		}
		consumed := len(chunk)
		if i >= 0 {
			consumed++
			if buf[i] == '\r' {
				if i+1 < len(buf) {
					if buf[i+1] == '\n' {
						consumed++
					}
				} else {
					s.pendingLF = true
				}
			}
		}
		_, _ = s.reader.Discard(consumed)
		onData()
		if i >= 0 {
			if total != nil {
				*total = n
			}
			return line, nil
		}
	}
}
