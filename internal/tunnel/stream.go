package tunnel

import (
	"net"
	"os"
	"sync"
	"time"
)

// streamWriteChunk bounds the copy one stream Write hands to smux at a time.
const streamWriteChunk = 64 << 10

// streamCloseFlush bounds how long Close waits for a handed-off chunk to
// reach the session before closing the stream anyway.
const streamCloseFlush = 10 * time.Second

// ownedStream gives a smux stream exact net.Conn write semantics.
//
// smux's Write is not exact under a write deadline: once a frame is queued
// for the send loop, a deadline that fires returns (0, timeout) while the
// frame is still sent later, holding the caller's slice. A caller that then
// retries the "unwritten" suffix (transport.IdleWriteConn does, by design)
// puts duplicate bytes on the stream, and one that reuses its buffer (the
// WebSocket writer, net/http) races with the send loop. Both corrupt the
// byte stream the peer reads.
//
// ownedStream never passes a write deadline to smux. Each Write copies the
// caller's bytes into chunks and hands them, one at a time, to a background
// smux write without a deadline. A chunk handed off is counted as written:
// smux either sends it or the stream dies. The write deadline bounds only
// the wait for the previous chunk to finish, so a timeout reports exactly
// the bytes that will reach the peer.
type ownedStream struct {
	net.Conn

	wmu sync.Mutex // serializes Write and CloseWrite

	mu       sync.Mutex
	deadline time.Time
	dlChange chan struct{} // closed and replaced when the deadline changes
	busy     chan struct{} // closed when the in-flight chunk finishes; nil when idle
	werr     error         // first background write error
	closed   bool
}

func ownStream(c net.Conn) net.Conn {
	return &ownedStream{Conn: c, dlChange: make(chan struct{})}
}

func (s *ownedStream) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	s.deadline = t
	close(s.dlChange)
	s.dlChange = make(chan struct{})
	s.mu.Unlock()
	return nil
}

func (s *ownedStream) SetDeadline(t time.Time) error {
	if err := s.Conn.SetReadDeadline(t); err != nil {
		return err
	}
	return s.SetWriteDeadline(t)
}

// idle waits until no chunk is in flight. It reports a timeout when the write
// deadline passes first, and the background write's error if it failed.
func (s *ownedStream) idle() error {
	for {
		s.mu.Lock()
		busy, dl, changed, werr, closed := s.busy, s.deadline, s.dlChange, s.werr, s.closed
		s.mu.Unlock()
		if werr != nil {
			return werr
		}
		if closed {
			return net.ErrClosed
		}
		if busy == nil {
			return nil
		}
		var expire <-chan time.Time
		if !dl.IsZero() {
			d := time.Until(dl)
			if d <= 0 {
				return os.ErrDeadlineExceeded
			}
			t := time.NewTimer(d)
			expire = t.C
			defer t.Stop()
		}
		select {
		case <-busy:
		case <-changed:
		case <-expire:
			return os.ErrDeadlineExceeded
		}
	}
}

func (s *ownedStream) Write(p []byte) (int, error) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	written := 0
	for written < len(p) {
		if err := s.idle(); err != nil {
			return written, err
		}
		n := min(len(p)-written, streamWriteChunk)
		b := append([]byte(nil), p[written:written+n]...)
		done := make(chan struct{})
		s.mu.Lock()
		s.busy = done
		s.mu.Unlock()
		go func() {
			_, err := s.Conn.Write(b)
			s.mu.Lock()
			if err != nil && s.werr == nil {
				s.werr = err
			}
			if s.busy == done {
				s.busy = nil
			}
			s.mu.Unlock()
			close(done)
		}()
		written += n
	}
	return written, nil
}

// flush waits, bounded, for the chunk in flight so CloseWrite does not cut
// bytes Write already reported as written.
func (s *ownedStream) flush() {
	s.mu.Lock()
	busy := s.busy
	s.mu.Unlock()
	if busy == nil {
		return
	}
	t := time.NewTimer(streamCloseFlush)
	defer t.Stop()
	select {
	case <-busy:
	case <-t.C:
	}
}

// Close returns at once, like a socket close: pending writers and readers
// are released immediately, while a chunk Write already reported as written
// still gets a bounded chance to reach the session before the stream closes.
func (s *ownedStream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.dlChange)
	s.dlChange = make(chan struct{})
	busy := s.busy
	s.mu.Unlock()
	if busy == nil {
		return s.Conn.Close()
	}
	_ = s.Conn.SetReadDeadline(time.Now())
	go func() {
		s.flush()
		_ = s.Conn.Close()
	}()
	return nil
}

// CloseWrite half-closes the stream when the underlying stream can (net/http
// uses it to finish a connection gracefully).
func (s *ownedStream) CloseWrite() error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	s.flush()
	if c, ok := s.Conn.(interface{ CloseWrite() error }); ok {
		return c.CloseWrite()
	}
	return nil
}
