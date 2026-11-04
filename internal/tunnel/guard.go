package tunnel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
)

// smux v2 wire format, mirrored from github.com/xtaci/smux (frame.go).
const (
	smuxHeaderSize = 8 // ver(1) cmd(1) length(2, LE) sid(4, LE)
	smuxCmdSYN     = 0
	smuxCmdFIN     = 1
	smuxCmdPSH     = 2
	smuxCmdNOP     = 3
	smuxCmdUPD     = 4
	smuxUPDSize    = 8 // consumed(4, LE) window(4, LE)

	// smuxInitialPeerWindow is the window a smux v2 sender assumes for a
	// stream until the receiver's first UPD arrives.
	smuxInitialPeerWindow = 262144
)

// maxGuardedStreams bounds the streams the guard tracks per tunnel: the
// streams in use (MaxStreamsPerTunnel), the smux accept backlog (1024) and
// streams closed on one side only. A peer that exceeds it is not a smux
// implementation that finishes its streams.
const maxGuardedStreams = 4 * 1024

// errWindowViolation ends a tunnel whose peer sent a stream more data than
// the stream's receive window allows.
var errWindowViolation = errors.New("tunnel peer violated the stream receive window")

// guardedConn enforces the smux v2 per-stream receive window on the
// connection under a smux session.
//
// smux advertises a receive window per stream (MaxStreamBuffer) and a
// well-behaved sender never has more than that in flight, but smux itself
// does not enforce it on receive: incoming data is buffered as long as the
// session-wide bucket (MaxReceiveBuffer, sized for MaxStreamsPerTunnel full
// windows) has tokens. A peer that ignores the window could therefore park
// up to MaxReceiveBuffer bytes on one stream while the codec budget is
// charged a single window for it.
//
// guardedConn parses the frame stream in both directions. For each stream it
// counts the payload bytes received and records the consumed offset of the
// last UPD this side sent; the peer learns that offset only from the UPD, so
// a compliant sender always satisfies
//
//	received - consumed <= max(window, initial peer window)
//
// (modulo 2^32, as smux computes it). A frame that breaks the bound fails the
// read, which ends the session, so the memory a peer can hold on this side is
// bounded by the windows that are charged to the codec budget.
type guardedConn struct {
	net.Conn
	limit uint32
	label string

	mu      sync.Mutex
	streams map[uint32]*guardStream
	in      frameScanner
	out     frameScanner
	failed  error
}

type guardStream struct {
	recv, consumed uint32
	finIn, finOut  bool
}

// frameScanner follows frame boundaries in one direction of a byte stream.
type frameScanner struct {
	hdr     [smuxHeaderSize]byte
	nhdr    int
	cmd     byte
	sid     uint32
	payload int // payload bytes of the current frame still to come
	upd     [smuxUPDSize]byte
	nupd    int
}

func guardConn(c net.Conn, window int64, label string) *guardedConn {
	limit := uint32(max(window, smuxInitialPeerWindow))
	return &guardedConn{Conn: c, limit: limit, label: label, streams: map[uint32]*guardStream{}}
}

func (g *guardedConn) Read(p []byte) (int, error) {
	n, err := g.Conn.Read(p)
	if n > 0 {
		g.mu.Lock()
		ferr := g.failed
		if ferr == nil {
			ferr = g.scan(&g.in, p[:n], true)
			if ferr != nil {
				g.failed = ferr
			}
		}
		g.mu.Unlock()
		if ferr != nil {
			slog.Warn("reverse tunnel closed: peer violated the multiplexer flow control", "tunnel", g.label, "err", ferr)
			_ = g.Conn.Close()
			return 0, ferr
		}
	}
	return n, err
}

func (g *guardedConn) Write(p []byte) (int, error) {
	// Apply outgoing frames before they are sent: once the peer reads a
	// UPD it may send more data at once, which can reach Read before this
	// Write returns. smux writes whole frames; a short write ends the
	// session, so applying the full buffer up front is exact.
	g.mu.Lock()
	// Outgoing frames come from the local smux session; they only move
	// consumed offsets and stream state, so they never fail.
	_ = g.scan(&g.out, p, false)
	g.mu.Unlock()
	return g.Conn.Write(p)
}

// scan advances s over b and applies each frame to the stream table.
func (g *guardedConn) scan(s *frameScanner, b []byte, inbound bool) error {
	for len(b) > 0 {
		if s.nhdr < smuxHeaderSize {
			k := copy(s.hdr[s.nhdr:], b)
			s.nhdr += k
			b = b[k:]
			if s.nhdr < smuxHeaderSize {
				return nil
			}
			s.cmd = s.hdr[1]
			s.sid = binary.LittleEndian.Uint32(s.hdr[4:])
			s.payload = int(binary.LittleEndian.Uint16(s.hdr[2:]))
			s.nupd = 0
			if err := g.frame(s, inbound); err != nil {
				return err
			}
			if s.payload == 0 {
				s.nhdr = 0
			}
			continue
		}
		k := min(len(b), s.payload)
		if s.cmd == smuxCmdUPD && s.nupd < smuxUPDSize {
			s.nupd += copy(s.upd[s.nupd:], b[:k])
			if s.nupd == smuxUPDSize && !inbound {
				if st := g.streams[s.sid]; st != nil {
					st.consumed = binary.LittleEndian.Uint32(s.upd[:4])
				}
			}
		}
		s.payload -= k
		b = b[k:]
		if s.payload == 0 {
			s.nhdr = 0
		}
	}
	return nil
}

// frame applies a frame header. smux validates versions, lengths and
// commands itself; the guard only tracks what bounds memory.
func (g *guardedConn) frame(s *frameScanner, inbound bool) error {
	switch s.cmd {
	case smuxCmdSYN:
		if _, ok := g.streams[s.sid]; !ok {
			if len(g.streams) >= maxGuardedStreams {
				return fmt.Errorf("%w: more than %d open streams", errWindowViolation, maxGuardedStreams)
			}
			g.streams[s.sid] = &guardStream{}
		}
	case smuxCmdFIN:
		st := g.streams[s.sid]
		if st == nil {
			return nil
		}
		if inbound {
			st.finIn = true
		} else {
			st.finOut = true
		}
		// Both sides finished: no data can follow on this stream.
		if st.finIn && st.finOut {
			delete(g.streams, s.sid)
		}
	case smuxCmdPSH:
		if !inbound {
			return nil
		}
		st := g.streams[s.sid]
		if st == nil {
			// smux drops data for streams it does not know.
			return nil
		}
		st.recv += uint32(s.payload)
		if st.recv-st.consumed > g.limit {
			return fmt.Errorf("%w: stream %d holds %d unread bytes, window %d", errWindowViolation, s.sid, st.recv-st.consumed, g.limit)
		}
	}
	return nil
}
