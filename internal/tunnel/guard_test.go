package tunnel

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"dbbridge/internal/config"

	"github.com/xtaci/smux"
)

func guardTestConfig() *config.TransportConfig {
	return &config.TransportConfig{CodecBufferLimit: 4 << 20, BufferPerDirection: 1 << 20}
}

func rawFrame(cmd byte, sid uint32, payload []byte) []byte {
	b := make([]byte, smuxHeaderSize+len(payload))
	b[0] = 2
	b[1] = cmd
	binary.LittleEndian.PutUint16(b[2:], uint16(len(payload)))
	binary.LittleEndian.PutUint32(b[4:], sid)
	copy(b[smuxHeaderSize:], payload)
	return b
}

// floodUnread opens one stream from a raw peer that ignores the window and
// pushes data nobody reads. It returns how many payload bytes the smux side
// took before it stopped reading or failed.
func floodUnread(t *testing.T, guarded bool, max int) (int, *smux.Session) {
	t.Helper()
	tc := guardTestConfig()
	a, b := net.Pipe()
	var conn net.Conn = a
	if guarded {
		conn = guardConn(a, ReceiveBuffer(tc), "test")
	}
	sess, err := smux.Server(conn, smuxConfig(tc))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sess.Close(); _ = b.Close() })
	go func() { _, _ = io.Copy(io.Discard, b) }() // drain the session's own frames
	if _, err := b.Write(rawFrame(smuxCmdSYN, 1, nil)); err != nil {
		t.Fatal(err)
	}
	chunk := make([]byte, 65535)
	sent := 0
	for sent < max {
		_ = b.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err := b.Write(rawFrame(smuxCmdPSH, 1, chunk)); err != nil {
			break
		}
		sent += len(chunk)
	}
	return sent, sess
}

// C-01: smux alone buffers far more than a stream window for a peer that
// ignores flow control; the guard ends the tunnel at the window.
func TestGuardEnforcesStreamWindow(t *testing.T) {
	w := int(ReceiveBuffer(guardTestConfig()))
	const max = 16 << 20

	t.Run("unguarded smux (baseline)", func(t *testing.T) {
		sent, _ := floodUnread(t, false, max)
		if sent < 8*w {
			t.Skipf("smux stopped at %d bytes; baseline no longer shows the gap", sent)
		}
		t.Logf("smux buffered %d bytes on one unread stream (window %d)", sent, w)
	})
	t.Run("guarded", func(t *testing.T) {
		sent, sess := floodUnread(t, true, max)
		if sent > w+2*65535 {
			t.Fatalf("guard let %d bytes through, window %d", sent, w)
		}
		// The guard closed the carrier: smux sees a read error and the
		// stream it accepted fails instead of holding the flood.
		st, err := sess.AcceptStream()
		if err == nil {
			_ = st.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, err = io.ReadAll(st)
			if err == nil {
				t.Fatal("stream ended cleanly after a window violation")
			}
		}
	})
}

// A compliant peer is never cut off: bulk transfers with a slow reader, many
// short streams and half-closes stay inside the windows smux advertises.
func TestGuardAllowsCompliantPeers(t *testing.T) {
	tc := guardTestConfig()
	a, b := net.Pipe()
	ga := guardConn(a, ReceiveBuffer(tc), "hub")
	gb := guardConn(b, ReceiveBuffer(tc), "db")
	hub, err := smux.Client(ga, smuxConfig(tc))
	if err != nil {
		t.Fatal(err)
	}
	db, err := smux.Server(gb, smuxConfig(tc))
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	defer db.Close()

	// The database side echoes every stream back, reading slowly.
	go func() {
		for {
			st, err := db.AcceptStream()
			if err != nil {
				return
			}
			go func() {
				defer st.Close()
				buf := make([]byte, 7000)
				for {
					n, err := st.Read(buf)
					if n > 0 {
						if _, werr := st.Write(buf[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()

	roundTrip := func(size int) {
		st, err := hub.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		data := make([]byte, size)
		_, _ = rand.Read(data)
		var got bytes.Buffer
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			slow := make([]byte, 3000)
			for got.Len() < size {
				n, err := st.Read(slow)
				got.Write(slow[:n])
				if err != nil {
					return
				}
				if got.Len()%(1<<20) < 3000 {
					time.Sleep(time.Millisecond)
				}
			}
		}()
		if _, err := st.Write(data); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if !bytes.Equal(got.Bytes(), data) {
			t.Fatalf("echo of %d bytes returned %d bytes", size, got.Len())
		}
	}
	roundTrip(24 << 20)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); roundTrip(300 << 10) }()
	}
	wg.Wait()
	for i := 0; i < 300; i++ {
		roundTrip(1000)
	}
	if hub.IsClosed() || db.IsClosed() {
		t.Fatal("guard closed a compliant session")
	}
	waitUntil(t, func() bool {
		ga.mu.Lock()
		gb.mu.Lock()
		defer ga.mu.Unlock()
		defer gb.mu.Unlock()
		return len(ga.streams) == 0 && len(gb.streams) == 0
	})
}
