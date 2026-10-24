package tunnel

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// wsReadLimit bounds one WebSocket message of a tunnel. smux writes one frame
// (8-byte header plus at most MaxFrameSize, itself at most 65535 bytes) per
// Write, and wsConn sends each Write as one binary message.
const wsReadLimit = 128 << 10

// wsConn adapts a WebSocket to the byte stream smux runs on: each Write is
// one binary message and Read concatenates message payloads. Text or other
// non-binary messages are a protocol violation and end the stream.
type wsConn struct {
	c      *websocket.Conn
	rmu    sync.Mutex
	r      io.Reader
	wmu    sync.Mutex
	remote net.Addr
	local  net.Addr
	once   sync.Once
}

// errTextMessage reports a non-binary message on a tunnel.
var errTextMessage = errors.New("tunnel: non-binary WebSocket message")

// newWSConn wraps c. remote replaces the socket's peer address, so the
// streams of a tunnel identify the tunnel (connection accounting keys on it)
// rather than the TCP peer.
func newWSConn(c *websocket.Conn, remote net.Addr) *wsConn {
	c.SetReadLimit(wsReadLimit)
	if remote == nil {
		remote = c.RemoteAddr()
	}
	return &wsConn{c: c, remote: remote, local: c.LocalAddr()}
}

func (w *wsConn) Read(p []byte) (int, error) {
	w.rmu.Lock()
	defer w.rmu.Unlock()
	for {
		if w.r == nil {
			mt, r, err := w.c.NextReader()
			if err != nil {
				var ce *websocket.CloseError
				if errors.As(err, &ce) {
					return 0, io.EOF
				}
				return 0, err
			}
			if mt != websocket.BinaryMessage {
				return 0, errTextMessage
			}
			w.r = r
		}
		n, err := w.r.Read(p)
		if errors.Is(err, io.EOF) {
			w.r = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (w *wsConn) Write(p []byte) (int, error) {
	w.wmu.Lock()
	defer w.wmu.Unlock()
	if err := w.c.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close sends a best-effort close frame and closes the socket.
func (w *wsConn) Close() error {
	var err error
	w.once.Do(func() {
		_ = w.c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		err = w.c.Close()
	})
	return err
}

func (w *wsConn) LocalAddr() net.Addr  { return w.local }
func (w *wsConn) RemoteAddr() net.Addr { return w.remote }
func (w *wsConn) SetDeadline(t time.Time) error {
	if err := w.c.SetReadDeadline(t); err != nil {
		return err
	}
	return w.c.SetWriteDeadline(t)
}
func (w *wsConn) SetReadDeadline(t time.Time) error  { return w.c.SetReadDeadline(t) }
func (w *wsConn) SetWriteDeadline(t time.Time) error { return w.c.SetWriteDeadline(t) }

var _ net.Conn = (*wsConn)(nil)
