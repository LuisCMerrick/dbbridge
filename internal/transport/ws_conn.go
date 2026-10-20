package transport

import (
	"crypto/tls"
	"dbbridge/internal/protocol"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WSSessionConn wraps an active WebSocket session connection.
type WSSessionConn struct {
	conn      *websocket.Conn
	streamID  string
	writeMu   sync.Mutex
	writeIdle time.Duration
	closed    bool
	closedMu  sync.Mutex
}

func (w *WSSessionConn) StreamID() string {
	return w.streamID
}

func (w *WSSessionConn) Conn() *websocket.Conn {
	return w.conn
}

// WriteBinaryFrame encodes and safely writes a 56-byte header DBBridge WS binary frame.
func (w *WSSessionConn) WriteBinaryFrame(epoch uint64, offset uint64, payload []byte) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	frame, err := protocol.EncodeWSFrameChecked(epoch, offset, payload)
	if err != nil {
		return err
	}
	return WriteWSMessage(w.conn, websocket.BinaryMessage, frame, w.writeIdle)
}

// WriteTextMessage safely writes a JSON or text control message.
func (w *WSSessionConn) WriteTextMessage(data []byte) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	return WriteWSMessage(w.conn, websocket.TextMessage, data, w.writeIdle)
}

// Close gracefully closes the connection without waiting for writeMu.
// Network I/O is interrupted at once so an active disconnect completes
// within one second.
func (w *WSSessionConn) Close() error {
	w.closedMu.Lock()
	defer w.closedMu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true

	// Best-effort Close frame, written synchronously so it reaches the socket
	// before it is torn down. WriteControl may run alongside a data writer and
	// gives up after the deadline, keeping Close well inside the 1s budget.
	_ = w.conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(100*time.Millisecond),
	)

	return closeWSHard(w.conn)
}

// closeWSHard closes a WebSocket connection without blocking on the peer.
// For wss, tls.Conn.Close would first send close_notify under its own 5s
// write deadline, which blocks when the peer stopped reading and the send
// buffer is full. Closing the raw TCP connection first makes that
// write fail at once; the TLS layer is then closed for its bookkeeping.
func closeWSHard(c *websocket.Conn) error {
	if c == nil {
		return nil
	}
	if tc, ok := c.NetConn().(*tls.Conn); ok {
		_ = tc.NetConn().Close()
	}
	return c.Close()
}
