package egress

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"dbbridge/internal/protocol"
	"dbbridge/internal/session"
)

// eofTailConn returns its tail together with a normal io.EOF.
type eofTailConn struct {
	net.Conn
	read bool
}

func (c *eofTailConn) Read(p []byte) (int, error) {
	if c.read {
		return 0, io.EOF
	}
	c.read = true
	return copy(p, "tail"), io.EOF
}

// DBB-AUD-03 boundary: only non-EOF read errors fence recovery. A normal
// backend EOF settles the tail bytes, records the down FIN and keeps the
// session open and recoverable (FR-04(6) "正常方向 EOF 按 FIN 排空", 5.9).
func TestBackendNormalEOFStillDrainsAndRecovers(t *testing.T) {
	cfg := fixCfg(t, nil)
	cfg.Transport.FrameSize = 512
	srv := &Server{cfg: cfg, ctx: context.Background()}
	c, b := net.Pipe()
	defer b.Close()
	s := session.NewSession("A", "db", "bi", &eofTailConn{Conn: c}, 4096, 2048, 8, 64, true)
	s.Configure(2*time.Second, 2*time.Second, "ai", nil)
	s.SetTransportMode("ws")
	id, e := s.ReserveStream(1, "ws")
	if e != nil {
		t.Fatal(e)
	}
	if e = s.ActivateDataPlane(id); e != nil {
		t.Fatal(e)
	}
	done := make(chan struct{})
	go func() { defer close(done); srv.runBackendReader(s, make(chan struct{}, 1)) }()
	defer func() { s.Close(); <-done }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reader did not stop at EOF")
	}
	if s.DownProducedOffset() != 4 || s.GetState() == session.StateClosed {
		t.Fatalf("EOF: produced=%d state=%s, want 4 and open", s.DownProducedOffset(), s.GetState())
	}
	time.Sleep(20 * time.Millisecond)
	if s.GetState() == session.StateClosed {
		t.Fatal("normal EOF closed the session instead of draining")
	}
	q := &protocol.ResumeRequest{ResumeRequestID: "eof-ok", ExpectedEpoch: "1", IngressInstanceID: "ai", Transport: protocol.TransportMode{Mode: "ws"}, KnownUpAckOffset: "0", UpNextOffset: "0", DownDeliveredOffset: "0"}
	r, err := s.Resume(q, time.Second)
	if err != nil || r.Epoch != "2" {
		t.Fatalf("resume after normal EOF: %+v %v", r, err)
	}
}
