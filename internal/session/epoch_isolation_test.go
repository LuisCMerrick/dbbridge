package session

import (
	"dbbridge/internal/protocol"
	"testing"
	"time"
)

func TestOldHeartbeatClearsNewRecoveryDeadline(t *testing.T) {
	s, _ := testSession(t, true)
	s.SetTransportMode("ws")
	id, e := s.ReserveStream(1, "ws")
	if e != nil {
		t.Fatal(e)
	}
	if e = s.ActivateDataPlane(id); e != nil {
		t.Fatal(e)
	}
	nonce := s.Heartbeat()
	s.Suspend(200*time.Millisecond, nil)
	r, e := s.Resume(testReq("r2"), 20*time.Millisecond)
	if e != nil {
		t.Fatal(e)
	}
	id2, e := s.ReserveStream(2, "ws")
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.Ack(2, id2, protocol.AckRequest{ReadyReceived: true, DownDeliveredOffset: "0", UpConfirmedOffset: "0", HeartbeatNonce: nonce, ProbeID: "new-probe"})
	if e != nil {
		t.Fatal(e)
	}
	s.mu.Lock()
	cleared := s.ResumeDeadline.IsZero()
	s.mu.Unlock()
	if cleared {
		t.Fatalf("old epoch heartbeat nonce cleared recovery deadline in epoch=%s before any new-channel ping proof", r.Epoch)
	}
}
func TestResumeFINCannotContradictUpNext(t *testing.T) {
	s, _ := testSession(t, true)
	s.SetTransportMode("ws")
	s.Suspend(time.Second, nil)
	req := testReq("r-fin")
	v := "100"
	req.UpFIN = protocol.DirectionFIN{Seen: true, FinalOffset: &v}
	r, e := s.Resume(req, 20*time.Millisecond)
	if e == nil {
		t.Fatalf("accepted FIN final=100 with frozen up_next=0, new epoch=%s", r.Epoch)
	}
}
