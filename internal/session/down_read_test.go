package session

import (
	"dbbridge/internal/resource"
	"testing"
)

func TestDownReadReservationFollowsAllocation(t *testing.T) {
	s, _ := auditSession(t, false)
	const allocation = 512
	payload := resource.New(allocation)
	r := NewRegistry(1<<20, 4<<20)
	r.SetResources(payload, nil, 0)
	r.Register(s)
	defer r.CloseAll()
	s.SetTransportMode("ws")
	stream, err := s.ReserveStream(1, "ws")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ActivateDataPlane(stream); err != nil {
		t.Fatal(err)
	}
	if !s.ReserveDownRead(allocation) {
		t.Fatal("read reservation failed")
	}
	buf := make([]byte, allocation)
	buf[0] = 'x'
	if _, err := s.FinishDownRead(buf[:1]); err != nil {
		t.Fatal(err)
	}
	// X-DP-3: a very short read is retained as a compact copy, so the charge
	// follows the retained allocation (1 byte), not the discarded read buffer.
	if s.BufferSize() != 1 || payload.Used() != 1 {
		t.Fatalf("short read charge does not follow the retained allocation: retained=%d payload=%d", s.BufferSize(), payload.Used())
	}
	if err := s.BeginEmit(1, 0, 1); err != nil {
		t.Fatal(err)
	}
	s.FinishEmit(1, 1, nil)
	if err := s.AdvanceDownAck(1, false); err != nil {
		t.Fatal(err)
	}
	if s.BufferSize() != 0 || payload.Used() != 0 {
		t.Fatal("complete frame ACK did not refund the full allocation")
	}
	if !s.ReserveDownRead(allocation) {
		t.Fatal("second read reservation failed")
	}
	s.Close()
	if payload.Used() != allocation {
		t.Fatal("closed session refunded an allocation still owned by its reader")
	}
	if _, err := s.FinishDownRead(nil); err != ErrSessionClosed {
		t.Fatal(err)
	}
	if payload.Used() != 0 || s.BufferSize() != 0 {
		t.Fatal("closed read leaked its reservation")
	}
}
