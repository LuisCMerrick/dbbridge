package ingress

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestUnexpectedSSEEOFClosesClient(t *testing.T) {
	e := e2eIngress(t, "http-sse", "eof_stream")
	e.conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err := e.conn.Read(make([]byte, 1))
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("client remained open after unexpected stream EOF")
	}
	if err == nil {
		t.Fatal("expected closed client")
	}
}
func TestPeerRecoveryOverrideIsSent(t *testing.T) {
	e := e2eIngress(t, "ws", "peer_recovery_off")
	e.conn.Write([]byte("ok"))
	p := make([]byte, 2)
	e.conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(e.conn, p); err != nil {
		t.Fatal(err)
	}
	for _, s := range e.reg.GetAllSessions() {
		if s.ResumeEnabled {
			t.Fatal("peer recovery override ignored")
		}
	}
}
func TestDownstreamOffsetGapIsFatal(t *testing.T) {
	e := e2eIngress(t, "http-sse", "offset_gap")
	e.conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err := e.conn.Read(make([]byte, 1))
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("offset gap did not close client")
	}
	if err == nil {
		t.Fatal("invalid bytes delivered")
	}
}
func TestSecondaryEndpointUsedAfterPrimaryFails(t *testing.T) {
	e := e2eIngress(t, "ws", "failover_create")
	e.conn.SetDeadline(time.Now().Add(2 * time.Second))
	e.conn.Write([]byte("backup"))
	p := make([]byte, 6)
	if _, err := io.ReadFull(e.conn, p); err != nil {
		t.Fatal(err)
	}
	if string(p) != "backup" {
		t.Fatal("byte mismatch")
	}
	if e.created.Load() != 1 {
		t.Fatal("more than one database session")
	}
}

var _ = errors.Is
