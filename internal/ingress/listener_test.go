package ingress

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestAuditUnexpectedSSEEOFClosesClient(t *testing.T) {
	e := reviewIngress(t, "http-sse", "eof_stream")
	e.conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err := e.conn.Read(make([]byte, 1))
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("client remained open after unexpected stream EOF")
	}
	if err == nil {
		t.Fatal("expected closed client")
	}
}
func TestAuditPeerRecoveryOverrideIsSent(t *testing.T) {
	e := reviewIngress(t, "ws", "peer_recovery_off")
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
func TestAuditDownstreamOffsetGapIsFatal(t *testing.T) {
	e := reviewIngress(t, "http-sse", "offset_gap")
	e.conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err := e.conn.Read(make([]byte, 1))
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("offset gap did not close client")
	}
	if err == nil {
		t.Fatal("invalid bytes delivered")
	}
}
func TestAuditSecondaryEndpointUsedAfterPrimaryFails(t *testing.T) {
	e := reviewIngress(t, "ws", "failover_create")
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
