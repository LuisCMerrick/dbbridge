package ingress

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
)

// With recovery disabled the client
// socket closes before the FIN-adjacent final-ACK query, but that query still
// runs afterwards: when egress confirms down_fin_acked the session is
// classified as a normal close and no cleanup DELETE is sent.
func TestFinalAckAfterFailureCloseKeepsNormalClassification(t *testing.T) {
	var finAck atomic.Bool
	var answered atomic.Int64
	var last atomic.Value
	const ackDelay = 300 * time.Millisecond
	e, deletes := dataplaneSetup(t, "ws", true, func(c *config.Config) { c.Transport.Recovery.EnabledRaw = false }, nil, func(w http.ResponseWriter, r *http.Request, base http.Handler) bool {
		if !strings.HasSuffix(r.URL.Path, "/ack") {
			return false
		}
		if finAck.Load() {
			// egress has not seen these FINs, so answer with the last
			// genuine response, now confirming the downstream FIN.
			resp, _ := last.Load().(protocol.AckResponse)
			var q protocol.AckRequest
			_ = json.NewDecoder(r.Body).Decode(&q)
			resp.ProbeID = q.ProbeID
			time.Sleep(ackDelay)
			resp.UpFinApplied, resp.DownFinAcked = true, true
			w.Header().Set(protocol.HeaderInstanceID, resp.InstanceID)
			_ = json.NewEncoder(w).Encode(resp)
			answered.Add(1)
			return true
		}
		rec := httptest.NewRecorder()
		base.ServeHTTP(rec, r)
		var resp protocol.AckResponse
		if rec.Code == http.StatusOK && json.Unmarshal(rec.Body.Bytes(), &resp) == nil {
			last.Store(resp)
		}
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
		return true
	})
	if err := testEcho2(t, e.conn, "fin-adjacent"); err != nil {
		t.Fatal(err)
	}
	s := testSession(t, e.m)
	time.Sleep(50 * time.Millisecond)
	finAck.Store(true)
	s.mu.Lock()
	g := s.g
	s.upEOF, s.upFINApplied, s.downFIN, s.downFINAcked = true, true, true, false
	s.mu.Unlock()
	start := time.Now()
	s.fault(g, observability.FaultLiveness, context.DeadlineExceeded, false)
	_ = e.conn.SetReadDeadline(start.Add(4 * time.Second))
	_, err := e.conn.Read(make([]byte, 1))
	if d := time.Since(start); err == nil || d >= ackDelay {
		t.Fatalf("client close waited for the final ACK: err=%v after %v", err, d)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !s.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	<-e.m.cleanups.idle()
	if !s.closed.Load() || answered.Load() == 0 {
		t.Fatalf("final ACK not settled: closed=%v answered=%d", s.closed.Load(), answered.Load())
	}
	if deletes.Load() != 0 {
		t.Fatalf("normal close sent %d cleanup DELETE(s)", deletes.Load())
	}
}
