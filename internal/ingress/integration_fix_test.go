package ingress

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dbbridge/internal/protocol"
)

// Integration follow-up X-DP-6: a trusted 410 DBB_SESSION_CLOSED whose
// message says the owner closed the session normally ends the ingress session
// as closed_normal even when the local FIN state is incomplete (the ack that
// would have told us was lost with the carrier), and no cleanup DELETE is
// sent for a session the owner already closed.
func TestIntegrationResume410NormalMessageEndsNormally(t *testing.T) {
	hook := func(w http.ResponseWriter, r *http.Request, base http.Handler) bool {
		if !strings.HasSuffix(r.URL.Path, "/resume") {
			return false
		}
		rec := httptest.NewRecorder()
		base.ServeHTTP(rec, r)
		w.Header().Set(protocol.HeaderInstanceID, rec.Header().Get(protocol.HeaderInstanceID))
		w.WriteHeader(http.StatusGone)
		_ = json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: protocol.ErrSessionClosed, Message: protocol.SessionClosedNormalMessage, SessionClosed: true})
		return true
	}
	e, deletes := fix2Setup(t, "ws", true, nil, nil, hook)
	e.conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := fixEcho2(t, e.conn, "x"); err != nil {
		t.Fatal(err)
	}
	s := fix2Session(t, e.m)
	s.mu.Lock()
	g := s.g
	s.mu.Unlock()
	g.ws.Close()
	if _, err := e.conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the local connection to close")
	}
	for i := 0; i < 200 && e.m.Count() > 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	s.mu.Lock()
	owner := s.ownerClosedNormal
	s.mu.Unlock()
	if !owner {
		t.Fatal("410 normal message not treated as a normal close")
	}
	e.m.Close()
	if n := deletes.Load(); n != 0 {
		t.Fatalf("owner-closed session sent %d cleanup DELETE(s)", n)
	}
}
