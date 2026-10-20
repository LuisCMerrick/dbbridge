package transport

import (
	"context"
	"dbbridge/internal/protocol"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDeleteRejectsUnboundGatewaySuccess(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("X-DBB-Instance-ID", "wrong-instance")
		_, _ = w.Write([]byte("<html>gateway default success page</html>"))
	}))
	defer ts.Close()
	c := testHTTPClient(ts.URL)
	defer c.Close()
	err := c.DeleteSession(context.Background(), "original-session", "session-token", "original-instance", 7)
	t.Logf("DELETE with wrong response instance and HTML body => error=%v", err)
	if err == nil {
		t.Error("unbound gateway response is reported as confirmed remote session closure")
	}
}

func TestDeleteResponseRequiresOriginalBinding(t *testing.T) {
	valid := protocol.DeleteSessionResponse{SessionID: "sid", InstanceID: "bi", Epoch: "7", Closed: true}
	for _, kind := range []string{"valid", "session", "instance", "epoch", "not_closed", "legacy", "no_content"} {
		t.Run(kind, func(t *testing.T) {
			response := valid
			switch kind {
			case "session":
				response.SessionID = "another-session"
			case "instance":
				response.InstanceID = "another-instance"
			case "epoch":
				response.Epoch = "6"
			case "not_closed":
				response.Closed = false
			}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if kind == "no_content" {
					w.WriteHeader(204)
				} else if kind == "legacy" {
					_, _ = w.Write([]byte(`{"closed":true}`))
				} else {
					_ = json.NewEncoder(w).Encode(response)
				}
			}))
			defer ts.Close()
			c := testHTTPClient(ts.URL)
			defer c.Close()
			err := c.DeleteSession(context.Background(), "sid", "secret", "bi", 7)
			if (err == nil) != (kind == "valid") {
				t.Fatalf("delete binding case=%s error=%v", kind, err)
			}
		})
	}
}

// A resume response names its upstream FIN flag "received"; requests use
// "seen". A self-test between two nodes of this code would not notice a
// mismatch, so the response is checked against the protocol schema.
func TestResumeResponseMatchesDocumentedFINSchema(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"session_id":"sid","instance_id":"bi","resume_request_id":"request","epoch":"2","transport":{"mode":"ws"},"up_written_offset":"0","ack_up_offset":"0","down_replay_from":"0","down_produced_offset":"0","down_retained_from":"0","up_fin":{"received":false,"final_offset":null,"applied":false},"down_fin":{"seen":false,"final_offset":null,"acked":false},"resume_remaining_ms":1000}`))
	}))
	defer ts.Close()
	c := testHTTPClient(ts.URL)
	defer c.Close()
	_, err := c.ResumeSession(context.Background(), "sid", "secret", "bi", protocol.ResumeRequest{ExpectedEpoch: "1", ResumeRequestID: "request", Transport: protocol.TransportMode{Mode: "ws"}, UpNextOffset: "0", KnownUpAckOffset: "0", DownDeliveredOffset: "0"})
	t.Logf("section 5.8 up_fin.received response => error=%v", err)
	if err != nil {
		t.Error("documented 1.4 response cannot be decoded by the current peer")
	}
}
