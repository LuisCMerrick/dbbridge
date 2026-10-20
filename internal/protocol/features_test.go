package protocol

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestHasFeature(t *testing.T) {
	h := http.Header{}
	if HasFeature(h, FeatureFlowLimits) {
		t.Fatal("empty header offers nothing")
	}
	h.Add(HeaderFeatures, "other, Flow-Limits ")
	if !HasFeature(h, FeatureFlowLimits) {
		t.Fatal("feature in a list, any case, must match")
	}
	h = http.Header{HeaderFeatures: {"flow-limitsx"}}
	if HasFeature(h, FeatureFlowLimits) {
		t.Fatal("prefix must not match")
	}
}

// The extension fields stay off the wire unless set, so a plain 1.4 peer
// (strict decoder, no such fields) still accepts the message.
func TestFlowLimitFieldsOmittedWhenZero(t *testing.T) {
	b, _ := json.Marshal(SessionLimits{MaxFrameSize: 1, UpMaxInflightFrames: 1, UpWindowBytes: 1, DownBufferBytes: 1})
	if strings.Contains(string(b), "up_receipt_limit") || strings.Contains(string(b), "down_max_frames") {
		t.Fatalf("zero extension fields encoded: %s", b)
	}
	var back SessionLimits
	if err := DecodeJSON(bytes.NewReader(b), 1<<10, &back); err != nil {
		t.Fatal(err)
	}
	b, _ = json.Marshal(SessionLimits{MaxFrameSize: 1, UpMaxInflightFrames: 1, UpWindowBytes: 1, DownBufferBytes: 1, UpReceiptLimit: 64, DownMaxFrames: 512})
	if err := DecodeJSON(bytes.NewReader(b), 1<<10, &back); err != nil || back.UpReceiptLimit != 64 || back.DownMaxFrames != 512 {
		t.Fatalf("round trip: %v %+v", err, back)
	}
}

func TestAckRequestedOnlyOnAckUp(t *testing.T) {
	m := WSTextControlMessage{Type: "ack_up", SessionID: "s", InstanceID: "i", Epoch: "1", StreamID: "x", AckUpOffset: "5"}
	b, _ := json.Marshal(m)
	if strings.Contains(string(b), "ack_requested") {
		t.Fatalf("unset hint encoded: %s", b)
	}
	m.AckRequested = true
	b, _ = json.Marshal(m)
	var got WSTextControlMessage
	if err := DecodeJSON(bytes.NewReader(b), 1<<10, &got); err != nil || !got.AckRequested {
		t.Fatalf("ack_up with hint: %v %s", err, b)
	}
	ping := WSTextControlMessage{Type: "ping", SessionID: "s", InstanceID: "i", Epoch: "1", StreamID: "x", Nonce: "n", AckRequested: true}
	b, _ = json.Marshal(ping)
	if strings.Contains(string(b), "ack_requested") {
		t.Fatalf("hint encoded on ping: %s", b)
	}
	bad := []byte(`{"type":"ping","session_id":"s","instance_id":"i","epoch":"1","stream_id":"x","nonce":"n","ack_requested":true}`)
	if err := DecodeJSON(bytes.NewReader(bad), 1<<10, &got); err == nil {
		t.Fatal("ack_requested on ping must be rejected")
	}
}

func TestClampAdvertisedFrames(t *testing.T) {
	for in, want := range map[int]int{-1: -1, 0: 0, 64: 64, MaxAdvertisedFrames: MaxAdvertisedFrames, MaxAdvertisedFrames + 1: MaxAdvertisedFrames, 1 << 30: MaxAdvertisedFrames} {
		if got := ClampAdvertisedFrames(in); got != want {
			t.Errorf("ClampAdvertisedFrames(%d)=%d want %d", in, got, want)
		}
	}
}

// The flow-limits fields exist only in create responses; a create request
// carrying them has unknown fields.
func TestCreateRequestRejectsResponseOnlyLimitFields(t *testing.T) {
	q := CreateSessionRequest{ProtocolVersion: "1.4", Transport: TransportMode{Mode: "ws"}, CreateRequestID: "c", IngressInstanceID: "i", TargetID: "t", Limits: SessionLimits{MaxFrameSize: 1, UpMaxInflightFrames: 1, UpWindowBytes: 1, DownBufferBytes: 1}}
	b, _ := json.Marshal(q)
	var got CreateSessionRequest
	if err := DecodeJSON(bytes.NewReader(b), 4<<10, &got); err != nil {
		t.Fatalf("plain request: %v", err)
	}
	for _, field := range []string{"up_receipt_limit", "down_max_frames"} {
		q.Limits.UpReceiptLimit, q.Limits.DownMaxFrames = 0, 0
		if field == "up_receipt_limit" {
			q.Limits.UpReceiptLimit = 64
		} else {
			q.Limits.DownMaxFrames = 512
		}
		b, _ := json.Marshal(q)
		if err := DecodeJSON(bytes.NewReader(b), 4<<10, &got); err == nil || !strings.Contains(err.Error(), field) {
			t.Fatalf("%s in a create request: err=%v", field, err)
		}
	}
	var resp CreateSessionResponse
	r := CreateSessionResponse{ProtocolVersion: "1.4", NodeID: "n", InstanceID: "i", SessionID: "s", SessionToken: "t", Epoch: "1", Transport: TransportMode{Mode: "ws"}, Limits: SessionLimits{MaxFrameSize: 1, UpMaxInflightFrames: 1, UpWindowBytes: 1, DownBufferBytes: 1, UpReceiptLimit: 64, DownMaxFrames: 512}, UnattachedTimeoutMs: 1}
	b, _ = json.Marshal(r)
	if err := DecodeJSON(bytes.NewReader(b), 4<<10, &resp); err != nil || !resp.Limits.FlowLimitsNegotiated() {
		t.Fatalf("response with flow limits: %v", err)
	}
}
