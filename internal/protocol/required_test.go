package protocol

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestRequiredJSONFieldsAndOptionalFIN(t *testing.T) {
	request := CreateSessionRequest{ProtocolVersion: "1.4", Transport: TransportMode{Mode: "ws"}, Recovery: RecoveryNegotiation{}, CreateRequestID: "create", IngressInstanceID: "ai", TargetID: "db", Limits: SessionLimits{MaxFrameSize: 1024, UpMaxInflightFrames: 8, UpWindowBytes: 1024, DownBufferBytes: 1024}}
	b, _ := json.Marshal(request)
	for _, name := range []string{"protocol_version", "transport", "recovery", "create_request_id", "ingress_instance_id", "target_id", "limits"} {
		t.Run(name, func(t *testing.T) {
			var object map[string]json.RawMessage
			json.Unmarshal(b, &object)
			delete(object, name)
			missing, _ := json.Marshal(object)
			var got CreateSessionRequest
			if err := DecodeJSON(bytes.NewReader(missing), 16384, &got); err == nil {
				t.Fatal("omitted field accepted")
			}
		})
	}
	var fin DirectionFIN
	if err := DecodeJSON(bytes.NewBufferString(`{"seen":false}`), 16384, &fin); err != nil {
		t.Fatalf("optional FIN offsets: %v", err)
	}
	var rec RecoveryNegotiation
	if err := DecodeJSON(bytes.NewBufferString(`{"enabled":false}`), 16384, &rec); err == nil {
		t.Fatal("missing grace_ms accepted")
	}
	var ready ReadyEventData
	if err := DecodeJSON(bytes.NewBufferString(`{"type":"ready","protocol_version":"1.4","session_id":"s","instance_id":"bi","epoch":"1","stream_id":"st","transport":"ws","resume_request_id":null}`), 16384, &ready); err != nil {
		t.Fatalf("explicit nullable initial resume ID: %v", err)
	}
}

func TestControlFalseFieldsAndACKDiagnostics(t *testing.T) {
	m := WSTextControlMessage{Type: "ack_up", SessionID: "s", InstanceID: "bi", Epoch: "1", StreamID: "st", AckUpOffset: "0", UpFinApplied: false}
	b, _ := json.Marshal(m)
	var ctrl WSTextControlMessage
	if err := DecodeJSON(bytes.NewReader(b), 16384, &ctrl); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	json.Unmarshal(b, &fields)
	delete(fields, "up_fin_applied")
	b, _ = json.Marshal(fields)
	if err := DecodeJSON(bytes.NewReader(b), 16384, &ctrl); err == nil {
		t.Fatal("missing ack_up false state accepted")
	}
	ack := AckRequest{StreamID: "st", DownDeliveredOffset: "0", UpConfirmedOffset: "0", BackpressureReason: "none"}
	b, _ = json.Marshal(ack)
	for _, name := range []string{"data_tx_blocked", "data_rx_blocked", "backpressure_reason"} {
		json.Unmarshal(b, &fields)
		delete(fields, name)
		missing, _ := json.Marshal(fields)
		var q AckRequest
		if err := DecodeJSON(bytes.NewReader(missing), 16384, &q); err == nil {
			t.Fatalf("missing %s accepted", name)
		}
	}
}
