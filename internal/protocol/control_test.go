package protocol

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

const ctlBase = `"session_id":"s","instance_id":"i","epoch":"1","stream_id":"x"`

// Control unions reject unknown types and fields of another type.
func TestControlUnionStrictness(t *testing.T) {
	bad := []string{
		`{"type":"ping",` + ctlBase + `,"nonce":"n","code":"DBB_X","final_offset":"5"}`,
		`{"type":"ack_up",` + ctlBase + `,"ack_up_offset":"1","up_fin_applied":false,"nonce":"n"}`,
		`{"type":"bogus",` + ctlBase + `}`,
		`{"type":"",` + ctlBase + `}`,
	}
	for _, c := range bad {
		var m WSTextControlMessage
		if err := DecodeJSON(strings.NewReader(c), 1<<14, &m); err == nil {
			t.Errorf("accepted %s", c)
		}
	}
	good := []string{
		`{"type":"ping",` + ctlBase + `,"nonce":"n"}`,
		`{"type":"ack_up",` + ctlBase + `,"ack_up_offset":"1","up_fin_applied":true}`,
		`{"type":"fin",` + ctlBase + `,"final_offset":"9"}`,
		`{"type":"error",` + ctlBase + `,"code":"DBB_X","retryable":false,"session_closed":true}`,
	}
	for _, c := range good {
		var m WSTextControlMessage
		if err := DecodeJSON(strings.NewReader(c), 1<<14, &m); err != nil {
			t.Errorf("rejected %s: %v", c, err)
		}
	}
}

// MarshalJSON emits exactly the type's fields, so whatever a
// sender builds (even with stray fields set) decodes strictly on the peer.
func TestControlMarshalRoundTrip(t *testing.T) {
	rid := "r"
	full := WSTextControlMessage{SessionID: "s", InstanceID: "i", Epoch: "1", StreamID: "x", ProtocolVersion: "1.4", AckUpOffset: "3", UpFinApplied: true, Nonce: "n", FinalOffset: "4", Code: "DBB_X", Retryable: true, SessionClosed: true, Transport: "ws", ResumeRequestID: &rid}
	for _, kind := range []string{"ready", "ack_up", "ping", "fin", "error"} {
		m := full
		m.Type = kind
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(b, &obj); err != nil {
			t.Fatal(err)
		}
		if len(obj) != 5+len(controlFields(kind)) {
			t.Errorf("%s: unexpected fields %s", kind, b)
		}
		var back WSTextControlMessage
		if err := DecodeJSON(strings.NewReader(string(b)), 1<<14, &back); err != nil {
			t.Errorf("%s: own encoding rejected: %v (%s)", kind, err, b)
		}
	}
	// Zero-valued required fields are still present.
	b, _ := json.Marshal(WSTextControlMessage{Type: "ack_up", SessionID: "s", InstanceID: "i", Epoch: "1", StreamID: "x", AckUpOffset: "0"})
	if !strings.Contains(string(b), `"up_fin_applied":false`) {
		t.Errorf("missing zero field: %s", b)
	}
}

// Unpaired surrogate escapes are rejected, valid pairs accepted.
func TestRejectLoneSurrogates(t *testing.T) {
	for _, s := range []string{`"s\ud800"`, `"\udc00x"`, `"\ud800\u0041"`, `"\ud83d"`, `"\uD800"`} {
		var v string
		if err := DecodeJSON(strings.NewReader(s), 64, &v); err == nil {
			t.Errorf("accepted %s as %q", s, v)
		}
	}
	for s, want := range map[string]string{`"\ud83d\ude00"`: "\U0001F600", `"\\ud800"`: `\ud800`, `"a\u0041"`: "aA", `"\\\ud83d\ude00"`: "\\\U0001F600"} {
		var v string
		if err := DecodeJSON(strings.NewReader(s), 64, &v); err != nil || v != want {
			t.Errorf("%s: got %q err %v", s, v, err)
		}
	}
}

type testEmbedded struct{ A string }
type testOuter struct {
	testEmbedded
	B string `json:"b"`
}

// Untagged embedded structs are refused instead of mis-validated.
func TestEmbeddedFieldRefused(t *testing.T) {
	var v testOuter
	if err := DecodeJSON(strings.NewReader(`{"A":"x","b":"y"}`), 64, &v); err == nil {
		t.Fatal("embedded struct silently accepted")
	}
}

// Non-canonical base64 and wrong encoded lengths are rejected before
// decoding; FormatSSEDataChecked mirrors ParseSSEData's range checks.
func TestSSEDataStrictBase64(t *testing.T) {
	good := FormatSSEData(0, []byte("A"))
	if _, _, err := ParseSSEData(good); err != nil {
		t.Fatal(err)
	}
	parts := strings.SplitN(good, ":", 4)
	if parts[3] != "QQ==" {
		t.Fatalf("fixture %q", parts[3])
	}
	for _, b64 := range []string{"QR==", "QQ", "QQ==QQ==", base64.StdEncoding.EncodeToString(make([]byte, 4096))} {
		if _, _, err := ParseSSEData(strings.Join(parts[:3], ":") + ":" + b64); err == nil {
			t.Errorf("accepted %q", b64)
		}
	}
	if _, err := FormatSSEDataChecked(0, nil); err == nil {
		t.Error("empty payload accepted")
	}
	if _, err := FormatSSEDataChecked(0, make([]byte, MaxFramePayload+1)); err == nil {
		t.Error("oversized payload accepted")
	}
	if _, err := FormatSSEDataChecked(^uint64(0), []byte("x")); err == nil {
		t.Error("overflowing offset accepted")
	}
	if s, err := FormatSSEDataChecked(7, []byte("x")); err != nil || s != FormatSSEData(7, []byte("x")) {
		t.Errorf("checked format mismatch: %q %v", s, err)
	}
}
