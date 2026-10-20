package protocol

import (
	"strings"
	"testing"
)

func TestBoundaryExactSchema(t *testing.T) {
	for _, body := range []string{
		`{"protocol_version":"bad","PROTOCOL_VERSION":"1.4","expected_node_id":"B","nonce":"n"}`,
		`{"protocol_version":"1.4","expected_node_id":"B","nonce":"n","Nonce":"overridden"}`,
	} {
		var q ProbeRequest
		err := DecodeJSON(strings.NewReader(body), 16384, &q)
		t.Logf("decoded=%+v error=%v", q, err)
		if err == nil {
			t.Error("non-schema alias silently overwrote canonical field")
		}
	}
	var q AckRequest
	if err := DecodeJSON(strings.NewReader(`{"down_delivered_offset":"0","up_confirmed_offset":"0","down_fin_applied":null}`), 16384, &q); err == nil {
		t.Error("null accepted as a boolean")
	}
}
