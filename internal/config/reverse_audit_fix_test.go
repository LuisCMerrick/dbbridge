package config

import (
	"strings"
	"testing"
)

// Two reverse peers expecting the same node could not be told apart by the
// public side when they share a token.
func TestReversePeersNeedDistinctExpectedNode(t *testing.T) {
	body := strings.Replace(publicSide(`  - id: plant2
    mode: reverse
    expected_node_id: plant-a
    credential_ref: plant2
    token_env: DBB_TEST_TOKEN`, ""), "listeners:", "listeners:", 1)
	_, err := testLoad(t, body)
	if err == nil || !strings.Contains(err.Error(), "distinct expected_node_id") {
		t.Fatalf("duplicate reverse expected_node_id accepted: %v", err)
	}
	ok := strings.Replace(body, "expected_node_id: plant-a\n    credential_ref: plant2", "expected_node_id: plant-z\n    credential_ref: plant2", 1)
	if _, err := testLoad(t, ok); err != nil {
		t.Fatalf("distinct reverse peers sharing a token rejected: %v", err)
	}
}

// The database side must have room for the stream windows of its first
// session (and an HTTP carrier's buffers unless it is WebSocket only).
func TestReverseCodecBudgetChecked(t *testing.T) {
	_, err := testLoad(t, dbSide(dbSideBase, "transport: {codec_buffer_limit: 2MiB}\n"))
	if err == nil || !strings.Contains(err.Error(), "too small for reverse tunnels") {
		t.Fatalf("2MiB codec accepted for reverse auto transport: %v", err)
	}
	c, err := testLoad(t, dbSide(dbSideBase, "transport: {codec_buffer_limit: 4MiB}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if need := c.minReverseCodecBudget(); need > c.Transport.CodecBufferLimit || need <= c.minCodecBudget() {
		t.Fatalf("reverse codec minimum %d (codec %d, data plane %d)", need, c.Transport.CodecBufferLimit, c.minCodecBudget())
	}
	if w := c.Transport.TunnelStreamWindow(); w != 256<<10 {
		t.Fatalf("stream window %d with a 4MiB codec, want 256KiB", w)
	}
}
