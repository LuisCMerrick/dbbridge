package stream

import "testing"

// Trimming beyond capacity drops the oldest receipt, never the newest.
func TestReceiptTrimDropsOldest(t *testing.T) {
	rt := NewReceiptTable(2)
	rt.Record(0, 1, "a")
	rt.Record(1, 1, "b")
	rt.Record(2, 1, "c")
	if rt.Count() != 2 {
		t.Fatalf("count=%d", rt.Count())
	}
	if ex, _, _ := rt.CheckFrame(2, 1, "c"); !ex {
		t.Fatal("newest receipt dropped")
	}
	if ex, _, _ := rt.CheckFrame(0, 1, "a"); ex {
		t.Fatal("oldest receipt kept")
	}
}
