package stream

import "testing"

func TestRegressionUnconfirmedReceiptEviction(t *testing.T) {
	r := NewReceiptTable(2)
	r.Record(0, 1, "a")
	r.Record(1, 1, "b")
	r.Record(2, 1, "c")
	if floor := r.ReceiptFloor(); floor != 0 {
		t.Fatalf("unconfirmed receipt evicted, floor advanced to %d without confirmation", floor)
	}
}
