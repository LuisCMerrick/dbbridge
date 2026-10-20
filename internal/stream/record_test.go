package stream

import "testing"

// RecordChecked refuses an overflow instead of evicting a receipt.
func TestRecordCheckedRefusesOverflow(t *testing.T) {
	rt := NewReceiptTable(2)
	if !rt.RecordChecked(0, 1, "a") || !rt.RecordChecked(1, 1, "b") {
		t.Fatal("record within capacity refused")
	}
	if rt.RecordChecked(2, 1, "c") {
		t.Fatal("overflow accepted")
	}
	if rt.Count() != 2 || !rt.Boundary(1) || !rt.Boundary(2) || rt.Boundary(3) {
		t.Fatal("overflow changed the recorded receipts")
	}
}

// Reset releases everything and clears the FIN marker; the window stays
// positioned at its previous end.
func TestResetIsComplete(t *testing.T) {
	w := NewByteWindow(10, 1024)
	if err := w.Append(10, []byte("abcd")); err != nil {
		t.Fatal(err)
	}
	w.SetFinalOffset(14)
	w.Reset()
	if w.AllocatedBytes() != 0 || w.BufferedBytes() != 0 || w.StartOffset() != 14 || w.EndOffset() != 14 {
		t.Fatalf("reset: alloc=%d size=%d start=%d", w.AllocatedBytes(), w.BufferedBytes(), w.StartOffset())
	}
	if err := w.Append(14, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, _, eof := w.ReadRange(14, 1); eof {
		t.Fatal("stale FIN marker survived Reset")
	}
}
