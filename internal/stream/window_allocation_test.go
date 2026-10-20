package stream

import "testing"

func TestAudit20261018ByteWindowCountsAllocatedCapacity(t *testing.T) {
	const n = 4097
	w := NewByteWindow(0, n)
	if err := w.Append(0, make([]byte, n)); err != nil {
		t.Fatal(err)
	}
	allocated := 0
	for _, block := range w.blocks {
		allocated += cap(block.data)
	}
	t.Logf("direction limit=%d logical/charged bytes=%d allocated slice capacity=%d", n, w.BufferedBytes(), allocated)
	if allocated > w.BufferedBytes() {
		t.Error("raw retained allocation capacity exceeds the direction quota while charged only by payload length")
	}
}

func TestOwnedByteWindowChargesTailAndPartialACK(t *testing.T) {
	w := NewByteWindow(0, 32)
	data := make([]byte, 4, 32)
	copy(data, "data")
	if err := w.AppendOwned(0, data); err != nil {
		t.Fatal(err)
	}
	if w.AllocatedBytes() != 32 || w.AvailableCapacity() != 0 {
		t.Fatal("owned allocation tail was not charged")
	}
	w.Acknowledge(2)
	if w.AllocatedBytes() != 32 || w.AvailableCapacity() != 0 {
		t.Fatal("partial ACK refunded a still-live allocation")
	}
	if got, _, _ := w.ReadRange(2, 2); string(got) != "ta" {
		t.Fatalf("partial ACK changed bytes: %q", got)
	}
	w.Acknowledge(4)
	if w.AllocatedBytes() != 0 || w.AvailableCapacity() != 32 {
		t.Fatal("final ACK did not refund the allocation")
	}
}
