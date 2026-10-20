package stream_test

import (
	"testing"

	"dbbridge/internal/stream"
)

func TestByteWindowAppendAndRead(t *testing.T) {
	bw := stream.NewByteWindow(0, 1024*1024)

	data1 := []byte("hello ")
	data2 := []byte("world!")

	if err := bw.Append(0, data1); err != nil {
		t.Fatalf("Append data1 failed: %v", err)
	}
	if err := bw.Append(uint64(len(data1)), data2); err != nil {
		t.Fatalf("Append data2 failed: %v", err)
	}

	expectedLen := uint64(len(data1) + len(data2))
	if bw.EndOffset() != expectedLen {
		t.Errorf("expected end offset %d, got %d", expectedLen, bw.EndOffset())
	}

	// Read range
	readData, nextOffset, eof := bw.ReadRange(0, 100)
	if eof {
		t.Errorf("unexpected eof")
	}
	if string(readData) != "hello world!" {
		t.Errorf("read data mismatch: got %q", string(readData))
	}
	if nextOffset != expectedLen {
		t.Errorf("expected next offset %d, got %d", expectedLen, nextOffset)
	}

	// Acknowledge partial
	bw.Acknowledge(6)
	if bw.StartOffset() != 6 {
		t.Errorf("expected start offset 6, got %d", bw.StartOffset())
	}

	// Read remaining
	remaining, _, _ := bw.ReadRange(6, 100)
	if string(remaining) != "world!" {
		t.Errorf("remaining mismatch: got %q", string(remaining))
	}
}

func TestByteWindowGapDetection(t *testing.T) {
	bw := stream.NewByteWindow(0, 1024*1024)
	if err := bw.Append(10, []byte("invalid offset")); err == nil {
		t.Errorf("expected error on offset gap, got nil")
	}
}

func TestReceiptTableDeDuplication(t *testing.T) {
	rt := stream.NewReceiptTable(4)

	// Record frame at 0..10
	rt.Record(0, 10, "sha_a")

	// Same frame check -> exists
	exists, conflict, tooOld := rt.CheckFrame(0, 10, "sha_a")
	if !exists || conflict || tooOld {
		t.Fatalf("expected frame to exist, got exists=%v, conflict=%v, tooOld=%v", exists, conflict, tooOld)
	}

	// Same range, different sha -> conflict
	exists, conflict, tooOld = rt.CheckFrame(0, 10, "sha_b")
	if exists || !conflict || tooOld {
		t.Fatalf("expected conflict for mismatched sha, got conflict=%v", conflict)
	}

	// Advance receipt floor past 10
	rt.ConfirmAdvancesReceiptFloor(10)
	if rt.ReceiptFloor() != 10 {
		t.Errorf("expected receipt floor 10, got %d", rt.ReceiptFloor())
	}

	// Frame at 0..10 is now below receipt floor -> tooOld
	_, _, tooOld = rt.CheckFrame(0, 10, "sha_a")
	if !tooOld {
		t.Fatalf("expected frame below receipt floor to be tooOld, got tooOld=%v", tooOld)
	}
}
