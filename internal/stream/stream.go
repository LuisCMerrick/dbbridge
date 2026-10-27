package stream

import (
	"errors"
	"sync"
)

// ByteWindow buffers contiguous bytes from a start uint64 offset and releases acknowledged offsets.
type byteBlock struct {
	start uint64
	data  []byte
}
type ByteWindow struct {
	mu          sync.Mutex
	startOffset uint64
	blocks      []byteBlock
	size        int
	allocated   int64
	maxCapacity int64
	chunkSize   int
	finalOffset uint64
	hasFinal    bool
}

func NewByteWindow(off uint64, max int64) *ByteWindow {
	return &ByteWindow{startOffset: off, maxCapacity: max, chunkSize: 32 << 10}
}

func (b *ByteWindow) SetChunkSize(size int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if size > 0 {
		b.chunkSize = size
	}
}
func (b *ByteWindow) Append(off uint64, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if off != b.startOffset+uint64(b.size) || uint64(len(data)) > ^uint64(0)-off {
		return errors.New("invalid byte range")
	}
	if int64(len(data)) > b.maxCapacity-b.allocated {
		return errors.New("byte window capacity exceeded")
	}
	for pos := 0; pos < len(data); {
		end := min(len(data), pos+b.chunkSize)
		copyData := make([]byte, end-pos)
		copy(copyData, data[pos:end])
		b.blocks = append(b.blocks, byteBlock{off + uint64(pos), copyData})
		b.allocated += int64(cap(copyData))
		pos = end
	}
	b.size += len(data)
	return nil
}

// AppendOwned adopts a complete allocation. The caller must reserve cap(data)
// bytes before allocating and must not reuse the buffer after a successful call.
func (b *ByteWindow) AppendOwned(off uint64, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if off != b.startOffset+uint64(b.size) || uint64(len(data)) > ^uint64(0)-off || len(data) == 0 {
		return errors.New("invalid byte range")
	}
	cost := int64(cap(data))
	if cost > b.maxCapacity-b.allocated {
		return errors.New("byte window capacity exceeded")
	}
	b.blocks = append(b.blocks, byteBlock{off, data})
	b.size += len(data)
	b.allocated += cost
	return nil
}
func (b *ByteWindow) Acknowledge(off uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if off <= b.startOffset {
		return
	}
	if off > b.startOffset+uint64(b.size) {
		off = b.startOffset + uint64(b.size)
	}
	b.size -= int(off - b.startOffset)
	b.startOffset = off
	keep := b.blocks[:0]
	for _, v := range b.blocks {
		end := v.start + uint64(len(v.data))
		if end <= off {
			b.allocated -= int64(cap(v.data))
			continue
		}
		// Preserve the original slice and start for a partial ACK. ReadRange
		// hides the acknowledged prefix; cap(data) remains the allocation cost.
		keep = append(keep, v)
	}
	for i := len(keep); i < len(b.blocks); i++ {
		b.blocks[i] = byteBlock{}
	}
	b.blocks = keep
}
func (b *ByteWindow) ReadRange(off uint64, max int) ([]byte, uint64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	end := b.startOffset + uint64(b.size)
	if off < b.startOffset || off > end {
		return nil, end, false
	}
	n := int(end - off)
	if n > max {
		n = max
	}
	if n < 0 {
		n = 0
	}
	out := make([]byte, n)
	pos := 0
	for _, v := range b.blocks {
		blockEnd := v.start + uint64(len(v.data))
		if blockEnd <= off {
			continue
		}
		start := 0
		if off > v.start {
			start = int(off - v.start)
		}
		k := copy(out[pos:], v.data[start:])
		pos += k
		off += uint64(k)
		if pos == n {
			break
		}
	}
	return out, off, b.hasFinal && off == b.finalOffset
}

// Segments returns up to limit bytes from off as views of the window's blocks,
// without copying. Blocks are immutable once appended (Append copies,
// AppendOwned adopts a buffer its caller no longer touches) and Acknowledge
// or Reset only drop references, so the views stay valid after the lock is
// released and after the range is acknowledged; the caller must not modify
// them. ok is false when off is outside the window.
func (b *ByteWindow) Segments(off uint64, limit int) (segs [][]byte, next uint64, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	end := b.startOffset + uint64(b.size)
	if off < b.startOffset || off > end {
		return nil, end, false
	}
	if limit < 0 {
		limit = 0
	}
	n := int(min(end-off, uint64(limit)))
	for _, v := range b.blocks {
		if n == 0 {
			break
		}
		blockEnd := v.start + uint64(len(v.data))
		if blockEnd <= off {
			continue
		}
		seg := v.data[off-v.start:]
		if len(seg) > n {
			seg = seg[:n]
		}
		segs = append(segs, seg)
		n -= len(seg)
		off += uint64(len(seg))
	}
	return segs, off, true
}

// Reset releases every buffered byte and clears the FIN marker. The
// window stays positioned at its previous end offset, so a reused window keeps
// accepting appends at the next contiguous offset.
func (b *ByteWindow) Reset() {
	b.mu.Lock()
	b.startOffset += uint64(b.size)
	b.blocks = nil
	b.size = 0
	b.allocated = 0
	b.hasFinal = false
	b.finalOffset = 0
	b.mu.Unlock()
}
func (b *ByteWindow) SetFinalOffset(off uint64) {
	b.mu.Lock()
	b.hasFinal = true
	b.finalOffset = off
	b.mu.Unlock()
}
func (b *ByteWindow) StartOffset() uint64 { b.mu.Lock(); defer b.mu.Unlock(); return b.startOffset }
func (b *ByteWindow) EndOffset() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.startOffset + uint64(b.size)
}
func (b *ByteWindow) BufferedBytes() int { b.mu.Lock(); defer b.mu.Unlock(); return b.size }
func (b *ByteWindow) AllocatedBytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.allocated
}
func (b *ByteWindow) AvailableCapacity() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.maxCapacity - b.allocated
}

// DigestReceipt records an upstream frame that was verified and written to backend socket.
type DigestReceipt struct {
	Offset uint64
	Length int
	SHA256 string
}

// ReceiptTable tracks recent verified upstream frames for idempotency and de-duplication.
type ReceiptTable struct {
	mu           sync.Mutex
	receiptFloor uint64
	receipts     []DigestReceipt
	maxReceipts  int
}

func NewReceiptTable(maxReceipts int) *ReceiptTable {
	if maxReceipts <= 0 {
		maxReceipts = 64
	}
	return &ReceiptTable{
		receiptFloor: 0,
		receipts:     make([]DigestReceipt, 0, maxReceipts),
		maxReceipts:  maxReceipts,
	}
}

func (rt *ReceiptTable) ReceiptFloor() uint64 {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.receiptFloor
}

// CheckFrame verifies if the frame is already recorded or too old.
// Returns:
// exists: true if matching offset, length, and sha256 exists
// conflict: true if same offset/range exists but length or sha256 mismatch
// tooOld: true if entire frame is below receiptFloor
func (rt *ReceiptTable) CheckFrame(offset uint64, length int, sha string) (exists bool, conflict bool, tooOld bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	end := offset + uint64(length)
	if end <= rt.receiptFloor {
		return false, false, true
	}

	for _, r := range rt.receipts {
		rEnd := r.Offset + uint64(r.Length)
		// Check exact match
		if r.Offset == offset && r.Length == length {
			if r.SHA256 == sha {
				return true, false, false
			}
			return false, true, false
		}
		// Check partial overlap conflict
		if !(end <= r.Offset || offset >= rEnd) {
			return false, true, false
		}
	}
	return false, false, false
}

// Record records a newly written frame.
func (rt *ReceiptTable) Record(offset uint64, length int, sha string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.receipts = append(rt.receipts, DigestReceipt{
		Offset: offset,
		Length: length,
		SHA256: sha,
	})

	// Callers keep receipts within capacity. If that is ever violated, drop
	// the oldest receipt rather than the one just written: a retransmit of the
	// newest frame is the most likely duplicate. A dropped old frame is still
	// rejected as too old by the session (its end is at or below upAckOffset).
	if n := len(rt.receipts) - rt.maxReceipts; n > 0 {
		rt.receipts = append(rt.receipts[:0], rt.receipts[n:]...)
	}

}

// RecordChecked records a newly written frame only while the table has room.
// It returns false, without recording or evicting anything, when the table is
// full, so the caller can treat the overflow as the invariant violation it is
// instead of losing an unconfirmed receipt.
func (rt *ReceiptTable) RecordChecked(offset uint64, length int, sha string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if len(rt.receipts) >= rt.maxReceipts {
		return false
	}
	rt.receipts = append(rt.receipts, DigestReceipt{Offset: offset, Length: length, SHA256: sha})
	return true
}

// ConfirmAdvancesReceiptFloor advances receiptFloor up to upConfirmedOffset and trims older receipts.
func (rt *ReceiptTable) ConfirmAdvancesReceiptFloor(upConfirmedOffset uint64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	if upConfirmedOffset <= rt.receiptFloor {
		return
	}
	rt.receiptFloor = upConfirmedOffset

	var remaining []DigestReceipt
	for _, r := range rt.receipts {
		if r.Offset+uint64(r.Length) > rt.receiptFloor {
			remaining = append(remaining, r)
		}
	}
	rt.receipts = remaining
}

func (rt *ReceiptTable) Count() int    { rt.mu.Lock(); defer rt.mu.Unlock(); return len(rt.receipts) }
func (rt *ReceiptTable) Capacity() int { return rt.maxReceipts }
func (rt *ReceiptTable) Boundary(off uint64) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if off == rt.receiptFloor {
		return true
	}
	for _, r := range rt.receipts {
		if r.Offset+uint64(r.Length) == off {
			return true
		}
	}
	return false
}

func (rt *ReceiptTable) Clear() { rt.mu.Lock(); defer rt.mu.Unlock(); rt.receipts = nil }
