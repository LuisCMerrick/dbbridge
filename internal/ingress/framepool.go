package ingress

import (
	"math/bits"
	"sync"
)

// Local-socket read buffers are recycled by size class. Only power-of-two
// sizes from 4 KiB to 1 MiB (the protocol's frame limit) are pooled, so a
// pooled buffer's capacity always equals the payload reservation charged for
// it; other sizes are plain allocations.
const (
	minPooledFrame = 12 // 4 KiB
	maxPooledFrame = 20 // 1 MiB
)

var framePools [maxPooledFrame + 1]sync.Pool

func frameClass(n int) (int, bool) {
	if n <= 0 || n&(n-1) != 0 {
		return 0, false
	}
	c := bits.TrailingZeros(uint(n))
	return c, c >= minPooledFrame && c <= maxPooledFrame
}

// getFrameBuf returns a buffer of length n, recycled when n is a pooled size.
// Its contents are unspecified; the reader overwrites what it uses.
func getFrameBuf(n int) []byte {
	if c, ok := frameClass(n); ok {
		if p, _ := framePools[c].Get().(*[]byte); p != nil {
			return (*p)[:n]
		}
	}
	return make([]byte, n)
}

// putFrameBuf recycles b. The caller must hold the only reference: nothing
// may read or write b afterwards.
func putFrameBuf(b []byte) {
	if c, ok := frameClass(cap(b)); ok {
		b = b[:cap(b)]
		framePools[c].Put(&b)
	}
}
