package protocol

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"strconv"
)

func segmentsLen(segs [][]byte) int {
	n := 0
	for _, s := range segs {
		n += len(s)
	}
	return n
}

func checkSegments(epochOK bool, offset uint64, n int) error {
	if !epochOK || n == 0 || n > MaxFramePayload || offset > math.MaxUint64-uint64(n) {
		return errors.New("invalid frame range")
	}
	return nil
}

// EncodeWSFrameSegments is EncodeWSFrameChecked for a payload held in several
// read-only segments: the payload is copied once, straight into the frame.
func EncodeWSFrameSegments(epoch, offset uint64, segs [][]byte) ([]byte, error) {
	n := segmentsLen(segs)
	if err := checkSegments(epoch != 0, offset, n); err != nil {
		return nil, err
	}
	msg := make([]byte, WSBinaryHeaderLen+n)
	pos := WSBinaryHeaderLen
	for _, s := range segs {
		pos += copy(msg[pos:], s)
	}
	sum := sha256.Sum256(msg[WSBinaryHeaderLen:])
	msg[0] = WSDataType
	binary.BigEndian.PutUint64(msg[4:12], epoch)
	binary.BigEndian.PutUint64(msg[12:20], offset)
	binary.BigEndian.PutUint32(msg[20:24], uint32(n))
	copy(msg[24:56], sum[:])
	return msg, nil
}

// AppendSSEDataSegments appends the FormatSSEData encoding
// (<offset>:<length>:<sha256>:<base64_payload>) of a payload held in several
// read-only segments to dst, without first joining the segments.
func AppendSSEDataSegments(dst []byte, offset uint64, segs [][]byte) ([]byte, error) {
	n := segmentsLen(segs)
	if err := checkSegments(true, offset, n); err != nil {
		return nil, err
	}
	h := sha256.New()
	for _, s := range segs {
		h.Write(s)
	}
	var sum [sha256.Size]byte
	h.Sum(sum[:0])
	dst = strconv.AppendUint(dst, offset, 10)
	dst = append(dst, ':')
	dst = strconv.AppendInt(dst, int64(n), 10)
	dst = append(dst, ':')
	dst = hex.AppendEncode(dst, sum[:])
	dst = append(dst, ':')
	// Base64 works on 3-byte groups; carry a segment's tail into the next.
	var carry [3]byte
	c := 0
	for _, s := range segs {
		if c > 0 {
			k := copy(carry[c:], s)
			c += k
			s = s[k:]
			if c < 3 {
				continue
			}
			dst = base64.StdEncoding.AppendEncode(dst, carry[:])
			c = 0
		}
		whole := len(s) - len(s)%3
		dst = base64.StdEncoding.AppendEncode(dst, s[:whole])
		c = copy(carry[:], s[whole:])
	}
	if c > 0 {
		dst = base64.StdEncoding.AppendEncode(dst, carry[:c])
	}
	return dst, nil
}

// EncodeWSFrameHeader returns the 56-byte DATA frame header for payload, with
// the checks of EncodeWSFrameChecked, for writers that send header and
// payload without joining them.
func EncodeWSFrameHeader(epoch, offset uint64, payload []byte) (h [WSBinaryHeaderLen]byte, err error) {
	if err = checkSegments(epoch != 0, offset, len(payload)); err != nil {
		return h, err
	}
	sum := sha256.Sum256(payload)
	h[0] = WSDataType
	binary.BigEndian.PutUint64(h[4:12], epoch)
	binary.BigEndian.PutUint64(h[12:20], offset)
	binary.BigEndian.PutUint32(h[20:24], uint32(len(payload)))
	copy(h[24:56], sum[:])
	return h, nil
}
