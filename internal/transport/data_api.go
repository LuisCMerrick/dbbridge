package transport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"dbbridge/internal/protocol"
	"encoding/hex"
	"net/http"
	"strconv"
)

func (c *TripleTransportClient) SendUpFrame(ctx context.Context, sid, token, inst string, ep, off uint64, data []byte, down, confirmed uint64) (*protocol.UpResponse, error) {
	b, e := c.getBinding(sid, ep)
	if e != nil {
		return nil, e
	}
	sum := sha256.Sum256(data)
	h := http.Header{"Content-Type": []string{"application/octet-stream"}, protocol.HeaderOffset: []string{strconv.FormatUint(off, 10)}, protocol.HeaderLength: []string{strconv.Itoa(len(data))}, protocol.HeaderSHA256: []string{hex.EncodeToString(sum[:])}, protocol.HeaderAckDownOffset: []string{strconv.FormatUint(down, 10)}, protocol.HeaderConfirmUpOffset: []string{strconv.FormatUint(confirmed, 10)}}
	var r protocol.UpResponse
	e = c.request(ctx, c.dataClient, "POST", sessionPath(sid, "/up", ep, true), bytes.NewReader(data), sid, token, inst, ep, h, &r, 200, 202)
	if e == nil {
		e = protocol.ValidateBinding(r.SessionID, r.InstanceID, r.Epoch, r.StreamID, r.Transport, sid, inst, ep, b.id, b.mode)
	}
	return &r, e
}
