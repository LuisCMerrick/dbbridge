package config

import (
	"errors"
	"fmt"
)

func (c *Config) validateMetadataBudget() error {
	if int64(c.Transport.ReceiptLimitPerSession) > c.Transport.MetadataLimit/160 || int64(c.Transport.UpMaxInflightFrames) > c.Transport.MetadataLimit/192 {
		return errors.New("session metadata exceeds process metadata budget")
	}
	if c.Transport.CodecBufferLimit < c.minCodecBudget() || c.Transport.MetadataLimit < 512*1024 {
		return errors.New("codec/metadata budget too small")
	}
	// Subtract before multiplying so explicit huge sizes cannot wrap a signed
	// metadata estimate. Include block headers as well as frames and receipts.
	metaLimit := uint64(c.Transport.MetadataLimit - 2*min(c.Transport.MetadataLimit/4, 1<<20))
	metaCost := uint64(64 << 10)
	for _, part := range []struct{ count, size uint64 }{
		{uint64(c.Transport.ReceiptLimitPerSession), 160},
		{uint64(c.Transport.UpMaxInflightFrames), 192},
		{uint64(c.Transport.BufferPerDirection/int64(c.Transport.ChunkSize)) + 513, 32},
	} {
		if metaCost > metaLimit || part.count > (metaLimit-metaCost)/part.size {
			return errors.New("session metadata exceeds available process budget")
		}
		metaCost += part.count * part.size
	}
	// every concurrent session (plus its idempotency tombstone) must
	// fit at once; otherwise creates fail with a misleading quota error long
	// before limits.max_sessions is reached.
	perSession := metaCost + sessionTombstoneMetadata
	if sessions := uint64(c.Limits.MaxSessions); sessions > metaLimit/perSession {
		need := sessions*perSession + uint64(c.Transport.MetadataLimit) - metaLimit
		return fmt.Errorf("limits.max_sessions (%d) x per-session metadata (~%d KiB) exceeds the process metadata budget (%d KiB of transport.metadata_limit); raise transport.metadata_limit to at least %d MiB or lower limits.max_sessions to %d",
			sessions, perSession>>10, metaLimit>>10, (need+(1<<20)-1)>>20, metaLimit/perSession)
	}
	return nil
}

// sessionTombstoneMetadata approximates the metadata kept per session after
// it closes (idempotency record and tombstone, ~8 KiB measured).
const sessionTombstoneMetadata = 8 << 10

// minCodecBudget is the smallest codec_buffer_limit in which the largest
// single codec charge of one max_frame_size frame fits, so no stream can wait
// for codec memory that can never be granted:
//   - SSE downstream (egress) and SSE reading (local ingress) hold the frame,
//     its Base64 text and the event copies: 6 x frame + 16 KiB;
//   - WS downstream holds the frame copy and the encoded binary message:
//     2 x frame + the 56-byte header; WS readers and /up hold at most
//     frame + header, and a WS text control message 16 KiB.
//
// A node whose egress allows only WebSocket and that has no local ingress
// role (no listeners or outbound peers) needs only the WS figure. A
// bidirectional node (http.enabled with local listeners) needs room for one
// maximal egress charge plus the local ingress reserve (at least one local WS
// frame) so remote peers cannot starve local clients.
func (c *Config) minCodecBudget() int64 {
	frame := int64(c.Transport.MaxFrameSize)
	need := max(2*frame+wsBinaryHeaderLen, 16<<10)
	sse := len(c.Listeners) > 0 || len(c.Peers) > 0 || !c.HTTP.Enabled
	for _, mode := range c.HTTP.AllowedTransports {
		sse = sse || mode == "http-sse"
	}
	if len(c.HTTP.AllowedTransports) == 0 {
		sse = true
	}
	if sse {
		need = max(need, 6*frame+16384)
	}
	if c.HTTP.Enabled && len(c.Listeners) > 0 {
		egress := max(2*frame+wsBinaryHeaderLen, int64(16<<10))
		egressSSE := len(c.HTTP.AllowedTransports) == 0
		for _, mode := range c.HTTP.AllowedTransports {
			egressSSE = egressSSE || mode == "http-sse"
		}
		if egressSSE {
			egress = max(egress, 6*frame+16384)
		}
		need = max(need, egress+frame+wsBinaryHeaderLen)
	}
	return need
}

// wsBinaryHeaderLen is the fixed DBBridge WebSocket binary frame header.
const wsBinaryHeaderLen = 56
