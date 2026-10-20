package egress

import (
	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
)

// Codec memory is charged per transport for what a frame really holds while
// it is encoded and written.

// wsDownCost is one downstream WS frame: the copy read from the session
// buffer plus the encoded binary message (header + payload).
func wsDownCost(size int) int64 { return 2*int64(size) + protocol.WSBinaryHeaderLen }

// sseDownCost is one downstream SSE frame: the copy read from the session
// buffer, its Base64 text, the formatted data line and the event string.
func sseDownCost(size int) int64 { return 6*int64(size) + 16384 }

// wsTextCost is the reservation of one inbound WS text message.
const wsTextCost = 16 << 10

func configuredMaxFrame(c *config.Config) int {
	if c.Transport.MaxFrameSize > 0 {
		return c.Transport.MaxFrameSize
	}
	return 256 << 10
}

// maxCodecCharge is the largest single codec charge the egress makes for one
// frame with the configured transports and max_frame_size.
func maxCodecCharge(c *config.Config) int64 {
	n := configuredMaxFrame(c)
	charge := max(wsDownCost(n), int64(wsTextCost))
	modes := c.HTTP.AllowedTransports
	if len(modes) == 0 {
		modes = []string{"ws", "http-sse"}
	}
	for _, m := range modes {
		if m == "http-sse" {
			charge = max(charge, sseDownCost(n))
		}
	}
	return charge
}

// applyIngressCodecReserve keeps an eighth of Codec (at least one WS frame of
// the local ingress) for the local ingress role of a bidirectional node, so
// remote peers cannot stall local clients' encode and WS reads. The reserve
// never shrinks the egress part below one maximal egress charge. Nodes
// without local listeners reserve nothing; a reserve already set is kept.
func applyIngressCodecReserve(c *config.Config) {
	res := c.Resources
	if res == nil || len(c.Listeners) == 0 || res.IngressCodecReserve() != 0 {
		return
	}
	limit := res.Codec.Limit()
	floor := max(limit/8, int64(configuredMaxFrame(c))+protocol.WSBinaryHeaderLen)
	floor = min(floor, limit-maxCodecCharge(c))
	if floor > 0 {
		res.SetIngressCodecReserve(floor)
	}
}

// peerCodecLimit is one peer's part of the egress Codec: its fair share
// among the enabled inbound peers, but never less than one maximal charge so
// every stream can make progress.
func (s *Server) peerCodecLimit(peer string) int64 {
	egress := s.cfg.Resources.EgressCodecLimit()
	if s.auth == nil {
		return egress
	}
	return min(max(s.auth.PeerShareOf(peer, egress), maxCodecCharge(s.cfg)), egress)
}

// codecFor returns peer's share of the egress Codec. Every egress codec
// charge (WS reader, /up body, downstream encode) is taken from it, so one
// peer cannot hold the codec memory every other peer's data plane and the
// local ingress need.
func (s *Server) codecFor(peer string) *resource.Budget {
	if b, ok := s.peerCodec.Load(peer); ok {
		return b.(*resource.Budget)
	}
	b, _ := s.peerCodec.LoadOrStore(peer, s.cfg.Resources.EgressCodecShare(s.peerCodecLimit(peer)))
	return b.(*resource.Budget)
}

// resizePeerCodec applies the current number of enabled peers to every
// peer's codec share after a credentials reload.
func (s *Server) resizePeerCodec() {
	s.peerCodec.Range(func(k, v any) bool {
		v.(*resource.Budget).SetLimit(s.peerCodecLimit(k.(string)))
		return true
	})
}
