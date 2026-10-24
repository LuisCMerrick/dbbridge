package tunnel

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// Transports a tunnel can run over, in the order metrics list them.
var metricTransports = []string{modeWS, modeSSE, modePoll}

func (h *Hub) countRejected(code string) {
	if code == "" {
		code = "other"
	}
	h.statsMu.Lock()
	if h.rejectedBy == nil {
		h.rejectedBy = map[string]int64{}
	}
	h.rejectedBy[code]++
	h.statsMu.Unlock()
}

// Ready reports whether every reverse peer of the hub has at least one live
// tunnel, and names the peers that have none.
func (h *Hub) Ready() (bool, []string) {
	var missing []string
	for _, id := range h.peerIDsLocked() {
		if h.Tunnels(id) == 0 {
			missing = append(missing, id)
		}
	}
	return len(missing) == 0, missing
}

func (h *Hub) peerIDsLocked() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.peerIDs()
}

// WriteMetrics renders the public side's tunnel metric families.
func (h *Hub) WriteMetrics(w io.Writer) {
	type key struct{ peer, transport string }
	counts := map[key]int{}
	h.mu.Lock()
	ids := h.peerIDs()
	for _, id := range ids {
		for _, t := range h.peers[id].tunnels {
			if !t.sess.IsClosed() {
				counts[key{id, t.Transport}]++
			}
		}
	}
	h.mu.Unlock()
	fmt.Fprintf(w, "# HELP dbb_reverse_hub_tunnels Live reverse tunnels accepted from each reverse peer, by transport\n# TYPE dbb_reverse_hub_tunnels gauge\n")
	for _, id := range ids {
		for _, tr := range metricTransports {
			fmt.Fprintf(w, "dbb_reverse_hub_tunnels{peer=%q,transport=%q} %d\n", id, tr, counts[key{id, tr}])
		}
	}
	fmt.Fprintf(w, "# HELP dbb_reverse_hub_accepted_total Reverse tunnels accepted and registered\n# TYPE dbb_reverse_hub_accepted_total counter\n")
	fmt.Fprintf(w, "dbb_reverse_hub_accepted_total %d\n", h.accepted.Load())
	h.statsMu.Lock()
	codes := make([]string, 0, len(h.rejectedBy))
	for c := range h.rejectedBy {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	fmt.Fprintf(w, "# HELP dbb_reverse_hub_rejected_total Reverse tunnel handshakes refused, by DBBridge error code\n# TYPE dbb_reverse_hub_rejected_total counter\n")
	for _, c := range codes {
		fmt.Fprintf(w, "dbb_reverse_hub_rejected_total{code=%q} %d\n", c, h.rejectedBy[c])
	}
	h.statsMu.Unlock()
}

// WriteMetrics renders the database side's tunnel metric families.
func (c *Client) WriteMetrics(w io.Writer) {
	type key struct{ endpoint, transport string }
	counts := map[key]int{}
	c.mu.Lock()
	for _, t := range c.live {
		counts[key{t.endpoint, t.transport}]++
	}
	fb := make(map[string]int64, len(c.fallbackBy))
	for k, v := range c.fallbackBy {
		fb[k] = v
	}
	c.mu.Unlock()
	fmt.Fprintf(w, "# HELP dbb_reverse_tunnels Live reverse tunnels dialed by this node, by endpoint and transport\n# TYPE dbb_reverse_tunnels gauge\n")
	for _, ep := range c.cfg.Reverse.Endpoints {
		for _, tr := range metricTransports {
			fmt.Fprintf(w, "dbb_reverse_tunnels{endpoint=%q,transport=%q} %d\n", ep.ID, tr, counts[key{ep.ID, tr}])
		}
	}
	fmt.Fprintf(w, "# HELP dbb_reverse_tunnel_established_total Reverse tunnels established\n# TYPE dbb_reverse_tunnel_established_total counter\n")
	fmt.Fprintf(w, "dbb_reverse_tunnel_established_total %d\n", c.established.Load())
	fmt.Fprintf(w, "# HELP dbb_reverse_tunnel_lost_total Established reverse tunnels that ended while the node was running\n# TYPE dbb_reverse_tunnel_lost_total counter\n")
	fmt.Fprintf(w, "dbb_reverse_tunnel_lost_total %d\n", c.lost.Load())
	fmt.Fprintf(w, "# HELP dbb_reverse_tunnel_dial_failures_total Failed reverse tunnel dials (after any transport fallback)\n# TYPE dbb_reverse_tunnel_dial_failures_total counter\n")
	fmt.Fprintf(w, "dbb_reverse_tunnel_dial_failures_total %d\n", c.dialFailures.Load())
	keys := make([]string, 0, len(fb))
	for k := range fb {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Fprintf(w, "# HELP dbb_reverse_tunnel_fallback_total Transport fallbacks of reverse.transport auto\n# TYPE dbb_reverse_tunnel_fallback_total counter\n")
	for _, k := range keys {
		from, to, _ := strings.Cut(k, ">")
		fmt.Fprintf(w, "dbb_reverse_tunnel_fallback_total{from=%q,to=%q} %d\n", from, to, fb[k])
	}
}
