package protocol

import (
	"net/http"
	"strings"
)

// HeaderFeatures lists optional protocol extensions the sender understands,
// comma separated. It is a header rather than a JSON field because every
// protocol body is decoded strictly (unknown fields are rejected), while
// peers of any version ignore headers they do not know. An extension may
// add JSON fields to a message only after the receiver offered it.
const HeaderFeatures = "X-DBB-Features"

// FeatureFlowLimits: offered by ingress on POST /v1/sessions. Egress that
// understands it advertises its per-session flow-control limits in the
// create response (SessionLimits.UpReceiptLimit, DownMaxFrames) and may set
// ack_requested on ack_up when an explicit control ACK would unblock the
// session. Without it both sides behave exactly as plain 1.4.
const FeatureFlowLimits = "flow-limits"

// MaxAdvertisedFrames bounds UpReceiptLimit and DownMaxFrames. A sender
// advertises at most this value; a receiver treats a larger advertisement as
// exactly this value (ClampAdvertisedFrames). Assuming fewer frames than the
// peer really accepts is always safe, so neither side fails a session over it.
const MaxAdvertisedFrames = 1 << 20

// ClampAdvertisedFrames limits an advertised frame count to
// MaxAdvertisedFrames. Negative values are returned unchanged so validation
// can reject them.
func ClampAdvertisedFrames(n int) int { return min(n, MaxAdvertisedFrames) }

// FlowLimitsNegotiated reports whether a create response's limits carry the
// FeatureFlowLimits fields, i.e. whether the session negotiated the
// extension. Only such sessions may receive ack_requested on ack_up; a
// receiver ignores the hint on any other session.
func (l SessionLimits) FlowLimitsNegotiated() bool {
	return l.UpReceiptLimit > 0 || l.DownMaxFrames > 0
}

// HasFeature reports whether h offers feature in HeaderFeatures.
func HasFeature(h http.Header, feature string) bool {
	for _, v := range h.Values(HeaderFeatures) {
		for f := range strings.SplitSeq(v, ",") {
			if strings.EqualFold(strings.TrimSpace(f), feature) {
				return true
			}
		}
	}
	return false
}
