package protocol

// Flow limits: an egress advertises its per-session flow-control limits in
// every create response (SessionLimits.UpReceiptLimit, DownMaxFrames) and may
// set ack_requested on ack_up when an explicit control ACK would unblock the
// session. A create request never carries these fields.

// MaxAdvertisedFrames bounds UpReceiptLimit and DownMaxFrames. A sender
// advertises at most this value; a receiver treats a larger advertisement as
// exactly this value (ClampAdvertisedFrames). Assuming fewer frames than the
// peer really accepts is always safe, so neither side fails a session over it.
const MaxAdvertisedFrames = 1 << 20

// ClampAdvertisedFrames limits an advertised frame count to
// MaxAdvertisedFrames. Negative values are returned unchanged so validation
// can reject them.
func ClampAdvertisedFrames(n int) int { return min(n, MaxAdvertisedFrames) }
