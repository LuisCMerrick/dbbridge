package transport

import (
	"context"
	"dbbridge/internal/protocol"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"
)

func (c *TripleTransportClient) Probe(ctx context.Context, node, nonce string) (*protocol.ProbeResponse, error) {
	var q protocol.ProbeResponse
	e := c.request(ctx, c.controlClient, "POST", "/v1/peer/probe", jsonBody(protocol.ProbeRequest{ProtocolVersion: protocol.ProtocolVersionString, ExpectedNodeID: node, Nonce: nonce}), "", "", "", 0, nil, &q, 200)
	if e != nil {
		return nil, e
	}
	seen := map[string]bool{}
	valid := q.ProtocolVersion == protocol.ProtocolVersionString && q.NodeID == node && q.InstanceID != "" && q.Nonce == nonce && len(q.Transports) > 0
	for _, m := range q.Transports {
		if m != "ws" && m != "http-sse" || seen[m] {
			valid = false
		}
		seen[m] = true
	}
	if !valid {
		return nil, fmt.Errorf("%w: probe identity/capability", ErrProtocol)
	}
	return &q, nil
}
func (c *TripleTransportClient) CreateSession(ctx context.Context, target string, limits protocol.SessionLimits, id string, recovery protocol.RecoveryNegotiation, inst, mode string) (*protocol.CreateSessionResponse, error) {
	q := protocol.CreateSessionRequest{ProtocolVersion: protocol.ProtocolVersionString, TargetID: target, CreateRequestID: id, IngressInstanceID: c.instanceID, Transport: protocol.TransportMode{Mode: mode}, Recovery: recovery, Limits: limits}
	var r protocol.CreateSessionResponse
	// Offer the flow-limits extension; an egress that does not know it
	// ignores the header and answers plain 1.4.
	h := http.Header{protocol.HeaderFeatures: {protocol.FeatureFlowLimits}}
	e := c.request(ctx, c.controlClient, "POST", "/v1/sessions", jsonBody(q), "", "", inst, 0, h, &r, 200, 201)
	if e != nil {
		return nil, e
	}
	if err := c.validateCreateResponse(&r, limits, recovery, inst, mode); err != nil {
		return nil, err
	}
	return &r, nil
}

// validateCreateResponse checks a create response against the request; each
// failure names the offending field to ease troubleshooting.
func (c *TripleTransportClient) validateCreateResponse(r *protocol.CreateSessionResponse, limits protocol.SessionLimits, recovery protocol.RecoveryNegotiation, inst, mode string) error {
	bad := func(what string) error { return fmt.Errorf("%w: create response: %s", ErrProtocol, what) }
	// An egress configured beyond the advertised range is used as if it
	// advertised the range maximum.
	r.Limits.UpReceiptLimit = protocol.ClampAdvertisedFrames(r.Limits.UpReceiptLimit)
	r.Limits.DownMaxFrames = protocol.ClampAdvertisedFrames(r.Limits.DownMaxFrames)
	switch {
	case r.ProtocolVersion != protocol.ProtocolVersionString:
		return bad("protocol_version")
	case r.InstanceID != inst:
		return bad("instance_id")
	case !validSessionID(r.SessionID):
		return bad("session_id")
	case r.SessionToken == "":
		return bad("session_token")
	case r.Epoch != "1":
		return bad("epoch")
	case r.Transport.Mode != mode:
		return bad("transport")
	case r.Recovery.Enabled != recovery.Enabled || r.Recovery.GraceMs < 0,
		recovery.Enabled && (r.Recovery.GraceMs <= 0 || r.Recovery.GraceMs > recovery.GraceMs),
		!recovery.Enabled && r.Recovery.GraceMs != 0:
		return bad("recovery")
	case r.Limits.MaxFrameSize <= 0 || r.Limits.MaxFrameSize > limits.MaxFrameSize:
		return bad("limits.max_frame_size")
	case r.Limits.UpMaxInflightFrames <= 0 || r.Limits.UpMaxInflightFrames > limits.UpMaxInflightFrames:
		return bad("limits.up_max_inflight_frames")
	case r.Limits.UpWindowBytes <= 0 || r.Limits.UpWindowBytes > limits.UpWindowBytes:
		return bad("limits.up_window_bytes")
	case r.Limits.DownBufferBytes <= 0 || r.Limits.DownBufferBytes > limits.DownBufferBytes:
		return bad("limits.down_buffer_bytes")
	case r.Limits.UpReceiptLimit < 0 || r.Limits.UpReceiptLimit != 0 && r.Limits.UpReceiptLimit <= r.Limits.UpMaxInflightFrames:
		// Below inflight+1 the peer would refuse frames the negotiated
		// window allows.
		return bad("limits.up_receipt_limit")
	case r.Limits.DownMaxFrames < 0:
		return bad("limits.down_max_frames")
	case r.UnattachedTimeoutMs <= 0:
		return bad("unattached_timeout_ms")
	case c.expectedNodeID != "" && r.NodeID != c.expectedNodeID:
		return bad("node_id")
	}
	return nil
}

func (c *TripleTransportClient) SendAck(ctx context.Context, sid, token, inst string, ep uint64, q protocol.AckRequest) (*protocol.AckResponse, error) {
	b, e := c.getBinding(sid, ep)
	if e != nil {
		return nil, e
	}
	q.StreamID = b.id
	var r protocol.AckResponse
	e = c.request(ctx, c.controlClient, "POST", sessionPath(sid, "/ack", ep, true), jsonBody(q), sid, token, inst, ep, nil, &r, 200)
	if e == nil {
		e = protocol.ValidateBinding(r.SessionID, r.InstanceID, r.Epoch, r.StreamID, r.Transport, sid, inst, ep, b.id, b.mode)
		if r.ProbeID != q.ProbeID {
			e = ErrProtocol
		}
	}
	return &r, e
}
func (c *TripleTransportClient) ResumeSession(ctx context.Context, sid, token, inst string, q protocol.ResumeRequest) (*protocol.ResumeResponse, error) {
	var r protocol.ResumeResponse
	e := c.request(ctx, c.controlClient, "POST", sessionPath(sid, "/resume", 0, false), jsonBody(q), "", token, inst, 0, nil, &r, 200)
	if e != nil {
		return nil, e
	}
	ep, pe := protocol.Offset(q.ExpectedEpoch)
	newEp, ne := protocol.Offset(r.Epoch)
	ack, ae := protocol.Offset(r.AckUpOffset)
	next, _ := protocol.Offset(q.UpNextOffset)
	known, _ := protocol.Offset(q.KnownUpAckOffset)
	if pe != nil || ne != nil || ae != nil || ep == math.MaxUint64 || newEp != ep+1 || r.SessionID != sid || r.InstanceID != inst || r.ResumeRequestID != q.ResumeRequestID || r.Transport.Mode != q.Transport.Mode || r.DownReplayFrom != q.DownDeliveredOffset || ack < known || ack > next || r.UpWrittenOffset != r.AckUpOffset || r.ResumeRemainingMs <= 0 || r.ResumeRemainingMs > math.MaxInt64/int64(time.Millisecond) {
		return nil, fmt.Errorf("%w: resume response", ErrProtocol)
	}
	down, de := protocol.Offset(q.DownDeliveredOffset)
	produced, pe2 := protocol.Offset(r.DownProducedOffset)
	retained, re := protocol.Offset(r.DownRetainedFrom)
	if de != nil || pe2 != nil || re != nil || retained > down || down > produced {
		return nil, ErrProtocol
	}
	if q.UpFIN.Seen {
		if !r.UpFIN.Received || q.UpFIN.FinalOffset == nil || r.UpFIN.FinalOffset == nil || *q.UpFIN.FinalOffset != *r.UpFIN.FinalOffset {
			return nil, ErrProtocol
		}
	}
	if q.UpFIN.Applied && !r.UpFIN.Applied || q.DownFIN.Applied && !r.DownFIN.Acked {
		return nil, ErrProtocol
	}
	if r.DownFIN.Seen && (r.DownFIN.FinalOffset == nil || *r.DownFIN.FinalOffset != r.DownProducedOffset) {
		return nil, ErrProtocol
	}
	return &r, nil
}
func (c *TripleTransportClient) SendFin(ctx context.Context, sid, token, inst string, ep, off uint64) (*protocol.FinResponse, error) {
	b, e := c.getBinding(sid, ep)
	if e != nil {
		return nil, e
	}
	var r protocol.FinResponse
	e = c.request(ctx, c.controlClient, "POST", sessionPath(sid, "/fin", ep, true), jsonBody(protocol.FinRequest{Direction: "up", FinalOffset: strconv.FormatUint(off, 10)}), sid, token, inst, ep, nil, &r, 200)
	if e == nil {
		e = protocol.ValidateBinding(r.SessionID, r.InstanceID, r.Epoch, r.StreamID, r.Transport, sid, inst, ep, b.id, b.mode)
	}
	return &r, e
}
func (c *TripleTransportClient) DeleteSession(ctx context.Context, sid, token, inst string, ep uint64) error {
	var r protocol.DeleteSessionResponse
	err := c.request(ctx, c.controlClient, "DELETE", sessionPath(sid, "", ep, true), nil, "", token, inst, ep, nil, &r, 200)
	if err == nil && (!r.Closed || r.SessionID != sid || r.InstanceID != inst || r.Epoch != strconv.FormatUint(ep, 10)) {
		err = fmt.Errorf("%w: delete response binding", ErrProtocol)
	}
	return err
}
