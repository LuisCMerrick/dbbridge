package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

// Header definitions for DBBridge specification
const (
	HeaderPeerAuth        = "Authorization"
	HeaderSessionToken    = "X-DBB-Session-Token"
	HeaderInstanceID      = "X-DBB-Instance-ID"
	HeaderOffset          = "X-DBB-Offset"
	HeaderLength          = "X-DBB-Length"
	HeaderSHA256          = "X-DBB-SHA256"
	HeaderAckDownOffset   = "X-DBB-Ack-Down-Offset"
	HeaderConfirmUpOffset = "X-DBB-Confirm-Up-Offset"
	HeaderStreamID        = "X-DBB-Stream-ID"
	HeaderWSPurpose       = "X-DBB-WS-Purpose" // "check" or "session"
	HeaderWSNonce         = "X-DBB-Nonce"
	HeaderWSSessionID     = "X-DBB-Session-ID"
	HeaderWSEpoch         = "X-DBB-Epoch"
)

// Subprotocol for WebSocket
const SecWebSocketProtocol = "dbbridge.v1.4"

// ProtocolVersionString is the mandatory version string for v1.4
const ProtocolVersionString = "1.4"

// Supported transport modes
const (
	TransportModeWS       = "ws"
	TransportModeSSE      = "http-sse"
	TransportStrategyAuto = "auto"
)

// Standard Error Codes for protocol 1.4
const (
	ErrBadFrame             = "DBB_BAD_FRAME"
	ErrBadDigest            = "DBB_BAD_DIGEST"
	ErrBadAck               = "DBB_BAD_ACK"
	ErrUnauthorized         = "DBB_UNAUTHORIZED"
	ErrTargetForbidden      = "DBB_TARGET_FORBIDDEN"
	ErrSessionNotFound      = "DBB_SESSION_NOT_FOUND"
	ErrStaleEpoch           = "DBB_STALE_EPOCH"
	ErrStreamExists         = "DBB_STREAM_EXISTS"
	ErrIdempotencyConflict  = "DBB_IDEMPOTENCY_CONFLICT"
	ErrResumeInProgress     = "DBB_RESUME_IN_PROGRESS"
	ErrInstanceMismatch     = "DBB_INSTANCE_MISMATCH"
	ErrNodeMismatch         = "DBB_NODE_MISMATCH"
	ErrProtocolMismatch     = "DBB_PROTOCOL_MISMATCH"
	ErrRecoveryDisabled     = "DBB_RECOVERY_DISABLED"
	ErrRecoveryUnsupported  = "DBB_RECOVERY_UNSUPPORTED"
	ErrOffsetGap            = "DBB_OFFSET_GAP"
	ErrFrameConflict        = "DBB_FRAME_CONFLICT"
	ErrReplayTooOld         = "DBB_REPLAY_TOO_OLD"
	ErrWindowExceeded       = "DBB_WINDOW_EXCEEDED"
	ErrSessionClosed        = "DBB_SESSION_CLOSED"
	ErrBackendLost          = "DBB_BACKEND_LOST"
	ErrResumeExpired        = "DBB_RESUME_EXPIRED"
	ErrFrameTooLarge        = "DBB_FRAME_TOO_LARGE"
	ErrQuotaExceeded        = "DBB_QUOTA_EXCEEDED"
	ErrDialFailed           = "DBB_DIAL_FAILED"
	ErrBufferExhausted      = "DBB_BUFFER_EXHAUSTED"
	ErrDraining             = "DBB_DRAINING"
	ErrCreatePending        = "DBB_CREATE_PENDING"
	ErrResumePending        = "DBB_RESUME_PENDING"
	ErrInternal             = "DBB_INTERNAL_ERROR"
	ErrTransportUnsupported = "DBB_TRANSPORT_UNSUPPORTED"
	ErrTransportMismatch    = "DBB_TRANSPORT_MISMATCH"
	ErrWSHandshakeFailed    = "DBB_WS_HANDSHAKE_FAILED"
	ErrWSProtocolError      = "DBB_WS_PROTOCOL_ERROR"
)

type ErrorResponse struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	Retryable     bool   `json:"retryable"`
	SessionClosed bool   `json:"session_closed"`
}

type TransportMode struct {
	Mode string `json:"mode"`
}

type SessionLimits struct {
	MaxFrameSize        int   `json:"max_frame_size"`
	UpMaxInflightFrames int   `json:"up_max_inflight_frames"`
	UpWindowBytes       int64 `json:"up_window_bytes"`
	DownBufferBytes     int64 `json:"down_buffer_bytes"`
	// Advertised by egress only when the create request offered
	// FeatureFlowLimits (see features.go); zero and omitted otherwise.
	// A create request never carries them (DecodeJSON rejects them there).
	UpReceiptLimit int `json:"up_receipt_limit,omitempty"`
	DownMaxFrames  int `json:"down_max_frames,omitempty"`
}

type RecoveryNegotiation struct {
	Enabled bool  `json:"enabled"`
	GraceMs int64 `json:"grace_ms"`
}

// ProbeRequest for POST /v1/peer/probe
type ProbeRequest struct {
	ProtocolVersion string `json:"protocol_version"`
	ExpectedNodeID  string `json:"expected_node_id"`
	Nonce           string `json:"nonce"`
}

// ProbeResponse for POST /v1/peer/probe
type ProbeResponse struct {
	ProtocolVersion      string   `json:"protocol_version"`
	NodeID               string   `json:"node_id"`
	InstanceID           string   `json:"instance_id"`
	Nonce                string   `json:"nonce"`
	RecoveryAvailable    bool     `json:"recovery_available"`
	AcceptingNewSessions bool     `json:"accepting_new_sessions"`
	Transports           []string `json:"transports"`
}

type CreateSessionRequest struct {
	ProtocolVersion   string              `json:"protocol_version"`
	Transport         TransportMode       `json:"transport"`
	Recovery          RecoveryNegotiation `json:"recovery"`
	CreateRequestID   string              `json:"create_request_id"`
	IngressInstanceID string              `json:"ingress_instance_id"`
	TargetID          string              `json:"target_id"`
	Limits            SessionLimits       `json:"limits"`
}

type CreateSessionResponse struct {
	ProtocolVersion     string              `json:"protocol_version"`
	NodeID              string              `json:"node_id"`
	InstanceID          string              `json:"instance_id"`
	SessionID           string              `json:"session_id"`
	SessionToken        string              `json:"session_token"`
	Epoch               string              `json:"epoch"` // Decimal string
	Transport           TransportMode       `json:"transport"`
	Recovery            RecoveryNegotiation `json:"recovery"`
	Limits              SessionLimits       `json:"limits"`
	UnattachedTimeoutMs int64               `json:"unattached_timeout_ms"`
}

type UpResponse struct {
	SessionID      string         `json:"session_id"`
	InstanceID     string         `json:"instance_id"`
	Epoch          string         `json:"epoch"`
	FrameAccepted  bool           `json:"frame_accepted"`
	AckUpOffset    string         `json:"ack_up_offset"`    // Decimal string
	DownAckOffset  string         `json:"down_ack_offset"`  // Decimal string
	UpReceiveLimit string         `json:"up_receive_limit"` // Decimal string
	UpFinApplied   bool           `json:"up_fin_applied"`
	StreamID       string         `json:"stream_id"`
	Transport      *TransportMode `json:"transport"`
}

type AckRequest struct {
	StreamID            string `json:"stream_id,omitempty"`
	ReadyReceived       bool   `json:"ready_received,omitempty"`
	DownDeliveredOffset string `json:"down_delivered_offset"` // Decimal string
	DownFinApplied      bool   `json:"down_fin_applied"`
	UpConfirmedOffset   string `json:"up_confirmed_offset"` // Decimal string
	HeartbeatNonce      string `json:"heartbeat_nonce,omitempty"`
	ProbeID             string `json:"probe_id,omitempty"`
	DataTxBlocked       bool   `json:"data_tx_blocked"`
	DataRxBlocked       bool   `json:"data_rx_blocked"`
	BackpressureReason  string `json:"backpressure_reason"`
}

type AckResponse struct {
	SessionID          string         `json:"session_id"`
	InstanceID         string         `json:"instance_id"`
	Epoch              string         `json:"epoch"`
	AckUpOffset        string         `json:"ack_up_offset"`
	UpFinApplied       bool           `json:"up_fin_applied"`
	DownAckOffset      string         `json:"down_ack_offset"`
	DownFinAcked       bool           `json:"down_fin_acked"`
	ProbeID            string         `json:"probe_id,omitempty"`
	StreamID           string         `json:"stream_id"`
	Transport          *TransportMode `json:"transport"`
	DataTxBlocked      bool           `json:"data_tx_blocked"`
	DataRxBlocked      bool           `json:"data_rx_blocked"`
	BackpressureReason string         `json:"backpressure_reason"`
}

type DirectionFIN struct {
	Seen        bool    `json:"seen"`
	FinalOffset *string `json:"final_offset,omitempty"`
	Applied     bool    `json:"applied,omitempty"`
	Acked       bool    `json:"acked,omitempty"`
}

type ResumeRequest struct {
	ResumeRequestID     string        `json:"resume_request_id"`
	ExpectedEpoch       string        `json:"expected_epoch"`
	IngressInstanceID   string        `json:"ingress_instance_id"`
	Transport           TransportMode `json:"transport"`
	KnownUpAckOffset    string        `json:"known_up_ack_offset"`
	UpNextOffset        string        `json:"up_next_offset"`
	DownDeliveredOffset string        `json:"down_delivered_offset"`
	UpFIN               DirectionFIN  `json:"up_fin"`
	DownFIN             DirectionFIN  `json:"down_fin"`
}

type ResumeResponse struct {
	SessionID          string        `json:"session_id"`
	InstanceID         string        `json:"instance_id"`
	ResumeRequestID    string        `json:"resume_request_id"`
	Epoch              string        `json:"epoch"`
	Transport          TransportMode `json:"transport"`
	UpWrittenOffset    string        `json:"up_written_offset"`
	AckUpOffset        string        `json:"ack_up_offset"`
	DownReplayFrom     string        `json:"down_replay_from"`
	DownProducedOffset string        `json:"down_produced_offset"`
	DownRetainedFrom   string        `json:"down_retained_from"`
	UpFIN              ResumeUpFIN   `json:"up_fin"`
	DownFIN            DirectionFIN  `json:"down_fin"`
	ResumeRemainingMs  int64         `json:"resume_remaining_ms"`
}

// The response names the Egress observation "received"; request FINs use "seen".
type ResumeUpFIN struct {
	Received    bool    `json:"received"`
	FinalOffset *string `json:"final_offset"`
	Applied     bool    `json:"applied"`
}

type DeleteSessionResponse struct {
	SessionID  string `json:"session_id"`
	InstanceID string `json:"instance_id"`
	Epoch      string `json:"epoch"`
	Closed     bool   `json:"closed"`
}

type FinRequest struct {
	Direction   string `json:"direction"`
	FinalOffset string `json:"final_offset"` // Decimal string
}

type FinResponse struct {
	SessionID    string         `json:"session_id"`
	InstanceID   string         `json:"instance_id"`
	Epoch        string         `json:"epoch"`
	UpFinApplied bool           `json:"up_fin_applied"`
	StreamID     string         `json:"stream_id"`
	Transport    *TransportMode `json:"transport"`
}

// WS Precheck Hello message
type WSHelloMessage struct {
	Type            string `json:"type"` // "hello"
	ProtocolVersion string `json:"protocol_version"`
	NodeID          string `json:"node_id"`
	InstanceID      string `json:"instance_id"`
	Nonce           string `json:"nonce"`
	Transport       string `json:"transport"` // "ws"
}

// Universal Ready event data for WS & SSE
type ReadyEventData struct {
	Type            string  `json:"type"` // "ready"
	ProtocolVersion string  `json:"protocol_version"`
	SessionID       string  `json:"session_id"`
	InstanceID      string  `json:"instance_id"`
	Epoch           string  `json:"epoch"`
	StreamID        string  `json:"stream_id"`
	Transport       string  `json:"transport"` // "ws" or "http-sse"
	ResumeRequestID *string `json:"resume_request_id"`
}

// WS Text Control messages
type WSTextControlMessage struct {
	Type            string  `json:"type"` // "ready", "ack_up", "ping", "fin", "error"
	ProtocolVersion string  `json:"protocol_version,omitempty"`
	SessionID       string  `json:"session_id"`
	InstanceID      string  `json:"instance_id"`
	Epoch           string  `json:"epoch"`
	StreamID        string  `json:"stream_id"`
	AckUpOffset     string  `json:"ack_up_offset,omitempty"`
	UpFinApplied    bool    `json:"up_fin_applied,omitempty"`
	Nonce           string  `json:"nonce,omitempty"`
	FinalOffset     string  `json:"final_offset,omitempty"`
	Code            string  `json:"code,omitempty"`
	Retryable       bool    `json:"retryable,omitempty"`
	SessionClosed   bool    `json:"session_closed,omitempty"`
	Transport       string  `json:"transport,omitempty"`
	ResumeRequestID *string `json:"resume_request_id,omitempty"`
	// AckRequested rides on ack_up only, and only for sessions that
	// negotiated FeatureFlowLimits.
	AckRequested bool `json:"ack_requested,omitempty"`
}

// WebSocket 56-byte Binary Header per Section 7.1:
// [0,1)   type, uint8, fixed 0x01 (DATA)
// [1,2)   flags, uint8, fixed 0
// [2,4)   reserved, uint16, fixed 0
// [4,12)  epoch, uint64
// [12,20) offset, uint64
// [20,24) length, uint32
// [24,56) sha256, 32 bytes (raw binary sha256)
const WSBinaryHeaderLen = 56
const WSDataType = uint8(0x01)

// MaxFramePayload is the largest DATA payload a WS or SSE frame may carry.
const MaxFramePayload = 1024 * 1024

// EncodeWSFrameChecked encodes a DATA frame after checking it is one that
// DecodeWSFrame accepts: nonzero epoch, 1..MaxFramePayload bytes and an end
// offset that does not overflow.
func EncodeWSFrameChecked(epoch, offset uint64, payload []byte) ([]byte, error) {
	if epoch == 0 || len(payload) == 0 || len(payload) > MaxFramePayload || offset > math.MaxUint64-uint64(len(payload)) {
		return nil, errors.New("invalid frame range")
	}
	return EncodeWSFrame(epoch, offset, payload), nil
}

// EncodeWSFrame encodes a DATA frame without range validation (tests use it to
// build malformed frames). Callers must keep len(payload) <= MaxFramePayload;
// a larger payload would not fit the protocol's length field, so it panics
// instead of emitting a frame with a truncated length.
func EncodeWSFrame(epoch, offset uint64, payload []byte) []byte {
	length := len(payload)
	if length > MaxFramePayload {
		panic(fmt.Sprintf("protocol: ws frame payload %d exceeds %d bytes", length, MaxFramePayload))
	}
	sum := sha256.Sum256(payload)

	msg := make([]byte, WSBinaryHeaderLen+length)
	msg[0] = WSDataType
	msg[1] = 0                              // flags
	binary.BigEndian.PutUint16(msg[2:4], 0) // reserved
	binary.BigEndian.PutUint64(msg[4:12], epoch)
	binary.BigEndian.PutUint64(msg[12:20], offset)
	binary.BigEndian.PutUint32(msg[20:24], uint32(length))
	copy(msg[24:56], sum[:])
	copy(msg[56:], payload)
	return msg
}

func DecodeWSFrame(msg []byte) (epoch, offset uint64, payload []byte, err error) {
	if len(msg) < WSBinaryHeaderLen {
		return 0, 0, nil, errors.New("ws frame smaller than 56-byte header")
	}
	if msg[0] != WSDataType {
		return 0, 0, nil, fmt.Errorf("invalid ws frame type: 0x%02x", msg[0])
	}
	if binary.BigEndian.Uint16(msg[2:4]) != 0 {
		return 0, 0, nil, errors.New("nonzero reserved bytes")
	}
	if msg[1] != 0 {
		return 0, 0, nil, fmt.Errorf("invalid ws frame flags: 0x%02x", msg[1])
	}
	epoch = binary.BigEndian.Uint64(msg[4:12])
	offset = binary.BigEndian.Uint64(msg[12:20])
	length := binary.BigEndian.Uint32(msg[20:24])

	if epoch == 0 || length == 0 || length > MaxFramePayload || offset > math.MaxUint64-uint64(length) {
		return 0, 0, nil, errors.New("invalid frame range")
	}
	if int(length) != len(msg)-WSBinaryHeaderLen {
		return 0, 0, nil, fmt.Errorf("ws frame length mismatch: header specifies %d, body has %d", length, len(msg)-WSBinaryHeaderLen)
	}

	payload = msg[WSBinaryHeaderLen:]
	expectedSum := msg[24:56]
	actualSum := sha256.Sum256(payload)
	for i := 0; i < 32; i++ {
		if expectedSum[i] != actualSum[i] {
			return 0, 0, nil, errors.New("ws frame sha256 mismatch")
		}
	}

	return epoch, offset, payload, nil
}

// FormatSSEData formats raw bytes into compact format: <offset>:<length>:<sha256>:<base64_payload>
func FormatSSEData(offset uint64, payload []byte) string {
	length := len(payload)
	sum := sha256.Sum256(payload)
	sumHex := hex.EncodeToString(sum[:])
	b64 := base64.StdEncoding.EncodeToString(payload)
	return fmt.Sprintf("%d:%d:%s:%s", offset, length, sumHex, b64)
}

// FormatSSEDataChecked is FormatSSEData with the range checks ParseSSEData
// applies on the receiving side.
func FormatSSEDataChecked(offset uint64, payload []byte) (string, error) {
	if len(payload) == 0 || len(payload) > MaxFramePayload || offset > math.MaxUint64-uint64(len(payload)) {
		return "", errors.New("invalid frame range")
	}
	return FormatSSEData(offset, payload), nil
}

// ParseSSEData parses <offset>:<length>:<sha256>:<base64_payload>
func ParseSSEData(line string) (offset uint64, payload []byte, err error) {
	parts := strings.SplitN(line, ":", 4)
	if len(parts) != 4 {
		return 0, nil, errors.New("invalid sse data format: expected 4 segments")
	}
	// Offset rejects signs, leading zeros and empty fields, like every other
	// offset on the wire.
	offset, err = Offset(parts[0])
	if err != nil {
		return 0, nil, fmt.Errorf("invalid offset: %w", err)
	}
	length, err := Offset(parts[1])
	if err != nil || length > MaxFramePayload {
		return 0, nil, errors.New("invalid length")
	}
	expectedLen := int(length)
	expectedSHA := parts[2]
	if expectedLen <= 0 || offset > math.MaxUint64-uint64(expectedLen) || len(expectedSHA) != 64 || strings.ToLower(expectedSHA) != expectedSHA {
		return 0, nil, errors.New("invalid SSE frame range or digest")
	}
	if len(parts[3]) != base64.StdEncoding.EncodedLen(expectedLen) {
		return 0, nil, errors.New("invalid base64 payload length")
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(parts[3])
	if err != nil {
		return 0, nil, fmt.Errorf("invalid base64 payload: %w", err)
	}
	if len(decoded) != expectedLen {
		return 0, nil, fmt.Errorf("payload length mismatch: got %d, expected %d", len(decoded), expectedLen)
	}
	sum := sha256.Sum256(decoded)
	actualSHA := hex.EncodeToString(sum[:])
	if actualSHA != expectedSHA {
		return 0, nil, fmt.Errorf("payload sha256 mismatch: got %s, expected %s", actualSHA, expectedSHA)
	}
	return offset, decoded, nil
}

// A control union has fields whose presence depends on its discriminator.
func controlFields(kind string) []string {
	switch kind {
	case "ready":
		return []string{"protocol_version", "transport", "resume_request_id"}
	case "ack_up":
		return []string{"ack_up_offset", "up_fin_applied"}
	case "ping":
		return []string{"nonce"}
	case "fin":
		return []string{"final_offset"}
	case "error":
		return []string{"code", "retryable", "session_closed"}
	}
	return nil
}

// controlUnionFields are the WSTextControlMessage fields whose presence is
// decided by the type discriminator (see controlFields).
var controlUnionFields = map[string]bool{
	"protocol_version": true, "transport": true, "resume_request_id": true,
	"ack_up_offset": true, "up_fin_applied": true, "nonce": true,
	"final_offset": true, "code": true, "retryable": true, "session_closed": true,
}

// MarshalJSON emits the common fields plus exactly the fields the message's
// type requires (present even when zero), in a single encoding pass. A type
// that has no field set (not a valid control type) keeps the plain
// omitempty encoding.
func (m WSTextControlMessage) MarshalJSON() ([]byte, error) {
	fields := controlFields(m.Type)
	if fields == nil {
		type plain WSTextControlMessage
		return json.Marshal(plain(m))
	}
	values := map[string]any{
		"protocol_version": m.ProtocolVersion, "transport": m.Transport,
		"resume_request_id": m.ResumeRequestID, "ack_up_offset": m.AckUpOffset,
		"up_fin_applied": m.UpFinApplied, "nonce": m.Nonce,
		"final_offset": m.FinalOffset, "code": m.Code,
		"retryable": m.Retryable, "session_closed": m.SessionClosed,
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	put := func(name string, v any) error {
		if buf.Len() == 0 {
			buf.WriteByte('{')
		} else {
			buf.WriteByte(',')
		}
		k, _ := json.Marshal(name)
		buf.Write(k)
		buf.WriteByte(':')
		if err := enc.Encode(v); err != nil {
			return err
		}
		buf.Truncate(buf.Len() - 1) // Encode appends a newline
		return nil
	}
	for _, kv := range []struct {
		k string
		v any
	}{{"type", m.Type}, {"session_id", m.SessionID}, {"instance_id", m.InstanceID}, {"epoch", m.Epoch}, {"stream_id", m.StreamID}} {
		if err := put(kv.k, kv.v); err != nil {
			return nil, err
		}
	}
	for _, name := range fields {
		if err := put(name, values[name]); err != nil {
			return nil, err
		}
	}
	if m.AckRequested && m.Type == "ack_up" {
		if err := put("ack_requested", true); err != nil {
			return nil, err
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}
