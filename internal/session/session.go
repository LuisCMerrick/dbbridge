package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
	"dbbridge/internal/resource"
	"dbbridge/internal/stream"
)

type State string

const (
	StateCreating  State = "CREATING"
	StateAttaching State = "ATTACHING"
	StateActive    State = "ACTIVE"
	StateDraining  State = "DRAINING"
	StateSuspended State = "SUSPENDED"
	StateResuming  State = "RESUMING"
	StateClosed    State = "CLOSED"
)

var (
	ErrSessionClosed       = errors.New("session is closed")
	ErrStaleEpoch          = errors.New("stale epoch")
	ErrInvalidToken        = errors.New("invalid resume token")
	ErrResumeExpired       = errors.New("session resume deadline exceeded")
	ErrBufferExhausted     = errors.New("session buffer exhausted or pruned")
	ErrResumeDisabled      = errors.New("session resume is disabled")
	ErrStreamExists        = errors.New("stream already exists in epoch")
	ErrFrameConflict       = errors.New("frame conflict or overlap detected")
	ErrReplayTooOld        = errors.New("replay frame is too old (receipt cleared)")
	ErrWindowExceeded      = errors.New("frame exceeds receive window")
	ErrIdempotencyConflict = errors.New("idempotency conflict")
	ErrBadAck              = errors.New("bad ack offset")
	ErrQuotaExceeded       = errors.New("quota exceeded")
	ErrResumeInProgress    = errors.New("resume in progress")
	ErrResumePending       = errors.New("resume pending")
	ErrNotActive           = errors.New("data plane not active")
	ErrTransportMismatch   = errors.New("transport mismatch")
	// ErrFrameTooLarge reports an upstream data frame longer than the
	// negotiated max_frame_size. It is distinct from ErrFrameConflict so the
	// transport can answer DBB_FRAME_TOO_LARGE.
	ErrFrameTooLarge = errors.New("frame exceeds negotiated max_frame_size")
)

// InflightUpFrame represents a verified upstream frame in receive queue.
type InflightUpFrame struct {
	Offset uint64
	Length int
	SHA256 string
	Data   []byte
}

type downFrame struct{ start, end uint64 }

type FINState struct {
	Seen        bool
	FinalOffset uint64
	Applied     bool // CloseWrite called
	Acked       bool // Remote peer acknowledged
}

// Session represents the Egress side session holding the backend DB TCP connection and state machine.
type Session struct {
	recoveryLease   *resource.RetentionLease
	metricSuspended bool
	mu              sync.Mutex

	ID             string
	SessionToken   string
	InstanceID     string // Local Egress instance ID
	OwnerPeer      string
	TargetID       string
	closeReason    string
	State          State
	Epoch          uint64
	CreatedAt      time.Time
	ResumeDeadline time.Time
	ResumeEnabled  bool

	// Backend DB connection
	BackendConn net.Conn

	// Stream offsets (all uint64)
	upWrittenOffset uint64 // Actually written to DB socket
	upAckOffset     uint64 // Stable boundary acknowledged to client
	receiptTable    *stream.ReceiptTable
	upReceiveWindow uint64 // Max inflight window in bytes (e.g. 2 MiB)
	maxInflight     int    // Max inflight frames (e.g. 8)

	// Upstream reordering queue
	upPendingFrames map[uint64]*InflightUpFrame
	writerCond      *sync.Cond
	writingClosed   bool

	// Barrier & inflight writing tracking
	inflightWriting bool
	inflightFrame   *InflightUpFrame
	inflightDoneCh  chan struct{}
	barrierPending  bool

	// Downstream buffer & offsets
	downBuffer         *stream.ByteWindow
	downProducedOffset uint64 // Read from DB socket
	downAckOffset      uint64 // Acknowledged delivered to client local socket
	downCond           *sync.Cond
	downReadReserved   int64 // Raw backend read allocation, including a short tail

	// Stream & Transport reservation
	transportMode    string // "ws" or "http-sse"
	currentStreamID  string
	streamSubscribed bool
	dataPlaneActive  bool // true after client ready ACK is received
	activeSSECancel  context.CancelFunc

	// FIN tracking
	UpFIN   FINState
	DownFIN FINState

	// Cached idempotency results
	lastResumeRequestID string
	lastResumeRequest   *protocol.ResumeRequest
	lastResumeResponse  *protocol.ResumeResponse

	// Timers and LRU
	graceTimer      *time.Timer
	activationTimer *time.Timer
	unattachedTimer *time.Timer
	attachDeadline  time.Time // absolute initial attach deadline; zero once activated
	attachEpoch     uint64    // epoch the initial attach deadline applies to
	onTimeout       func(*Session)
	onFinalize      func(*Session)
	closed          atomic.Bool
	finalized       atomic.Bool

	// Back reference to Registry for global resource accounting
	registry *Registry

	// Negotiated limits
	negotiatedMaxFrameSize                 int
	done                                   chan struct{}
	retainedBytes                          atomic.Int64
	notify                                 chan struct{}
	recoveryGrace                          time.Duration
	socketWriteTimeout                     time.Duration
	backpressureTimeout                    time.Duration
	ingressInstance                        string
	resumeBusy                             bool
	resumePendingID                        string
	resumePendingBytes                     []byte
	downFrames                             []downFrame
	downEmitted                            uint64
	emittingEnd                            uint64
	emitting                               bool
	pendingAck                             uint64
	finEmitting, finEmitted, pendingFinAck bool
	lastControl                            time.Time
	heartbeatNonce                         string
	heartbeatEpoch                         uint64
	heartbeatStream                        string
	transportWriting                       bool
	transportRxBlocked                     bool
	// downFINSent is set once the down FIN entered emit settlement in any
	// epoch. Unlike finEmitted it survives a resume, which is what a resume's
	// down_fin.applied claim is checked against.
	downFINSent bool
}

func generateID(prefix string, byteLen int) string {
	b := make([]byte, byteLen)
	_, _ = rand.Read(b)
	return prefix + hex.EncodeToString(b)
}

func NewSession(
	ownerPeer, targetID, instanceID string,
	backendConn net.Conn,
	maxBufferPerDir int64,
	upWindowBytes int64,
	maxInflightFrames int,
	receiptLimit int,
	resumeEnabled bool,
) *Session {
	s := &Session{
		ID:            generateID("s_", 16),
		SessionToken:  generateID("st_", 32),
		InstanceID:    instanceID,
		OwnerPeer:     ownerPeer,
		TargetID:      targetID,
		State:         StateAttaching,
		Epoch:         1,
		CreatedAt:     time.Now(),
		recoveryGrace: 30 * time.Second, socketWriteTimeout: 60 * time.Second, lastControl: time.Now(),
		ResumeEnabled:      resumeEnabled,
		BackendConn:        backendConn,
		upWrittenOffset:    0,
		upAckOffset:        0,
		upReceiveWindow:    uint64(upWindowBytes),
		maxInflight:        maxInflightFrames,
		receiptTable:       stream.NewReceiptTable(receiptLimit),
		upPendingFrames:    make(map[uint64]*InflightUpFrame),
		downBuffer:         stream.NewByteWindow(0, maxBufferPerDir),
		downProducedOffset: 0,
		downAckOffset:      0,
	}
	s.done = make(chan struct{})
	s.recoveryGrace = 30 * time.Second
	s.socketWriteTimeout = 60 * time.Second
	s.backpressureTimeout = 120 * time.Second
	s.lastControl = time.Now()
	s.writerCond = sync.NewCond(&s.mu)
	s.downCond = sync.NewCond(&s.mu)
	atomic.AddInt64(&observability.DefaultMetrics.ActiveSessions, 1)

	// Start single backend writer loop
	go s.runBackendWriter()

	return s
}

func (s *Session) GetState() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.State
}

func (s *Session) GetEpoch() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Epoch
}

func (s *Session) UpWrittenOffset() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upWrittenOffset
}

func (s *Session) UpAckOffset() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upAckOffset
}

func (s *Session) DownProducedOffset() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.downProducedOffset
}

func (s *Session) DownAckOffset() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.downAckOffset
}

func (s *Session) IsDataPlaneActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dataPlaneActive
}

func (s *Session) BufferSize() int { return int(s.retainedBytes.Load()) }

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (s *Session) Done() <-chan struct{} { return s.done }
