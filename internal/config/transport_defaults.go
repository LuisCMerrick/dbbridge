package config

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

func (c *Config) applyTransportDefaults() error {
	// WS defaults
	var err error
	c.Transport.WS.HandshakeTimeout, err = parseDuration(c.Transport.WS.HandshakeTimeoutRaw, 5*time.Second)
	if err != nil {
		return fmt.Errorf("invalid transport.ws.handshake_timeout: %w", err)
	}
	if c.Transport.WS.HandshakeTimeout <= 0 {
		return fmt.Errorf("transport.ws.handshake_timeout (%v) must be > 0", c.Transport.WS.HandshakeTimeout)
	}

	c.Transport.WS.WriteIdleTimeout, err = parseDuration(c.Transport.WS.WriteIdleTimeoutRaw, 60*time.Second)
	if err != nil {
		return fmt.Errorf("invalid transport.ws.write_idle_timeout: %w", err)
	}
	if c.Transport.WS.WriteIdleTimeout <= 0 {
		return fmt.Errorf("transport.ws.write_idle_timeout (%v) must be > 0", c.Transport.WS.WriteIdleTimeout)
	}
	if c.Transport.H2PoolSize <= 0 {
		c.Transport.H2PoolSize = 4
	}
	if c.Transport.H2PoolSize > MaxH2PoolSize {
		return fmt.Errorf("transport.h2_pool_size (%d) must be at most %d", c.Transport.H2PoolSize, MaxH2PoolSize)
	}

	frameSize, err := parseByteSize(c.Transport.FrameSizeRaw, 32*1024)
	if err != nil {
		return fmt.Errorf("invalid transport.frame_size: %w", err)
	}
	c.Transport.FrameSize = int(frameSize)

	maxFrameSize, err := parseByteSize(c.Transport.MaxFrameSizeRaw, 256*1024)
	if err != nil {
		return fmt.Errorf("invalid transport.max_frame_size: %w", err)
	}
	c.Transport.MaxFrameSize = int(maxFrameSize)

	if c.Transport.UpMaxInflightFrames <= 0 {
		c.Transport.UpMaxInflightFrames = 8
	}

	upWindowBytes, err := parseByteSize(c.Transport.UpWindowBytesRaw, 2*1024*1024)
	if err != nil {
		return fmt.Errorf("invalid transport.up_window_bytes: %w", err)
	}
	c.Transport.UpWindowBytes = upWindowBytes

	if c.Transport.UpMaxInflightFrames > (math.MaxInt-8)/2 {
		return errors.New("up_max_inflight_frames overflow")
	}
	minReceiptLimit := 2*c.Transport.UpMaxInflightFrames + 8
	if c.Transport.ReceiptLimitPerSession <= 0 {
		c.Transport.ReceiptLimitPerSession = max(64, minReceiptLimit)
	}
	if c.Transport.ReceiptLimitPerSession < minReceiptLimit {
		return errors.New("receipt_limit_per_session must be >= 2 * up_max_inflight_frames + 8")
	}

	c.Transport.Heartbeat, err = parseDuration(c.Transport.HeartbeatRaw, 15*time.Second)
	if err != nil {
		return fmt.Errorf("invalid transport.heartbeat: %w", err)
	}

	c.Transport.FailureDetectTimeout, err = parseDuration(c.Transport.FailureDetectTimeoutRaw, 45*time.Second)
	if err != nil {
		return fmt.Errorf("invalid transport.failure_detect_timeout: %w", err)
	}
	if c.Transport.FailureDetectTimeout <= 0 {
		return fmt.Errorf("transport.failure_detect_timeout (%v) must be > 0", c.Transport.FailureDetectTimeout)
	}

	c.Transport.Recovery.Enabled, err = parseBool(c.Transport.Recovery.EnabledRaw, true)
	if err != nil {
		return fmt.Errorf("invalid transport.recovery.enabled: %w", err)
	}
	c.Transport.Recovery.Grace, err = parseDuration(c.Transport.Recovery.GraceRaw, 30*time.Second)
	if err != nil {
		return fmt.Errorf("invalid transport.recovery.grace: %w", err)
	}
	c.Transport.Recovery.BarrierTimeout, err = parseDuration(c.Transport.Recovery.BarrierTimeoutRaw, 5*time.Second)
	if err != nil {
		return fmt.Errorf("invalid transport.recovery.barrier_timeout: %w", err)
	}

	c.Transport.UnattachedTimeout, err = parseDuration(c.Transport.UnattachedTimeoutRaw, 30*time.Second)
	if err != nil {
		return fmt.Errorf("invalid transport.unattached_timeout: %w", err)
	}
	if c.Transport.UnattachedTimeout <= 0 {
		return fmt.Errorf("transport.unattached_timeout (%v) must be > 0", c.Transport.UnattachedTimeout)
	}

	c.Transport.CreateRetryTimeout, err = parseDuration(c.Transport.CreateRetryTimeoutRaw, 30*time.Second)
	if err != nil {
		return fmt.Errorf("invalid transport.create_retry_timeout: %w", err)
	}
	if c.Transport.CreateRetryTimeout <= 0 {
		return fmt.Errorf("transport.create_retry_timeout (%v) must be > 0", c.Transport.CreateRetryTimeout)
	}

	c.Transport.AckBatchBytes, err = parseByteSize(c.Transport.AckBatchBytesRaw, 1*1024*1024)
	if err != nil {
		return fmt.Errorf("invalid transport.ack_batch_bytes: %w", err)
	}
	if c.Transport.AckBatchBytes <= 0 {
		return fmt.Errorf("transport.ack_batch_bytes (%d) must be > 0", c.Transport.AckBatchBytes)
	}

	c.Transport.AckMaxDelay, err = parseDuration(c.Transport.AckMaxDelayRaw, 200*time.Millisecond)
	if err != nil {
		return fmt.Errorf("invalid transport.ack_max_delay: %w", err)
	}
	if c.Transport.AckMaxDelay <= 0 {
		return fmt.Errorf("transport.ack_max_delay (%v) must be > 0", c.Transport.AckMaxDelay)
	}

	c.Transport.UpAckMaxDelay, err = parseDuration(c.Transport.UpAckMaxDelayRaw, 10*time.Millisecond)
	if err != nil {
		return fmt.Errorf("invalid transport.up_ack_max_delay: %w", err)
	}
	if c.Transport.UpAckMaxDelay <= 0 {
		return fmt.Errorf("transport.up_ack_max_delay (%v) must be > 0", c.Transport.UpAckMaxDelay)
	}

	chunkSize, err := parseByteSize(c.Transport.ChunkSizeRaw, 32*1024)
	if err != nil {
		return fmt.Errorf("invalid transport.chunk_size: %w", err)
	}
	c.Transport.ChunkSize = int(chunkSize)
	if c.Transport.ChunkSize <= 0 {
		return fmt.Errorf("transport.chunk_size (%d) must be > 0", c.Transport.ChunkSize)
	}

	c.Transport.BufferPerDirection, err = parseByteSize(c.Transport.BufferPerDirectionRaw, 4*1024*1024)
	if err != nil {
		return fmt.Errorf("invalid transport.buffer_per_direction: %w", err)
	}
	if c.Transport.BufferPerDirection <= 0 {
		return fmt.Errorf("transport.buffer_per_direction (%d) must be > 0", c.Transport.BufferPerDirection)
	}

	c.Transport.GlobalBufferLimit, err = parseByteSize(c.Transport.GlobalBufferLimitRaw, 256*1024*1024)
	if err != nil {
		return fmt.Errorf("invalid transport.global_buffer_limit: %w", err)
	}
	if c.Transport.GlobalBufferLimit <= 0 {
		return fmt.Errorf("transport.global_buffer_limit (%d) must be > 0", c.Transport.GlobalBufferLimit)
	}

	c.Transport.SuspendedBufferLimit, err = parseByteSize(c.Transport.SuspendedBufferLimitRaw, 64*1024*1024)
	if err != nil {
		return fmt.Errorf("invalid transport.suspended_buffer_limit: %w", err)
	}
	if c.Transport.SuspendedBufferLimit <= 0 {
		return fmt.Errorf("transport.suspended_buffer_limit (%d) must be > 0", c.Transport.SuspendedBufferLimit)
	}

	if c.Transport.UserAgent == "" {
		c.Transport.UserAgent = DefaultUserAgent
	}
	if err := validUserAgent(c.Transport.UserAgent); err != nil {
		return err
	}

	c.Transport.CodecBufferLimit, err = parseByteSize(c.Transport.CodecBufferLimitRaw, 32*1024*1024)
	if err != nil {
		return fmt.Errorf("invalid transport.codec_buffer_limit: %w", err)
	}
	if c.Transport.CodecBufferLimit <= 0 {
		return fmt.Errorf("transport.codec_buffer_limit (%d) must be > 0", c.Transport.CodecBufferLimit)
	}

	c.Transport.MetadataLimit, err = parseByteSize(c.Transport.MetadataLimitRaw, 32*1024*1024)
	if err != nil {
		return fmt.Errorf("invalid transport.metadata_limit: %w", err)
	}
	if c.Transport.MetadataLimit <= 0 {
		return fmt.Errorf("transport.metadata_limit (%d) must be > 0", c.Transport.MetadataLimit)
	}

	c.Transport.IdempotencyTombstoneTTL, err = parseDuration(c.Transport.IdempotencyTombstoneTTLRaw, 10*time.Minute)
	if err != nil {
		return fmt.Errorf("invalid transport.idempotency_tombstone_ttl: %w", err)
	}
	if c.Transport.IdempotencyTombstoneTTL < 10*time.Minute {
		return fmt.Errorf("transport.idempotency_tombstone_ttl (%v) must be >= 10m", c.Transport.IdempotencyTombstoneTTL)
	}

	c.Transport.DialTimeout, err = parseDuration(c.Transport.DialTimeoutRaw, 5*time.Second)
	if err != nil {
		return fmt.Errorf("invalid transport.dial_timeout: %w", err)
	}
	if c.Transport.DialTimeout <= 0 {
		return fmt.Errorf("transport.dial_timeout (%v) must be > 0", c.Transport.DialTimeout)
	}
	c.Transport.PeerDialTimeout, err = parseDuration(c.Transport.PeerDialTimeoutRaw, c.Transport.DialTimeout)
	if err != nil {
		return fmt.Errorf("invalid transport.peer_dial_timeout: %w", err)
	}
	if c.Transport.PeerDialTimeout <= 0 {
		return fmt.Errorf("transport.peer_dial_timeout (%v) must be > 0", c.Transport.PeerDialTimeout)
	}

	c.Transport.ControlRequestTimeout, err = parseDuration(c.Transport.ControlRequestTimeoutRaw, 10*time.Second)
	if err != nil {
		return fmt.Errorf("invalid transport.control_request_timeout: %w", err)
	}
	if c.Transport.ControlRequestTimeout <= 0 {
		return fmt.Errorf("transport.control_request_timeout (%v) must be > 0", c.Transport.ControlRequestTimeout)
	}

	c.Transport.UpRequestTimeout, err = parseDuration(c.Transport.UpRequestTimeoutRaw, 30*time.Second)
	if err != nil {
		return fmt.Errorf("invalid transport.up_request_timeout: %w", err)
	}
	if c.Transport.UpRequestTimeout <= 0 {
		return fmt.Errorf("transport.up_request_timeout (%v) must be > 0", c.Transport.UpRequestTimeout)
	}

	c.Transport.SocketWriteTimeout, err = parseDuration(c.Transport.SocketWriteTimeoutRaw, 60*time.Second)
	if err != nil {
		return fmt.Errorf("invalid transport.socket_write_timeout: %w", err)
	}
	if c.Transport.SocketWriteTimeout <= 0 {
		return fmt.Errorf("transport.socket_write_timeout (%v) must be > 0", c.Transport.SocketWriteTimeout)
	}

	c.Transport.SSEWriteIdleTimeout, err = parseDuration(c.Transport.SSEWriteIdleTimeoutRaw, 60*time.Second)
	if err != nil {
		return fmt.Errorf("invalid transport.sse_write_idle_timeout: %w", err)
	}
	if c.Transport.SSEWriteIdleTimeout <= 0 {
		return fmt.Errorf("transport.sse_write_idle_timeout (%v) must be > 0", c.Transport.SSEWriteIdleTimeout)
	}

	c.Transport.BackpressureTimeout, err = parseDuration(c.Transport.BackpressureTimeoutRaw, 120*time.Second)
	if err != nil {
		return fmt.Errorf("invalid transport.backpressure_timeout: %w", err)
	}
	if c.Transport.BackpressureTimeout <= 0 {
		return fmt.Errorf("transport.backpressure_timeout (%v) must be > 0", c.Transport.BackpressureTimeout)
	}

	c.Transport.ShutdownTimeout, err = parseDuration(c.Transport.ShutdownTimeoutRaw, 30*time.Second)
	if err != nil {
		return fmt.Errorf("invalid transport.shutdown_timeout: %w", err)
	}
	if c.Transport.ShutdownTimeout <= 0 {
		return fmt.Errorf("transport.shutdown_timeout (%v) must be > 0", c.Transport.ShutdownTimeout)
	}

	c.deriveTransportDefaults()

	// Positive value checks
	if c.Transport.FrameSize <= 0 {
		return fmt.Errorf("transport.frame_size (%d) must be > 0", c.Transport.FrameSize)
	}
	if c.Transport.MaxFrameSize <= 0 {
		return fmt.Errorf("transport.max_frame_size (%d) must be > 0", c.Transport.MaxFrameSize)
	}
	if c.Transport.Heartbeat <= 0 {
		return fmt.Errorf("transport.heartbeat (%v) must be > 0", c.Transport.Heartbeat)
	}
	if c.Transport.Recovery.Enabled {
		if c.Transport.Recovery.Grace <= 0 {
			return fmt.Errorf("transport.recovery.grace (%v) must be > 0", c.Transport.Recovery.Grace)
		}
		if c.Transport.Recovery.BarrierTimeout <= 0 {
			return fmt.Errorf("transport.recovery.barrier_timeout (%v) must be > 0", c.Transport.Recovery.BarrierTimeout)
		}
	}

	if c.HTTP.Enabled && c.HTTP.TLS.Enabled {
		if c.HTTP.TLS.CertFile == "" || c.HTTP.TLS.KeyFile == "" {
			return errors.New("http.tls.cert_file and key_file are required when http.tls.enabled is true")
		}
		if !c.offline {
			if _, err := c.LoadServerCertificate(); err != nil {
				return WithCode(CodeTLS, fmt.Errorf("http.tls certificate/key: %w", err))
			}
			c.warnInsecurePermissions(c.resolvePath(c.HTTP.TLS.KeyFile))
		}
	}

	// Cross validation for protocol constraints
	if c.Transport.FrameSize > c.Transport.MaxFrameSize {
		return fmt.Errorf("frame_size (%s) must be <= max_frame_size (%s)", valueNote(c.Transport.FrameSize, c.Transport.FrameSizeRaw), valueNote(c.Transport.MaxFrameSize, c.Transport.MaxFrameSizeRaw))
	}
	if c.Transport.MaxFrameSize > 1024*1024 {
		return fmt.Errorf("max_frame_size (%d) must be <= 1 MiB", c.Transport.MaxFrameSize)
	}
	if int64(c.Transport.MaxFrameSize) > c.Transport.UpWindowBytes {
		return fmt.Errorf("max_frame_size (%s) must be <= up_window_bytes (%s)", valueNote(c.Transport.MaxFrameSize, c.Transport.MaxFrameSizeRaw), valueNote(c.Transport.UpWindowBytes, c.Transport.UpWindowBytesRaw))
	}
	if c.Transport.UpWindowBytes > c.Transport.BufferPerDirection {
		return fmt.Errorf("up_window_bytes (%s) must be <= buffer_per_direction (%s)", valueNote(c.Transport.UpWindowBytes, c.Transport.UpWindowBytesRaw), valueNote(c.Transport.BufferPerDirection, c.Transport.BufferPerDirectionRaw))
	}
	if c.Transport.AckBatchBytes >= c.Transport.BufferPerDirection {
		return fmt.Errorf("ack_batch_bytes (%s) must be < buffer_per_direction (%s)", valueNote(c.Transport.AckBatchBytes, c.Transport.AckBatchBytesRaw), valueNote(c.Transport.BufferPerDirection, c.Transport.BufferPerDirectionRaw))
	}
	if c.Transport.SuspendedBufferLimit >= c.Transport.GlobalBufferLimit {
		return fmt.Errorf("suspended_buffer_limit (%s) must be < global_buffer_limit (%s)", valueNote(c.Transport.SuspendedBufferLimit, c.Transport.SuspendedBufferLimitRaw), valueNote(c.Transport.GlobalBufferLimit, c.Transport.GlobalBufferLimitRaw))
	}
	if c.Transport.Heartbeat > time.Duration(math.MaxInt64/3) || c.Transport.FailureDetectTimeout < 3*c.Transport.Heartbeat {
		return fmt.Errorf("failure_detect_timeout (%s) must be >= 3 * heartbeat (%s)", valueNote(c.Transport.FailureDetectTimeout, c.Transport.FailureDetectTimeoutRaw), valueNote(c.Transport.Heartbeat, c.Transport.HeartbeatRaw))
	}
	if c.Transport.Recovery.Enabled && c.Transport.Recovery.Grace > 0 {
		if c.Transport.Recovery.BarrierTimeout >= c.Transport.Recovery.Grace {
			return fmt.Errorf("recovery.barrier_timeout (%s) must be < recovery.grace (%s)", valueNote(c.Transport.Recovery.BarrierTimeout, c.Transport.Recovery.BarrierTimeoutRaw), valueNote(c.Transport.Recovery.Grace, c.Transport.Recovery.GraceRaw))
		}
	}
	if c.Transport.SQLReplay {
		return errors.New("sql_replay is not supported and must be false")
	}
	return nil
}

// rawUnset reports whether a raw scalar was omitted (nil or blank string), in
// which case the parsed value holds the built-in default.
func rawUnset(v interface{}) bool {
	if v == nil {
		return true
	}
	str, ok := v.(string)
	return ok && strings.TrimSpace(str) == ""
}

// deriveTransportDefaults links defaults of dependent fields so that changing
// one value does not trip a constraint against an unset neighbour:
// failure_detect_timeout follows heartbeat; up_window_bytes, ack_batch_bytes,
// max_frame_size and frame_size follow buffer_per_direction;
// suspended_buffer_limit follows global_buffer_limit; and
// recovery.barrier_timeout follows recovery.grace. Explicit values are kept.
func (c *Config) deriveTransportDefaults() {
	t := &c.Transport
	if rawUnset(t.FailureDetectTimeoutRaw) && t.Heartbeat > 0 && t.Heartbeat <= time.Duration(math.MaxInt64/3) {
		t.FailureDetectTimeout = max(t.FailureDetectTimeout, 3*t.Heartbeat)
	}
	if t.BufferPerDirection > 0 {
		if rawUnset(t.UpWindowBytesRaw) {
			t.UpWindowBytes = min(t.UpWindowBytes, t.BufferPerDirection)
		}
		if rawUnset(t.AckBatchBytesRaw) {
			// The acknowledgement batch stays at most a quarter of the
			// per-direction buffer so the sender is never stalled waiting
			// for an acknowledgement of half its window.
			t.AckBatchBytes = max(1, min(t.AckBatchBytes, t.BufferPerDirection/4))
		}
	}
	if rawUnset(t.MaxFrameSizeRaw) && t.UpWindowBytes > 0 {
		t.MaxFrameSize = max(1, min(t.MaxFrameSize, int(min(t.UpWindowBytes, int64(math.MaxInt32)))))
	}
	if rawUnset(t.FrameSizeRaw) && t.MaxFrameSize > 0 {
		t.FrameSize = min(t.FrameSize, t.MaxFrameSize)
	}
	if rawUnset(t.SuspendedBufferLimitRaw) && t.GlobalBufferLimit > 0 {
		t.SuspendedBufferLimit = max(1, min(t.SuspendedBufferLimit, t.GlobalBufferLimit/4))
	}
	if rawUnset(t.Recovery.BarrierTimeoutRaw) && t.Recovery.Grace > 0 {
		t.Recovery.BarrierTimeout = max(time.Millisecond, min(t.Recovery.BarrierTimeout, t.Recovery.Grace/2))
	}
}

// valueNote renders a value for a constraint error and says when it is a
// built-in or derived default rather than a configured value.
func valueNote(v any, raw interface{}) string {
	if rawUnset(raw) {
		return fmt.Sprintf("%v, default", v)
	}
	return fmt.Sprint(v)
}
