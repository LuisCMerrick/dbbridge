package session

import (
	"time"

	"dbbridge/internal/protocol"
)

func (s *Session) SetOnFinalize(fn func(*Session)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onFinalize = fn
}

func (s *Session) SetNegotiatedLimits(limits protocol.SessionLimits) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.negotiatedMaxFrameSize = limits.MaxFrameSize
	if limits.UpWindowBytes > 0 {
		s.upReceiveWindow = uint64(limits.UpWindowBytes)
	}
	if limits.UpMaxInflightFrames > 0 {
		s.maxInflight = limits.UpMaxInflightFrames
	}
}

func (s *Session) NegotiatedMaxFrameSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.negotiatedMaxFrameSize
}

func (s *Session) SetRegistry(r *Registry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registry = r
}

func (s *Session) Configure(grace, writeTimeout time.Duration, ingress string, notify chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if grace > 0 {
		s.recoveryGrace = grace
	}
	if writeTimeout > 0 {
		s.socketWriteTimeout = writeTimeout
	}
	s.ingressInstance = ingress
	s.notify = notify
}

func (s *Session) SetBackpressureTimeout(timeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if timeout > 0 {
		s.backpressureTimeout = timeout
	}
}

func (s *Session) SetChunkSize(size int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.downBuffer.SetChunkSize(size)
}

func (s *Session) UpReceiveLimit(ack uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ack > ^uint64(0)-s.upReceiveWindow {
		return ^uint64(0)
	}
	return ack + s.upReceiveWindow
}
