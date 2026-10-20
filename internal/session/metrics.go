package session

// MetricsSnapshot returns only bounded aggregate values under the session lock.
func (s *Session) MetricsSnapshot() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]int64{
		"state_" + string(s.State): 1,
		"up_pending_frames":        int64(len(s.upPendingFrames)),
		"down_retained_frames":     int64(len(s.downFrames)),
		"digest_receipts":          int64(s.receiptTable.Count()),
		"down_unconfirmed_bytes":   int64(s.downProducedOffset - s.downAckOffset),
	}
}
