package service

// notifyStopping is a no-op: the Windows handler reports StopPending itself.
func notifyStopping() error { return nil }
