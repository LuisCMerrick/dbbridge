package auth

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/observability"
)

// rejectionReminder is how often an unchanged reload error is logged again
// at ERROR level while it persists.
const rejectionReminder = 5 * time.Minute

// reloadState serializes reloads and remembers the last rejection so that a
// persistent error is logged once (and then as a periodic reminder) instead
// of on every poll.
type reloadState struct {
	mu        sync.Mutex
	loaded    bool
	envLogged bool
	lastErr   string
	loggedAt  time.Time
}

// Reload re-reads the credentials now, as on SIGHUP or a Windows service
// parameter-change request. It returns the reason a candidate was rejected;
// the previous credentials then stay in effect.
func (a *Authenticator) Reload() error { return a.reload("request") }

func (a *Authenticator) reloadLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-t.C:
			_ = a.reload("interval")
		}
	}
}

// loadCredentialsFile performs one periodic reload.
func (a *Authenticator) loadCredentialsFile() { _ = a.reload("interval") }

func (a *Authenticator) reload(trigger string) error {
	st := &a.reloads
	st.mu.Lock()
	defer st.mu.Unlock()
	f, refs, err := a.source.Read()
	var warnings []string
	if err == nil {
		warnings, err = config.ValidateCredentialSnapshot(f, a.constraints)
		err = config.WithCode(config.CodeCredential, err)
	}
	now := time.Now()
	if err != nil {
		atomic.AddInt64(&observability.DefaultMetrics.AuthReloadFailed, 1)
		atomic.StoreInt64(&observability.DefaultMetrics.AuthCredentialsStale, 1)
		if !st.loaded {
			// Nothing was ever installed: authentication refuses every
			// request and Node.Start reports the error.
			a.mu.Lock()
			a.initialErr = err
			a.mu.Unlock()
			return err
		}
		msg := err.Error()
		if msg != st.lastErr || now.Sub(st.loggedAt) >= rejectionReminder || trigger == "request" {
			slog.Error("credential reload rejected; the previous credentials stay in effect, including identities the candidate disables or removes",
				"code", config.CodeReloadRejected, "cause_code", config.ErrorCode(err), "trigger", trigger, "err", err)
			st.loggedAt = now
		}
		st.lastErr = msg
		return &config.CodedError{Code: config.CodeReloadRejected, Err: err}
	}
	first := !st.loaded
	changed := a.install(f, refs, true)
	st.loaded = true
	atomic.StoreInt64(&observability.DefaultMetrics.AuthCredentialsStale, 0)
	atomic.StoreInt64(&observability.DefaultMetrics.AuthReloadLastSuccessUnix, now.Unix())
	if st.lastErr != "" {
		slog.Info("credential reload succeeded; the earlier reload error is resolved", "trigger", trigger)
		st.lastErr = ""
	}
	if !st.envLogged {
		st.envLogged = true
		if envs, _ := config.TokenSources(f, a.source.Dir()); len(envs) > 0 {
			slog.Info("tokens from environment variables are read once per process; rotating them requires a restart (token files are re-read on reload)", "variables", envs)
		}
	}
	if changed && !first {
		atomic.AddInt64(&observability.DefaultMetrics.AuthReloadSuccess, 1)
		slog.Info("credentials reloaded", "trigger", trigger, "revision", a.Revision())
		// Only a changed snapshot is reported, not every reload tick.
		for _, w := range warnings {
			slog.Warn("credentials reload warning", "warning", w)
		}
	} else if trigger == "request" {
		slog.Info("credentials re-read; no change", "trigger", trigger)
	}
	return nil
}
