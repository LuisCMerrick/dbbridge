package observability

import (
	"bytes"
	"context"
	"dbbridge/internal/resource"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	ActiveSessions    int64
	CreatingSessions  int64
	SuspendedSessions int64
	ClosedSessions    int64

	UpBytesTotal      int64
	DownBytesTotal    int64
	PostCountTotal    int64
	PiggybackAckTotal int64
	ExplicitAckTotal  int64
	SSEReconnectTotal int64
	ResumeSuccess     int64
	ResumeFailed      int64

	DialFailedTotal   int64
	DialTotal         int64
	AuthReloadSuccess int64
	AuthReloadFailed  int64
	// AuthCredentialsStale is 1 while the last credential reload was
	// rejected and the previous snapshot is still in effect.
	AuthCredentialsStale int64
	// AuthReloadLastSuccessUnix is the Unix time of the last credential
	// load that was accepted (changed or not).
	AuthReloadLastSuccessUnix int64
	UpFINAppliedTotal         int64
	DownFINAckedTotal         int64
	BackpressureTotal         int64
	AdmissionRejected         int64
	BufferMemoryBytes         int64
	SuspendedMemBytes         int64

	// Disconnect supplement metrics per v1.1 Section 6
	TerminatedDirectTotal          int64
	TerminatedAfterResumeFailTotal int64
	RemoteCleanupFailedTotal       int64
	RemoteCleanupUnknownTotal      int64
	ClientCloseDurationSumMs       int64
	ClientCloseCount               int64
}

var DefaultMetrics = &Metrics{}

type AdminServer struct {
	collect    func(io.Writer)
	listenAddr string
	listener   net.Listener
	server     *http.Server
	ready      atomic.Bool
	mu         sync.Mutex

	// renderMu serializes /metrics rendering; a rendering younger than
	// metricsTTL is served again so frequent scrapes cannot repeatedly walk
	// the session registry and retention locks.
	renderMu   sync.Mutex
	rendered   []byte
	renderedAt time.Time
	metricsTTL time.Duration
}

// maxAdminConns caps concurrent admin connections; extra connections are
// closed immediately.
const maxAdminConns = 16

// defaultMetricsTTL is how long one /metrics rendering is reused.
const defaultMetricsTTL = time.Second

type limitedListener struct {
	net.Listener
	slots chan struct{}
}

func (l *limitedListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			return &limitedConn{Conn: c, release: func() { <-l.slots }}, nil
		default:
			_ = c.Close()
		}
	}
}

type limitedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *limitedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

func NewAdminServer(listenAddr string) *AdminServer {
	return &AdminServer{
		listenAddr: listenAddr,
		metricsTTL: defaultMetricsTTL,
	}
}

func (s *AdminServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server != nil {
		return fmt.Errorf("admin server already started on %s", s.listenAddr)
	}

	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return fmt.Errorf("admin listen failed on %s: %w", s.listenAddr, err)
	}
	ln = &limitedListener{Listener: ln, slots: make(chan struct{}, maxAdminConns)}
	s.listener = ln

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/metrics", s.handleMetrics)

	s.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    8 << 10,
	}

	srv := s.server
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("admin server serve error", "err", err)
		}
	}()

	return nil
}

// Ready reports whether /readyz currently returns 200.
func (s *AdminServer) Ready() bool { return s.ready.Load() }

func (s *AdminServer) SetReady(ready bool) {
	s.ready.Store(ready)
}

func (s *AdminServer) Close() error {
	s.mu.Lock()
	srv := s.server
	s.server = nil
	s.mu.Unlock()
	if srv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		return srv.Close()
	}
	return nil
}

func (s *AdminServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *AdminServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.ready.Load() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready\n"))
	}
}

func (s *AdminServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	s.renderMu.Lock()
	if s.rendered == nil || time.Since(s.renderedAt) >= s.metricsTTL {
		var buf bytes.Buffer
		s.renderMetrics(&buf)
		s.rendered, s.renderedAt = buf.Bytes(), time.Now()
	}
	body := s.rendered
	s.renderMu.Unlock()
	_, _ = w.Write(body)
}

func (s *AdminServer) renderMetrics(w io.Writer) {
	s.mu.Lock()
	fn := s.collect
	s.mu.Unlock()
	WriteEventMetrics(w)
	if fn != nil {
		fn(w)
	}
	fmt.Fprintf(w, "# HELP dbb_active_sessions Number of currently active sessions\n")
	fmt.Fprintf(w, "# TYPE dbb_active_sessions gauge\ndbb_active_sessions %d\n", atomic.LoadInt64(&DefaultMetrics.ActiveSessions))
	fmt.Fprintf(w, "# HELP dbb_suspended_sessions Number of currently suspended sessions\n")
	fmt.Fprintf(w, "# TYPE dbb_suspended_sessions gauge\ndbb_suspended_sessions %d\n", atomic.LoadInt64(&DefaultMetrics.SuspendedSessions))
	fmt.Fprintf(w, "# HELP dbb_unbalanced_release_total Budget releases that exceeded the held amount (accounting bug indicator)\n")
	fmt.Fprintf(w, "# TYPE dbb_unbalanced_release_total counter\ndbb_unbalanced_release_total %d\n", resource.UnbalancedReleases())
	fmt.Fprintf(w, "# HELP dbb_up_bytes_total Total bytes sent upstream\n")
	fmt.Fprintf(w, "# TYPE dbb_up_bytes_total counter\ndbb_up_bytes_total %d\n", atomic.LoadInt64(&DefaultMetrics.UpBytesTotal))
	fmt.Fprintf(w, "# HELP dbb_down_bytes_total Total bytes received downstream\n")
	fmt.Fprintf(w, "# TYPE dbb_down_bytes_total counter\ndbb_down_bytes_total %d\n", atomic.LoadInt64(&DefaultMetrics.DownBytesTotal))
	fmt.Fprintf(w, "# HELP dbb_post_count_total Total HTTP POST requests\n")
	fmt.Fprintf(w, "# TYPE dbb_post_count_total counter\ndbb_post_count_total %d\n", atomic.LoadInt64(&DefaultMetrics.PostCountTotal))
	fmt.Fprintf(w, "# HELP dbb_piggyback_ack_total Total piggyback ACK counts\n")
	fmt.Fprintf(w, "# TYPE dbb_piggyback_ack_total counter\ndbb_piggyback_ack_total %d\n", atomic.LoadInt64(&DefaultMetrics.PiggybackAckTotal))
	fmt.Fprintf(w, "# HELP dbb_explicit_ack_total Total explicit /ack counts\n")
	fmt.Fprintf(w, "# TYPE dbb_explicit_ack_total counter\ndbb_explicit_ack_total %d\n", atomic.LoadInt64(&DefaultMetrics.ExplicitAckTotal))
	writeCounter(w, "dbb_resume_success", "Total successful resumes", atomic.LoadInt64(&DefaultMetrics.ResumeSuccess))
	writeCounter(w, "dbb_resume_failed", "Total failed resumes", atomic.LoadInt64(&DefaultMetrics.ResumeFailed))
	fmt.Fprintf(w, "# HELP dbb_buffer_memory_bytes Total buffered memory bytes in use\n")
	fmt.Fprintf(w, "# TYPE dbb_buffer_memory_bytes gauge\ndbb_buffer_memory_bytes %d\n", atomic.LoadInt64(&DefaultMetrics.BufferMemoryBytes))
	fmt.Fprintf(w, "# HELP dbb_suspended_mem_bytes Memory bytes occupied by suspended sessions\n")
	fmt.Fprintf(w, "# TYPE dbb_suspended_mem_bytes gauge\ndbb_suspended_mem_bytes %d\n", atomic.LoadInt64(&DefaultMetrics.SuspendedMemBytes))

	fmt.Fprintf(w, "# HELP dbb_terminated_direct_total Total sessions terminated directly on failure without resume\n")
	fmt.Fprintf(w, "# TYPE dbb_terminated_direct_total counter\ndbb_terminated_direct_total %d\n", atomic.LoadInt64(&DefaultMetrics.TerminatedDirectTotal))
	fmt.Fprintf(w, "# HELP dbb_terminated_after_resume_fail_total Total sessions terminated after recovery failed\n")
	fmt.Fprintf(w, "# TYPE dbb_terminated_after_resume_fail_total counter\ndbb_terminated_after_resume_fail_total %d\n", atomic.LoadInt64(&DefaultMetrics.TerminatedAfterResumeFailTotal))
	fmt.Fprintf(w, "# HELP dbb_remote_cleanup_failed_total Total remote cleanup delete attempts failed\n")
	fmt.Fprintf(w, "# TYPE dbb_remote_cleanup_failed_total counter\ndbb_remote_cleanup_failed_total %d\n", atomic.LoadInt64(&DefaultMetrics.RemoteCleanupFailedTotal))
	fmt.Fprintf(w, "# HELP dbb_remote_cleanup_unknown_total Total remote cleanup delete attempts with unknown/stale epoch\n")
	fmt.Fprintf(w, "# TYPE dbb_remote_cleanup_unknown_total counter\ndbb_remote_cleanup_unknown_total %d\n", atomic.LoadInt64(&DefaultMetrics.RemoteCleanupUnknownTotal))
	fmt.Fprintf(w, "# HELP dbb_client_close_duration_sum_ms Total client close duration in milliseconds\n")
	fmt.Fprintf(w, "# TYPE dbb_client_close_duration_sum_ms counter\ndbb_client_close_duration_sum_ms %d\n", atomic.LoadInt64(&DefaultMetrics.ClientCloseDurationSumMs))
	writeCounter(w, "dbb_client_close_count", "Total client connections actively closed", atomic.LoadInt64(&DefaultMetrics.ClientCloseCount))
	// Fixed order: a map range would reorder the families on every scrape.
	for _, c := range []struct {
		name, help string
		counter    *int64
	}{
		{"dial_total", "Backend dial attempts made by the egress role", &DefaultMetrics.DialTotal},
		{"dial_failed_total", "Backend dial attempts that failed", &DefaultMetrics.DialFailedTotal},
		{"auth_reload_success_total", "Credential reloads that installed a changed snapshot", &DefaultMetrics.AuthReloadSuccess},
		{"auth_reload_failed_total", "Credential load or reload attempts that were rejected", &DefaultMetrics.AuthReloadFailed},
		{"up_fin_applied_total", "Upstream FIN frames applied to the backend connection", &DefaultMetrics.UpFINAppliedTotal},
		{"down_fin_acked_total", "Downstream FIN frames acknowledged by the peer", &DefaultMetrics.DownFINAckedTotal},
		{"backpressure_total", "Times a session waited for buffer space", &DefaultMetrics.BackpressureTotal},
		{"admission_rejected_total", "Connections and requests rejected by admission control", &DefaultMetrics.AdmissionRejected},
	} {
		fmt.Fprintf(w, "# HELP dbb_%s %s\n# TYPE dbb_%s counter\ndbb_%s %d\n", c.name, c.help, c.name, c.name, atomic.LoadInt64(c.counter))
	}
	fmt.Fprintf(w, "# HELP dbb_auth_credentials_stale 1 while the last credential reload was rejected and the previous credentials are still in effect\n")
	fmt.Fprintf(w, "# TYPE dbb_auth_credentials_stale gauge\ndbb_auth_credentials_stale %d\n", atomic.LoadInt64(&DefaultMetrics.AuthCredentialsStale))
	fmt.Fprintf(w, "# HELP dbb_auth_reload_last_success_timestamp_seconds Unix time of the last accepted credential load\n")
	fmt.Fprintf(w, "# TYPE dbb_auth_reload_last_success_timestamp_seconds gauge\ndbb_auth_reload_last_success_timestamp_seconds %d\n", atomic.LoadInt64(&DefaultMetrics.AuthReloadLastSuccessUnix))
}

// writeCounter emits a counter under its Prometheus name (with _total) and,
// for compatibility with existing dashboards, under its legacy name.
// The legacy family is deprecated.
func writeCounter(w io.Writer, legacy, help string, v int64) {
	fmt.Fprintf(w, "# HELP %s_total %s\n# TYPE %s_total counter\n%s_total %d\n", legacy, help, legacy, legacy, v)
	fmt.Fprintf(w, "# HELP %s %s (deprecated: use %s_total)\n# TYPE %s counter\n%s %d\n", legacy, help, legacy, legacy, legacy, v)
}

func SetupLogger(format string, level string) {
	SetupLoggerOutput(format, level, os.Stdout)
}

func SetupLoggerOutput(format string, level string, output io.Writer) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: lvl,
	}

	var handler slog.Handler
	if format == "json" {
		handler = slog.NewJSONHandler(output, opts)
	} else {
		handler = slog.NewTextHandler(output, opts)
	}
	slog.SetDefault(slog.New(handler))
}

func (s *AdminServer) SetCollector(fn func(io.Writer)) { s.mu.Lock(); s.collect = fn; s.mu.Unlock() }
