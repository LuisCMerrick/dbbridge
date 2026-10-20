package observability

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// OBS-1: counters carry _total, every family has HELP and TYPE, and the
// output order is stable.
func TestAuditMetricsExposition(t *testing.T) {
	s := NewAdminServer("127.0.0.1:0")
	var a, b bytes.Buffer
	s.renderMetrics(&a)
	s.renderMetrics(&b)
	if a.String() != b.String() {
		t.Fatal("metric order changes between scrapes")
	}
	text := a.String()
	for _, want := range []string{"# TYPE dbb_resume_success_total counter\n", "# TYPE dbb_resume_failed_total counter\n", "# TYPE dbb_client_close_count_total counter\n", "# TYPE dbb_dial_total counter\n"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q", want)
		}
	}
	typed := map[string]bool{}
	for _, line := range strings.Split(text, "\n") {
		if f := strings.Fields(line); len(f) >= 3 && f[0] == "#" && f[1] == "TYPE" {
			if typed[f[2]] {
				t.Errorf("duplicate TYPE for %s", f[2])
			}
			typed[f[2]] = true
		}
	}
	for _, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name := strings.FieldsFunc(line, func(r rune) bool { return r == ' ' || r == '{' })[0]
		base := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(name, "_bucket"), "_count"), "_sum")
		if !typed[name] && !typed[base] {
			t.Errorf("sample %s has no TYPE", name)
		}
	}
}

// X-SEC-9: renders are cached briefly and connections are capped.
func TestAuditAdminCacheAndConnectionCap(t *testing.T) {
	s := NewAdminServer("127.0.0.1:0")
	var calls atomic.Int64
	s.SetCollector(func(io.Writer) { calls.Add(1) })
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	url := "http://" + s.listener.Addr().String() + "/metrics"
	for i := 0; i < 5; i++ {
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if calls.Load() != 1 {
		t.Fatalf("collector ran %d times for 5 scrapes within the cache window", calls.Load())
	}
	var conns []net.Conn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for i := 0; i < maxAdminConns; i++ {
		c, err := net.Dial("tcp", s.listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
	}
	extra, err := net.Dial("tcp", s.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	_ = extra.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err = extra.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection beyond the cap was served")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("connection beyond the cap was left open")
	}
}
