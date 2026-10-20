package observability

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

type eventKey struct{ role, kind, reason, from, to string }
type durationKey struct{ role, phase string }
type durationValue struct {
	count   uint64
	sum     float64
	buckets [10]uint64
}

var eventStats = struct {
	sync.Mutex
	events    map[eventKey]uint64
	durations map[durationKey]durationValue
}{events: map[eventKey]uint64{}, durations: map[durationKey]durationValue{}}
var durationBounds = [10]float64{.001, .005, .01, .05, .1, .5, 1, 5, 30, 60}

func oneOf(value string, allowed ...string) string {
	for _, s := range allowed {
		if value == s {
			return value
		}
	}
	return "other"
}
func RecordEvent(role, kind, reason, from, to string) {
	role = oneOf(role, "ingress", "egress")
	kind = oneOf(kind, "fault", "terminate", "resume", "fallback", "cleanup", "cache_hit", "cache_miss", "cache_invalidate", "precheck", "attach")
	reason = oneOf(reason, "normal", "network", "timeout", "protocol", "unauthorized", "forbidden", "instance", "offset", "quota", "backend_lost", "barrier", "buffer", "success", "failed", "unknown", "authentication", "transport", "shutdown", "local_socket", "dial_failed", "draining", "not_needed", "skipped")
	from = oneOf(from, "ws", "http-sse", "none")
	to = oneOf(to, "ws", "http-sse", "none")
	eventStats.Lock()
	eventStats.events[eventKey{role, kind, reason, from, to}]++
	eventStats.Unlock()
}
func ObserveDuration(role, phase string, d time.Duration) {
	role = oneOf(role, "ingress", "egress")
	phase = oneOf(phase, "barrier", "local_close", "recovery", "cleanup", "probe", "precheck", "attach", "control")
	seconds := max(0, d.Seconds())
	key := durationKey{role, phase}
	eventStats.Lock()
	v := eventStats.durations[key]
	v.count++
	v.sum += seconds
	for i, b := range durationBounds {
		if seconds <= b {
			v.buckets[i]++
		}
	}
	eventStats.durations[key] = v
	eventStats.Unlock()
}
func WriteEventMetrics(w io.Writer) {
	eventStats.Lock()
	events := make(map[eventKey]uint64, len(eventStats.events))
	for k, v := range eventStats.events {
		events[k] = v
	}
	durations := make(map[durationKey]durationValue, len(eventStats.durations))
	for k, v := range eventStats.durations {
		durations[k] = v
	}
	eventStats.Unlock()
	eventKeys := make([]eventKey, 0, len(events))
	for k := range events {
		eventKeys = append(eventKeys, k)
	}
	sort.Slice(eventKeys, func(i, j int) bool {
		a, b := eventKeys[i], eventKeys[j]
		return strings.Join([]string{a.role, a.kind, a.reason, a.from, a.to}, "\x00") < strings.Join([]string{b.role, b.kind, b.reason, b.from, b.to}, "\x00")
	})
	fmt.Fprintln(w, "# HELP dbb_session_events_total Session lifecycle events by role, event, reason and carrier transition")
	fmt.Fprintln(w, "# TYPE dbb_session_events_total counter")
	for _, k := range eventKeys {
		fmt.Fprintf(w, "dbb_session_events_total{role=%q,event=%q,reason=%q,from=%q,to=%q} %d\n", k.role, k.kind, k.reason, k.from, k.to, events[k])
	}
	durationKeys := make([]durationKey, 0, len(durations))
	for k := range durations {
		durationKeys = append(durationKeys, k)
	}
	sort.Slice(durationKeys, func(i, j int) bool {
		if durationKeys[i].role != durationKeys[j].role {
			return durationKeys[i].role < durationKeys[j].role
		}
		return durationKeys[i].phase < durationKeys[j].phase
	})
	fmt.Fprintln(w, "# HELP dbb_operation_duration_seconds Duration of session operations by role and phase")
	fmt.Fprintln(w, "# TYPE dbb_operation_duration_seconds histogram")
	for _, k := range durationKeys {
		v := durations[k]
		for i, b := range durationBounds {
			fmt.Fprintf(w, "dbb_operation_duration_seconds_bucket{role=%q,phase=%q,le=%q} %d\n", k.role, k.phase, fmt.Sprint(b), v.buckets[i])
		}
		fmt.Fprintf(w, "dbb_operation_duration_seconds_bucket{role=%q,phase=%q,le=\"+Inf\"} %d\n", k.role, k.phase, v.count)
		fmt.Fprintf(w, "dbb_operation_duration_seconds_count{role=%q,phase=%q} %d\n", k.role, k.phase, v.count)
		fmt.Fprintf(w, "dbb_operation_duration_seconds_sum{role=%q,phase=%q} %g\n", k.role, k.phase, v.sum)
	}
}
