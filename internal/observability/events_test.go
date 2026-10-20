package observability

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestEventMetricsUseBoundedLabelsAndRealBuckets(t *testing.T) {
	RecordEvent("ingress", "resume", "success", "ws", "http-sse")
	RecordEvent("secret-role", "secret-event", "secret-reason", "secret-from", "secret-to")
	ObserveDuration("ingress", "barrier", 20*time.Millisecond)
	var b bytes.Buffer
	WriteEventMetrics(&b)
	s := b.String()
	for _, want := range []string{`event="resume"`, `from="ws",to="http-sse"`, `phase="barrier",le="0.05"`, `dbb_operation_duration_seconds_count`} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %s", want)
		}
	}
	if strings.Contains(s, "secret-") {
		t.Fatal("unbounded labels leaked")
	}
}
