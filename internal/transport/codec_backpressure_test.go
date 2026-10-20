package transport

import (
	"bufio"
	"context"
	"dbbridge/internal/resource"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestSSECodecWaitReportsBackpressureAndReleases(t *testing.T) {
	for _, outcome := range []string{"release", "timeout", "cancel"} {
		t.Run(outcome, func(t *testing.T) {
			budget := resource.New(4096)
			if !budget.Try(budget.Limit()) {
				t.Fatal("fixture")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body := io.NopCloser(strings.NewReader("event: ping\ndata: {}\n\n"))
			s := &SSESubscriber{ctx: ctx, body: body, reader: bufio.NewReader(body), codec: budget, maxFrameSize: 1024}
			defer s.Close()
			blocked := make(chan bool, 2)
			result := make(chan error, 1)
			go func() {
				event, err := s.NextEventWithProgress(ctx, 100*time.Millisecond, nil, func(v bool) { blocked <- v })
				if event != nil {
					event.Release()
				}
				result <- err
			}()
			select {
			case v := <-blocked:
				if !v {
					t.Fatal("missing wait notification")
				}
			case <-time.After(time.Second):
				t.Fatal("missing wait notification")
			}
			if outcome == "release" {
				budget.Put(budget.Limit())
			} else if outcome == "cancel" {
				cancel()
			}
			select {
			case err := <-result:
				if outcome == "release" && err != nil || outcome == "timeout" && !errors.Is(err, ErrBackpressure) || outcome == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("outcome=%s error=%v", outcome, err)
				}
			case <-time.After(time.Second):
				t.Fatal("codec wait failed to finish")
			}
			if v := <-blocked; v {
				t.Fatal("wait state not cleared")
			}
			if outcome != "release" {
				budget.Put(budget.Limit())
			}
			if budget.Used() != 0 {
				t.Fatal("codec lease leaked")
			}
		})
	}
}
