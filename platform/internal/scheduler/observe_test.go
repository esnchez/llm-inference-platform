package scheduler

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"llm-inference-platform/internal/simworker"
)

// recorder is an Observer that keeps every event as a string.
type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(format string, args ...any) {
	r.mu.Lock()
	r.events = append(r.events, fmt.Sprintf(format, args...))
	r.mu.Unlock()
}

func (r *recorder) count(event string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.events {
		if e == event {
			n++
		}
	}
	return n
}

func (r *recorder) Admitted(model, path string) { r.add("admitted %s %s", model, path) }
func (r *recorder) QueueLeft(model, outcome string, _ time.Duration) {
	r.add("queue_left %s %s", model, outcome)
}
func (r *recorder) WorkerFirstToken(worker string, _ time.Duration) { r.add("first_token %s", worker) }
func (r *recorder) WorkerTokenGap(worker string, _ time.Duration)   { r.add("token_gap %s", worker) }
func (r *recorder) WorkerFailure(worker, kind string)               { r.add("failure %s %s", worker, kind) }
func (r *recorder) Retried(model, result string)                    { r.add("retried %s %s", model, result) }
func (r *recorder) BreakerTransition(worker, to string)             { r.add("breaker %s %s", worker, to) }
func (r *recorder) HealthCheck(worker string, ok bool)              { r.add("health %s %v", worker, ok) }
func (r *recorder) RestartDetected(worker string)                   { r.add("restart %s", worker) }

func TestObserverAdmissionAndQueueEvents(t *testing.T) {
	rec := &recorder{}
	s := newScheduler(t, lifecycleConfig(func(c *Config) { c.Observer = rec }), faulty(t, "a", 10, simworker.Faults{}))
	held, err := s.Acquire(context.Background(), req("m", 10))
	if err != nil {
		t.Fatal(err)
	}
	queued := acquireAsync(t, context.Background(), s, req("m", 10), 1)
	ctx, cancel := context.WithCancel(context.Background())
	gone := acquireAsync(t, ctx, s, req("m", 10), 2)
	cancel()
	if r := get(t, gone); r.err == nil {
		t.Fatal("cancelled request got a lease")
	}
	held.Release()
	r := get(t, queued)
	if r.err != nil {
		t.Fatal(r.err)
	}
	r.lease.Release()

	for event, want := range map[string]int{
		"admitted m immediate":   1,
		"admitted m queued":      1,
		"queue_left m admitted":  1,
		"queue_left m cancelled": 1,
	} {
		if got := rec.count(event); got != want {
			t.Errorf("%q: %d, want %d (events %v)", event, got, want, rec.events)
		}
	}
}

func TestObserverStreamFailureRetryAndBreaker(t *testing.T) {
	rec := &recorder{}
	var logs bytes.Buffer
	a := faulty(t, "a", 100, simworker.Faults{Hang: true})
	b := faulty(t, "b", 100, simworker.Faults{})
	s := newScheduler(t, lifecycleConfig(func(c *Config) {
		c.Observer = rec
		c.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
		c.MaxRetries = 1
		c.BreakerFailures = 1
		c.BreakerCooldown = time.Minute
		c.TTFTTimeout = 20 * time.Millisecond
		c.TTFTTimeoutPerToken = -1
	}), a, b)
	clock := useClock(s)

	for range 2 { // round-robin: one of them starts on a
		if _, _, err := execute(t, s, gen(3)); err != nil {
			t.Fatal(err)
		}
	}
	checks := map[string]int{
		"failure a ttft_timeout": 1,
		"retried m succeeded":    1,
		"breaker a open":         1,
		"first_token b":          2,
		"token_gap b":            4, // 3 tokens per request
	}
	for event, want := range checks {
		if got := rec.count(event); got != want {
			t.Errorf("%q: %d, want %d (events %v)", event, got, want, rec.events)
		}
	}

	// Recovery: half-open, then a good probe closes it.
	_ = a.SetFaults(simworker.Faults{})
	clock.advance(time.Minute)
	if err := s.RemoveWorker("b"); err != nil { // so the probe must go to a
		t.Fatal(err)
	}
	if _, _, err := execute(t, s, gen(3)); err != nil {
		t.Fatal(err)
	}
	if rec.count("breaker a half_open") != 1 || rec.count("breaker a closed") != 1 {
		t.Errorf("recovery transitions missing: %v", rec.events)
	}

	for _, msg := range []string{`"msg":"worker added"`, `"msg":"worker hung"`, `"msg":"request retried"`,
		`"msg":"breaker opened"`, `"reason":"failures"`, `"msg":"breaker half-open"`, `"msg":"breaker closed"`,
		`"msg":"worker draining"`, `"msg":"worker removed"`} {
		if !strings.Contains(logs.String(), msg) {
			t.Errorf("log %s missing:\n%s", msg, logs.String())
		}
	}
}

func TestObserverHealthAndRestart(t *testing.T) {
	rec := &recorder{}
	var logs bytes.Buffer
	a := faulty(t, "a", 100, simworker.Faults{})
	s := newScheduler(t, lifecycleConfig(func(c *Config) {
		c.Observer = rec
		c.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
		c.HealthInterval = 2 * time.Millisecond
	}), a)
	waitFor(t, "a health check", func() bool { return rec.count("health a true") > 0 })
	a.Restart()
	waitFor(t, "restart", func() bool { return rec.count("restart a") == 1 })
	_ = a.SetFaults(simworker.Faults{Down: true})
	waitFor(t, "unhealthy", func() bool { return !status(t, s, "a").Healthy })
	if rec.count("health a false") < 3 || rec.count("breaker a open") != 1 {
		t.Fatalf("events: %v", rec.events)
	}
	for _, msg := range []string{`"msg":"worker restart detected"`, `"msg":"worker unhealthy"`, `"reason":"unhealthy"`} {
		if !strings.Contains(logs.String(), msg) {
			t.Errorf("log %s missing:\n%s", msg, logs.String())
		}
	}
}
