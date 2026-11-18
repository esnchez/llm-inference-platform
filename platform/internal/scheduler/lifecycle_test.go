package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/simworker"
)

// fakeClock drives the breaker cooldowns without sleeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func useClock(s *Scheduler) *fakeClock {
	c := &fakeClock{t: time.Unix(1_000_000, 0)}
	s.mu.Lock()
	s.now = c.now
	s.mu.Unlock()
	return c
}

// faulty returns a simulated worker with injected faults and a 1ms ITL.
func faulty(t *testing.T, id string, budget int, f simworker.Faults) *simworker.Worker {
	t.Helper()
	w, err := simworker.New(simworker.Config{
		ID: id, Models: []string{"m"}, TokenBudget: budget,
		InterTokenLatency: time.Millisecond, Faults: f,
	})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// lifecycleConfig disables health checks and retries unless a test
// enables them, so that each test exercises one mechanism.
func lifecycleConfig(mutate func(*Config)) func(*Config) {
	return func(c *Config) {
		c.HealthInterval = -1
		c.MaxRetries = -1
		c.Placement = &RoundRobin{}
		if mutate != nil {
			mutate(c)
		}
	}
}

// execute acquires and runs one request to the end.
func execute(t *testing.T, s *Scheduler, r inference.InferenceRequest) (worker string, chunks int, err error) {
	t.Helper()
	lease, err := s.Acquire(context.Background(), r)
	if err != nil {
		return "", 0, err
	}
	defer lease.Release()
	out, wait := inference.StartStream(context.Background(), lease, 4)
	for range out {
		chunks++
	}
	return lease.WorkerID(), chunks, wait()
}

func status(t *testing.T, s *Scheduler, id string) *inference.WorkerStatus {
	t.Helper()
	for _, snap := range s.Snapshots() {
		if snap.WorkerID == id {
			return snap.Status
		}
	}
	return nil
}

func gen(maxNew int) inference.InferenceRequest {
	r := req("m", maxNew+1)
	r.PromptTokens, r.MaxNewTokens = 1, maxNew
	return r
}

func TestBreakerOpensProbesAndCloses(t *testing.T) {
	a := faulty(t, "a", 100, simworker.Faults{ErrorRate: 1})
	s := newScheduler(t, lifecycleConfig(func(c *Config) {
		c.BreakerFailures = 2
		c.BreakerCooldown = time.Minute
	}), a)
	clock := useClock(s)

	for i := range 2 {
		if _, _, err := execute(t, s, gen(3)); !errors.Is(err, inference.ErrWorkerFailure) {
			t.Fatalf("request %d: %v, want a worker failure", i, err)
		}
	}
	if st := status(t, s, "a"); st.Breaker != "open" || st.Failures != 2 || st.BreakerOpens != 1 {
		t.Fatalf("after 2 failures: %+v", st)
	}
	if _, err := s.Acquire(context.Background(), gen(3)); !errors.Is(err, inference.ErrNoHealthyWorkers) {
		t.Fatalf("open breaker: %v, want ErrNoHealthyWorkers", err)
	}
	var ra *inference.RetryAfterError
	if _, err := s.Acquire(context.Background(), gen(3)); !errors.As(err, &ra) || ra.RetryAfter <= 0 {
		t.Fatalf("no Retry-After on %v", err)
	}

	// Cooldown over: half-open lets exactly one probe through.
	clock.advance(time.Minute)
	probe, err := s.Acquire(context.Background(), gen(3))
	if err != nil {
		t.Fatalf("half-open probe: %v", err)
	}
	if _, err := s.Acquire(context.Background(), gen(3)); !errors.Is(err, inference.ErrNoHealthyWorkers) {
		t.Fatalf("second request while probing: %v", err)
	}
	out, wait := inference.StartStream(context.Background(), probe, 4)
	for range out {
	}
	if err := wait(); !errors.Is(err, inference.ErrWorkerFailure) {
		t.Fatalf("probe: %v", err)
	}
	probe.Release()

	// The failed probe reopened it with a doubled cooldown.
	clock.advance(time.Minute)
	if _, err := s.Acquire(context.Background(), gen(3)); !errors.Is(err, inference.ErrNoHealthyWorkers) {
		t.Fatalf("1 min into a 2 min cooldown: %v", err)
	}
	clock.advance(time.Minute)
	_ = a.SetFaults(simworker.Faults{})
	if _, _, err := execute(t, s, gen(3)); err != nil {
		t.Fatalf("successful probe: %v", err)
	}
	if st := status(t, s, "a"); st.Breaker != "closed" || st.BreakerOpens != 2 {
		t.Fatalf("after a good probe: %+v", st)
	}
	if got := s.Stats().BreakerOpens; got != 2 {
		t.Fatalf("breaker_opens = %d", got)
	}
}

// badRequestWorker fails every stream with a request error, as vLLM does
// for a prompt longer than --max-model-len.
type badRequestWorker struct{ *simworker.Worker }

type badRequestLease struct{ inference.Lease }

func (w badRequestWorker) AcquireLease(ctx context.Context, r inference.InferenceRequest) (inference.Lease, error) {
	l, err := w.Worker.AcquireLease(ctx, r)
	if err != nil {
		return nil, err
	}
	return badRequestLease{l}, nil
}

func (l badRequestLease) ExecuteStream(context.Context, chan<- inference.TokenChunk) error {
	l.Lease.Release()
	return fmt.Errorf("%w: prompt longer than the engine's context", inference.ErrRequestTooLarge)
}

func TestRequestErrorsDoNotCountAgainstTheWorker(t *testing.T) {
	w := badRequestWorker{faulty(t, "a", 100, simworker.Faults{})}
	s := newScheduler(t, lifecycleConfig(func(c *Config) { c.BreakerFailures = 1; c.MaxRetries = 1 }), w)
	for range 3 {
		if _, _, err := execute(t, s, gen(3)); !errors.Is(err, inference.ErrRequestTooLarge) {
			t.Fatalf("err = %v", err)
		}
	}
	if st := status(t, s, "a"); st.Breaker != "closed" || st.Failures != 0 {
		t.Fatalf("request errors counted against the worker: %+v", st)
	}
	if st := s.Stats(); st.Retries != 0 || st.WorkerFailures != 0 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestRetryBeforeFirstTokenOnAnotherWorker(t *testing.T) {
	a := faulty(t, "a", 100, simworker.Faults{Down: true})
	b := faulty(t, "b", 100, simworker.Faults{})
	s := newScheduler(t, lifecycleConfig(func(c *Config) { c.MaxRetries = 1; c.BreakerFailures = 100 }), a, b)
	for i := range 6 {
		worker, chunks, err := execute(t, s, gen(3))
		if err != nil || chunks != 3 || worker != "b" {
			t.Fatalf("request %d: worker %s, %d chunks, err %v", i, worker, chunks, err)
		}
	}
	st := s.Stats()
	if st.Retries == 0 || st.Retries != st.RetrySuccesses || st.WorkerFailures != st.Retries {
		t.Fatalf("retries %d, successes %d, worker failures %d", st.Retries, st.RetrySuccesses, st.WorkerFailures)
	}
	for _, snap := range s.Snapshots() {
		if snap.ActiveLeases != 0 || snap.TokensReserved != 0 || snap.Status.InFlight != 0 {
			t.Fatalf("leaked capacity: %+v %+v", snap, snap.Status)
		}
	}
}

func TestNoRetryAfterTheFirstToken(t *testing.T) {
	a := faulty(t, "a", 100, simworker.Faults{FailAfterTokens: 2})
	b := faulty(t, "b", 100, simworker.Faults{FailAfterTokens: 2})
	s := newScheduler(t, lifecycleConfig(func(c *Config) { c.MaxRetries = 1 }), a, b)
	_, chunks, err := execute(t, s, gen(5))
	if chunks != 2 || !errors.Is(err, inference.ErrWorkerFailure) {
		t.Fatalf("%d chunks, err %v", chunks, err)
	}
	if st := s.Stats(); st.Retries != 0 || st.WorkerFailures != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestRetriesAreBounded(t *testing.T) {
	a := faulty(t, "a", 100, simworker.Faults{Down: true})
	b := faulty(t, "b", 100, simworker.Faults{Down: true})
	c := faulty(t, "c", 100, simworker.Faults{Down: true})
	s := newScheduler(t, lifecycleConfig(func(c *Config) { c.MaxRetries = 1; c.BreakerFailures = 100 }), a, b, c)
	if _, _, err := execute(t, s, gen(3)); !errors.Is(err, inference.ErrWorkerFailure) {
		t.Fatalf("err = %v", err)
	}
	if st := s.Stats(); st.Retries != 1 || st.WorkerFailures != 2 {
		t.Fatalf("one retry at most: %+v", st)
	}
}

func TestHangDetectedBeforeFirstTokenIsRetried(t *testing.T) {
	a := faulty(t, "a", 100, simworker.Faults{Hang: true})
	b := faulty(t, "b", 100, simworker.Faults{})
	s := newScheduler(t, lifecycleConfig(func(c *Config) {
		c.MaxRetries = 1
		c.BreakerFailures = 100
		c.TTFTTimeout = 20 * time.Millisecond
		c.TTFTTimeoutPerToken = -1
	}), a, b)
	for i := range 4 {
		if worker, _, err := execute(t, s, gen(3)); err != nil || worker != "b" {
			t.Fatalf("request %d: worker %s, err %v", i, worker, err)
		}
	}
	st := s.Stats()
	if st.TTFTTimeouts == 0 || st.TTFTTimeouts != st.Retries {
		t.Fatalf("ttft timeouts %d, retries %d", st.TTFTTimeouts, st.Retries)
	}
	if snap := s.Snapshots()[0]; snap.TokensReserved != 0 {
		t.Fatalf("the hung stream kept its capacity: %+v", snap)
	}
}

func TestStallMidStreamIsAWorkerFailure(t *testing.T) {
	a := faulty(t, "a", 100, simworker.Faults{StallAfterTokens: 2})
	s := newScheduler(t, lifecycleConfig(func(c *Config) { c.StallTimeout = 20 * time.Millisecond }), a)
	_, chunks, err := execute(t, s, gen(5))
	if chunks != 2 || !errors.Is(err, inference.ErrWorkerFailure) {
		t.Fatalf("%d chunks, err %v", chunks, err)
	}
	if st := s.Stats(); st.StallTimeouts != 1 {
		t.Fatalf("stall timeouts = %d", st.StallTimeouts)
	}
}

func TestHealthChecksExcludeAndReadmit(t *testing.T) {
	a := faulty(t, "a", 100, simworker.Faults{Down: true})
	s := newScheduler(t, lifecycleConfig(func(c *Config) {
		c.HealthInterval = 2 * time.Millisecond
		c.BreakerCooldown = time.Minute
	}), a)
	clock := useClock(s)
	waitFor(t, "a unhealthy", func() bool { return !status(t, s, "a").Healthy })
	if st := status(t, s, "a"); st.Breaker != "open" {
		t.Fatalf("an unhealthy worker's breaker must open: %+v", st)
	}
	if _, err := s.Acquire(context.Background(), gen(3)); !errors.Is(err, inference.ErrNoHealthyWorkers) {
		t.Fatalf("unhealthy: %v", err)
	}

	_ = a.SetFaults(simworker.Faults{})
	waitFor(t, "a healthy", func() bool { return status(t, s, "a").Healthy })
	clock.advance(time.Minute) // the breaker's cooldown
	if _, _, err := execute(t, s, gen(3)); err != nil {
		t.Fatalf("after recovery: %v", err)
	}
	if st := status(t, s, "a"); st.Breaker != "closed" || st.InstanceID == "" {
		t.Fatalf("after recovery: %+v", st)
	}
}

func TestRestartForgetsCachedPrefixes(t *testing.T) {
	a := faulty(t, "a", 1000, simworker.Faults{})
	p := NewPrefixAffinity(AffinityConfig{})
	s := newScheduler(t, lifecycleConfig(func(c *Config) {
		c.HealthInterval = 2 * time.Millisecond
		c.Placement = p
	}), a)
	waitFor(t, "first health check", func() bool { return status(t, s, "a").InstanceID != "" })
	if _, _, err := execute(t, s, prefixed(10, 1, 8)); err != nil {
		t.Fatal(err)
	}
	if b := p.Stats().Tree.BlocksPerWorker["a"]; b != 9 {
		t.Fatalf("tree holds %d blocks for a, want 9", b)
	}
	a.Restart()
	waitFor(t, "restart detected", func() bool { return s.Stats().RestartsDetected == 1 })
	if b := p.Stats().Tree.BlocksPerWorker["a"]; b != 0 {
		t.Fatalf("tree still holds %d blocks for the restarted worker", b)
	}
}

func TestAddWorkerAdmitsQueuedRequests(t *testing.T) {
	a := faulty(t, "a", 10, simworker.Faults{})
	s := newScheduler(t, lifecycleConfig(nil), a)
	held, err := s.Acquire(context.Background(), req("m", 10))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	queued := acquireAsync(t, context.Background(), s, req("m", 10), 1)

	if err := s.AddWorker(faulty(t, "b", 10, simworker.Faults{})); err != nil {
		t.Fatal(err)
	}
	r := get(t, queued)
	if r.err != nil || r.lease.WorkerID() != "b" {
		t.Fatalf("queued request: %v on %v", r.err, r.lease)
	}
	r.lease.Release()
	if err := s.AddWorker(faulty(t, "b", 10, simworker.Faults{})); err == nil {
		t.Fatal("duplicate worker ID accepted")
	}
	other, _ := simworker.New(simworker.Config{ID: "c", Models: []string{"other"}, TokenBudget: 10})
	if err := s.AddWorker(other); err != nil {
		t.Fatal(err)
	}
	if got := s.Models(); fmt.Sprint(got) != "[m other]" {
		t.Fatalf("models = %v", got)
	}
}

func TestRemoveWorkerDrains(t *testing.T) {
	a := faulty(t, "a", 100, simworker.Faults{})
	b := faulty(t, "b", 100, simworker.Faults{})
	p := NewPrefixAffinity(AffinityConfig{MaxLoad: 1})
	s := newScheduler(t, lifecycleConfig(func(c *Config) { c.Placement = p }), a, b)

	var onA inference.Lease
	for onA == nil {
		l, err := s.Acquire(context.Background(), prefixed(20, 1, 1))
		if err != nil {
			t.Fatal(err)
		}
		if l.WorkerID() == "a" {
			onA = l
		} else {
			l.Release()
		}
	}
	if err := s.RemoveWorker("a"); err != nil {
		t.Fatal(err)
	}
	if st := status(t, s, "a"); st == nil || st.State != "draining" || st.InFlight != 1 {
		t.Fatalf("a while draining: %+v", st)
	}
	for range 4 {
		if worker, _, err := execute(t, s, gen(3)); err != nil || worker != "b" {
			t.Fatalf("new request on %s (%v) while a drains", worker, err)
		}
	}
	// The stream in flight on a still completes.
	out, wait := inference.StartStream(context.Background(), onA, 4)
	for range out {
	}
	if err := wait(); err != nil {
		t.Fatalf("draining stream: %v", err)
	}
	onA.Release()
	if st := status(t, s, "a"); st != nil {
		t.Fatalf("a still registered after draining: %+v", st)
	}
	if _, ok := p.Stats().Tree.BlocksPerWorker["a"]; ok {
		t.Fatal("the removed worker's prefixes are still in the tree")
	}
	if err := s.RemoveWorker("a"); err == nil {
		t.Fatal("removing an unknown worker must fail")
	}
	if err := s.RemoveWorker("b"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire(context.Background(), gen(3)); !errors.Is(err, inference.ErrModelNotFound) {
		t.Fatalf("model without workers: %v", err)
	}
	if got := s.Models(); len(got) != 0 {
		t.Fatalf("models = %v", got)
	}
}

func TestDegradedWorkerOnlyUsedWhenOthersAreFull(t *testing.T) {
	a := faulty(t, "a", 100, simworker.Faults{Slowdown: 10})
	b := faulty(t, "b", 100, simworker.Faults{})
	s := newScheduler(t, lifecycleConfig(func(c *Config) { c.DegradedMinSamples = 2 }), a, b)
	// Round-robin until both have 2 samples.
	for !(status(t, s, "a").ITLSamples >= 2 && status(t, s, "b").ITLSamples >= 2) {
		if _, _, err := execute(t, s, gen(4)); err != nil {
			t.Fatal(err)
		}
	}
	if st := status(t, s, "a"); !st.Degraded {
		t.Fatalf("a should be degraded: %+v vs b %+v", st, status(t, s, "b"))
	}
	for range 4 {
		if worker, _, err := execute(t, s, gen(2)); err != nil || worker != "b" {
			t.Fatalf("placed on %s (%v), want b", worker, err)
		}
	}
	full, err := s.Acquire(context.Background(), req("m", 100)) // fills b
	if err != nil || full.WorkerID() != "b" {
		t.Fatalf("filling b: %v", err)
	}
	defer full.Release()
	l, err := s.Acquire(context.Background(), gen(2))
	if err != nil || l.WorkerID() != "a" {
		t.Fatalf("with b full the degraded worker must be used: %v", err)
	}
	l.Release()
}
