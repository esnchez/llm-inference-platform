package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/simworker"
)

func worker(t *testing.T, id string, budget int, models ...string) *simworker.Worker {
	t.Helper()
	w, err := simworker.New(simworker.Config{ID: id, Models: models, TokenBudget: budget})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

var reqSeq atomic.Uint64

func req(model string, cost int) inference.InferenceRequest {
	return inference.InferenceRequest{ID: fmt.Sprint("r", reqSeq.Add(1)), Model: model, PromptTokens: cost - 1, MaxNewTokens: 1}
}

// newScheduler uses a 1h safety poll by default, so tests prove that the
// CapacityChanged signal (not polling) wakes the dispatcher.
func newScheduler(t *testing.T, mutate func(*Config), workers ...inference.Worker) *Scheduler {
	t.Helper()
	cfg := Config{QueueTimeout: time.Hour, HeadMaxAge: time.Hour, SafetyPoll: time.Hour}
	if mutate != nil {
		mutate(&cfg)
	}
	s := New(cfg, workers...)
	t.Cleanup(s.Close)
	return s
}

type result struct {
	lease inference.Lease
	err   error
}

// acquireAsync starts Acquire in a goroutine and waits until the request is
// queued (stats.Waiting reaches wantWaiting).
func acquireAsync(t *testing.T, ctx context.Context, s *Scheduler, r inference.InferenceRequest, wantWaiting int) <-chan result {
	t.Helper()
	out := make(chan result, 1)
	go func() {
		l, err := s.Acquire(ctx, r)
		out <- result{l, err}
	}()
	waitFor(t, fmt.Sprintf("%d waiting", wantWaiting), func() bool { return s.Stats().Waiting == wantWaiting })
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func get(t *testing.T, c <-chan result) result {
	t.Helper()
	select {
	case r := <-c:
		return r
	case <-time.After(3 * time.Second):
		t.Fatal("Acquire did not return")
		return result{}
	}
}

func assertPending(t *testing.T, c <-chan result) {
	t.Helper()
	select {
	case r := <-c:
		t.Fatalf("request admitted too early: %+v", r)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestImmediateErrors(t *testing.T) {
	s := newScheduler(t, nil, worker(t, "a", 10, "m"), worker(t, "b", 20, "other"))
	ctx := context.Background()

	if _, err := s.Acquire(ctx, req("missing", 1)); !errors.Is(err, inference.ErrModelNotFound) {
		t.Fatalf("err = %v, want ErrModelNotFound", err)
	}
	// 15 tokens fit on b, but b does not serve m.
	if _, err := s.Acquire(ctx, req("m", 15)); !errors.Is(err, inference.ErrRequestTooLarge) {
		t.Fatalf("err = %v, want ErrRequestTooLarge", err)
	}
	bad := inference.InferenceRequest{Model: "m", MaxNewTokens: 0}
	if _, err := s.Acquire(ctx, bad); !errors.Is(err, inference.ErrInvalidRequest) {
		t.Fatalf("err = %v, want ErrInvalidRequest", err)
	}
	empty := newScheduler(t, nil)
	if _, err := empty.Acquire(ctx, req("m", 1)); !errors.Is(err, inference.ErrModelNotFound) {
		t.Fatalf("empty scheduler: err = %v", err)
	}
}

func TestImmediateAdmissionSpreadsLoad(t *testing.T) {
	s := newScheduler(t, nil, worker(t, "a", 10, "m"), worker(t, "b", 10, "m"))
	l1, err := s.Acquire(context.Background(), req("m", 10))
	if err != nil {
		t.Fatal(err)
	}
	l2, err := s.Acquire(context.Background(), req("m", 10))
	if err != nil {
		t.Fatal(err)
	}
	if l1.WorkerID() == l2.WorkerID() {
		t.Fatalf("both leases on %s", l1.WorkerID())
	}
	if got := s.Stats().AdmittedImmediately; got != 2 {
		t.Fatalf("admitted immediately = %d", got)
	}
}

func TestQueuedRequestAdmittedOnCapacitySignal(t *testing.T) {
	s := newScheduler(t, nil, worker(t, "a", 10, "m"))
	held, err := s.Acquire(context.Background(), req("m", 10))
	if err != nil {
		t.Fatal(err)
	}
	c := acquireAsync(t, context.Background(), s, req("m", 5), 1)
	assertPending(t, c)

	held.Release() // fires CapacityChanged; the safety poll is 1h
	r := get(t, c)
	if r.err != nil {
		t.Fatal(r.err)
	}
	r.lease.Release()
	st := s.Stats()
	if st.Waiting != 0 || st.AdmittedFromQueue != 1 || st.Models[0].QueuedTokens != 0 {
		t.Fatalf("stats after admission: %+v", st)
	}
}

func TestFIFOOrder(t *testing.T) {
	s := newScheduler(t, nil, worker(t, "a", 10, "m"))
	held, _ := s.Acquire(context.Background(), req("m", 10))
	first := acquireAsync(t, context.Background(), s, req("m", 6), 1)
	second := acquireAsync(t, context.Background(), s, req("m", 6), 2)

	held.Release()
	r := get(t, first)
	if r.err != nil {
		t.Fatal(r.err)
	}
	assertPending(t, second) // 6 + 6 > 10
	r.lease.Release()
	if r2 := get(t, second); r2.err != nil {
		t.Fatal(r2.err)
	} else {
		r2.lease.Release()
	}
}

func TestSkipAheadWhileHeadIsYoung(t *testing.T) {
	s := newScheduler(t, nil, worker(t, "a", 10, "m"))
	held, _ := s.Acquire(context.Background(), req("m", 6)) // 4 tokens free
	big := acquireAsync(t, context.Background(), s, req("m", 10), 1)
	// The small request fits now and the head is young: it passes the head
	// (it may be admitted before the queue ever holds two requests).
	small := acquireAsync(t, context.Background(), s, req("m", 4), 1)

	r := get(t, small)
	if r.err != nil {
		t.Fatal(r.err)
	}
	assertPending(t, big)
	r.lease.Release()
	held.Release()
	if rb := get(t, big); rb.err != nil {
		t.Fatal(rb.err)
	} else {
		rb.lease.Release()
	}
}

func TestFIFOBarrierWhenHeadIsOld(t *testing.T) {
	s := newScheduler(t, func(c *Config) { c.HeadMaxAge = time.Millisecond }, worker(t, "a", 10, "m"))
	held, _ := s.Acquire(context.Background(), req("m", 6)) // 4 tokens free
	big := acquireAsync(t, context.Background(), s, req("m", 10), 1)
	time.Sleep(5 * time.Millisecond) // the head ages past HeadMaxAge
	small := acquireAsync(t, context.Background(), s, req("m", 4), 2)

	// It would fit, but the barrier stops it from passing the old head.
	assertPending(t, small)
	if s.Stats().BarrierWakeups == 0 {
		t.Fatal("barrier never engaged")
	}
	held.Release()
	rb := get(t, big)
	if rb.err != nil {
		t.Fatal(rb.err)
	}
	assertPending(t, small) // 10 + 4 > 10
	rb.lease.Release()
	if rs := get(t, small); rs.err != nil {
		t.Fatal(rs.err)
	} else {
		rs.lease.Release()
	}
}

func TestQueueFullByTokens(t *testing.T) {
	// Budget 10, factor 2: at most 20 tokens queued for the model.
	s := newScheduler(t, func(c *Config) { c.MinRetryAfter = 2 * time.Second }, worker(t, "a", 10, "m"))
	held, _ := s.Acquire(context.Background(), req("m", 10))
	defer held.Release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	acquireAsync(t, ctx, s, req("m", 10), 1)
	acquireAsync(t, ctx, s, req("m", 10), 2)

	_, err := s.Acquire(context.Background(), req("m", 1))
	if !errors.Is(err, inference.ErrQueueFull) {
		t.Fatalf("err = %v, want ErrQueueFull", err)
	}
	var ra *inference.RetryAfterError
	if !errors.As(err, &ra) || ra.RetryAfter < 2*time.Second || ra.RetryAfter >= 4*time.Second {
		t.Fatalf("retry-after = %+v, want [2s, 4s)", ra)
	}
	if st := s.Stats(); st.RejectedQueueFull != 1 || st.Models[0].QueueTokenLimit != 20 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestQueueFullByRequestCount(t *testing.T) {
	s := newScheduler(t, func(c *Config) { c.MaxQueuedRequests = 1 },
		worker(t, "a", 10, "m"), worker(t, "b", 10, "n"))
	ha, _ := s.Acquire(context.Background(), req("m", 10))
	hb, _ := s.Acquire(context.Background(), req("n", 10))
	defer ha.Release()
	defer hb.Release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	acquireAsync(t, ctx, s, req("m", 1), 1)
	// The global cap applies across models.
	if _, err := s.Acquire(context.Background(), req("n", 1)); !errors.Is(err, inference.ErrQueueFull) {
		t.Fatalf("err = %v, want ErrQueueFull", err)
	}
}

func TestQueueTimeout(t *testing.T) {
	s := newScheduler(t, func(c *Config) { c.QueueTimeout = 20 * time.Millisecond }, worker(t, "a", 10, "m"))
	held, _ := s.Acquire(context.Background(), req("m", 10))
	defer held.Release()

	start := time.Now()
	_, err := s.Acquire(context.Background(), req("m", 1))
	if !errors.Is(err, inference.ErrQueueTimeout) {
		t.Fatalf("err = %v, want ErrQueueTimeout", err)
	}
	if d := time.Since(start); d < 20*time.Millisecond {
		t.Fatalf("returned after %v, before the timeout", d)
	}
	var ra *inference.RetryAfterError
	if !errors.As(err, &ra) {
		t.Fatal("timeout without Retry-After")
	}
	if st := s.Stats(); st.Waiting != 0 || st.RejectedQueueTimeout != 1 || st.Models[0].QueuedTokens != 0 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestClientDeadlineCapsTheWait(t *testing.T) {
	s := newScheduler(t, nil, worker(t, "a", 10, "m")) // queue timeout 1h
	held, _ := s.Acquire(context.Background(), req("m", 10))
	defer held.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := s.Acquire(ctx, req("m", 1))
	if !errors.Is(err, inference.ErrQueueTimeout) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want a timeout", err)
	}
	waitFor(t, "empty queue", func() bool { return s.Stats().Waiting == 0 })
}

func TestCancelWhileQueued(t *testing.T) {
	w := worker(t, "a", 10, "m")
	s := newScheduler(t, nil, w)
	held, _ := s.Acquire(context.Background(), req("m", 10))
	ctx, cancel := context.WithCancel(context.Background())
	c := acquireAsync(t, ctx, s, req("m", 5), 1)
	cancel()
	if r := get(t, c); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", r.err)
	}
	st := s.Stats()
	if st.Waiting != 0 || st.CancelledInQueue != 1 || st.Models[0].QueuedTokens != 0 {
		t.Fatalf("stats: %+v", st)
	}
	held.Release()
	waitFor(t, "capacity back", func() bool { return w.Snapshot().TokensReserved == 0 })
}

func TestModels(t *testing.T) {
	s := newScheduler(t, nil, worker(t, "a", 1, "y", "x"), worker(t, "b", 1, "x", "z"))
	if got, want := s.Models(), []string{"x", "y", "z"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Models() = %v, want %v", got, want)
	}
}

// TestStressInvariants hammers several realistic workers through the queue,
// with clients that cancel, and checks the invariants once everything ends:
// no worker ever exceeds its budget, no capacity is lost, the queues are
// empty, every request got exactly one outcome and no goroutine leaked.
// TestStressInvariants runs the same storm with both placements: the
// invariants (no budget overrun, no leak, every request accounted for) must
// not depend on where requests go.
func TestStressInvariants(t *testing.T) {
	t.Run("round-robin", func(t *testing.T) { stressInvariants(t, &RoundRobin{}) })
	t.Run("prefix-affinity", func(t *testing.T) {
		p := NewPrefixAffinity(AffinityConfig{MinMatchBlocks: 1})
		stressInvariants(t, p)
		if st := p.Stats(); st.AffinityHits == 0 || st.Tree.Nodes == 0 {
			t.Fatalf("prefix affinity never used: %+v", st)
		}
	})
}

func stressInvariants(t *testing.T, placement Placement) {
	const (
		budget   = 300
		requests = 1500
	)
	baseline := runtime.NumGoroutine()
	var workers []inference.Worker
	var sims []*simworker.Worker
	for i := range 3 {
		w, err := simworker.New(simworker.Config{
			ID: fmt.Sprint("w", i), Models: []string{"m"}, TokenBudget: budget,
			InterTokenLatency: 50 * time.Microsecond, ITLLoadFactor: 0.05, MeanOutputTokens: 8,
		})
		if err != nil {
			t.Fatal(err)
		}
		workers = append(workers, w)
		sims = append(sims, w)
	}
	// A deep queue (factor 50) so that most requests actually wait, pass each
	// other and hit the barrier, instead of being shed on arrival.
	s := New(Config{QueueTimeout: 2 * time.Second, HeadMaxAge: 20 * time.Millisecond, QueueTokenFactor: 50, SafetyPoll: 100 * time.Millisecond, Placement: placement}, workers...)

	var ok, full, timedOut, cancelled, overBudget atomic.Int64
	var wg sync.WaitGroup
	for i := range requests {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if i%10 == 0 { // some clients give up while queued
				go func() {
					time.Sleep(time.Duration(rand.IntN(5)) * time.Millisecond)
					cancel()
				}()
			}
			r := inference.InferenceRequest{ID: fmt.Sprint(i), Model: "m", PromptTokens: 1 + rand.IntN(60), MaxNewTokens: 1 + rand.IntN(40),
				// Five shared "system prompts" with a private tail.
				PrefixHashes: []uint64{uint64(rand.IntN(5)), uint64(1000 + i)}}
			lease, err := s.Acquire(ctx, r)
			switch {
			case err == nil:
			case errors.Is(err, inference.ErrQueueFull):
				full.Add(1)
				return
			case errors.Is(err, inference.ErrQueueTimeout):
				timedOut.Add(1)
				return
			case errors.Is(err, context.Canceled):
				cancelled.Add(1)
				return
			default:
				t.Errorf("unexpected error: %v", err)
				return
			}
			defer lease.Release()
			for _, w := range sims {
				if w.Snapshot().TokensReserved > budget {
					overBudget.Add(1)
				}
			}
			chunks, wait := inference.StartStream(ctx, lease, 4)
			for range chunks {
			}
			if err := wait(); err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("stream: %v", err)
			}
			ok.Add(1)
		}(i)
	}
	wg.Wait()
	s.Close()

	if n := overBudget.Load(); n > 0 {
		t.Fatalf("a worker exceeded its budget %d times", n)
	}
	if total := ok.Load() + full.Load() + timedOut.Load() + cancelled.Load(); total != requests {
		t.Fatalf("outcomes = %d, want %d", total, requests)
	}
	if ok.Load() == 0 {
		t.Fatal("no request succeeded")
	}
	st := s.Stats()
	if st.Waiting != 0 || st.Models[0].QueuedTokens != 0 || st.Models[0].QueuedRequests != 0 {
		t.Fatalf("queue not empty: %+v", st)
	}
	for _, w := range sims {
		if snap := w.Snapshot(); snap.TokensReserved != 0 || snap.ActiveLeases != 0 {
			t.Fatalf("leaked capacity: %+v", snap)
		}
	}
	waitFor(t, "goroutines to exit", func() bool { return runtime.NumGoroutine() <= baseline+2 })
	if st.AdmittedFromQueue == 0 {
		t.Fatal("the stress test never exercised the queue")
	}
	t.Logf("ok=%d full=%d timeout=%d cancelled=%d admitted_from_queue=%d barrier_wakeups=%d p50=%.1fms p99=%.1fms",
		ok.Load(), full.Load(), timedOut.Load(), cancelled.Load(), st.AdmittedFromQueue, st.BarrierWakeups, st.QueueWaitP50Ms, st.QueueWaitP99Ms)
}
