package simworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"llm-inference-platform/internal/inference"
)

const model = "sim-model"

func newWorker(t *testing.T, mutate func(*Config)) *Worker {
	t.Helper()
	cfg := Config{ID: "w1", Models: []string{model}, TokenBudget: 100}
	if mutate != nil {
		mutate(&cfg)
	}
	w, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return w
}

func request(prompt, maxNew int) inference.InferenceRequest {
	return inference.InferenceRequest{ID: "req", Model: model, PromptTokens: prompt, MaxNewTokens: maxNew}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	bad := []Config{
		{Models: []string{model}, TokenBudget: 1},
		{ID: "w", TokenBudget: 1},
		{ID: "w", Models: []string{model}},
		{ID: "w", Models: []string{model}, TokenBudget: 1, InterTokenLatency: -1},
	}
	for i, cfg := range bad {
		if _, err := New(cfg); err == nil {
			t.Errorf("config %d: expected error", i)
		}
	}
}

func TestAcquireLeaseAdmission(t *testing.T) {
	w := newWorker(t, nil)
	ctx := context.Background()

	l1, err := w.AcquireLease(ctx, request(30, 30))
	if err != nil {
		t.Fatalf("first lease: %v", err)
	}
	if got := w.Snapshot().TokensReserved; got != 60 {
		t.Fatalf("reserved = %d, want 60", got)
	}
	if _, err := w.AcquireLease(ctx, request(20, 21)); !errors.Is(err, inference.ErrAtCapacity) {
		t.Fatalf("over budget: err = %v, want ErrAtCapacity", err)
	}
	l2, err := w.AcquireLease(ctx, request(20, 20))
	if err != nil {
		t.Fatalf("exact fit: %v", err)
	}
	if _, err := w.AcquireLease(ctx, request(50, 51)); !errors.Is(err, inference.ErrRequestTooLarge) {
		t.Fatalf("too large: err = %v, want ErrRequestTooLarge", err)
	}
	if _, err := w.AcquireLease(ctx, inference.InferenceRequest{Model: "other", MaxNewTokens: 1}); !errors.Is(err, inference.ErrModelNotServed) {
		t.Fatalf("unknown model: err = %v, want ErrModelNotServed", err)
	}
	if _, err := w.AcquireLease(ctx, request(1, 0)); !errors.Is(err, inference.ErrInvalidRequest) {
		t.Fatalf("zero max tokens: err = %v, want ErrInvalidRequest", err)
	}

	l1.Release()
	l1.Release() // idempotent
	l2.Release()
	s := w.Snapshot()
	if s.TokensReserved != 0 || s.ActiveLeases != 0 {
		t.Fatalf("after release: %+v", s)
	}
	if s.LeasesGranted != 2 || s.LeasesRejected != 1 {
		t.Fatalf("counters: %+v", s)
	}
}

func TestAcquireLeaseCancelledContext(t *testing.T) {
	w := newWorker(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := w.AcquireLease(ctx, request(1, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestExecuteStreamEmitsAllTokens(t *testing.T) {
	w := newWorker(t, nil)
	l, err := w.AcquireLease(context.Background(), request(10, 5))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()

	chunks, wait := inference.StartStream(context.Background(), l, 0)
	var got []inference.TokenChunk
	for c := range chunks {
		got = append(got, c)
	}
	if err := wait(); err != nil {
		t.Fatalf("stream: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d chunks, want 5", len(got))
	}
	for i, c := range got {
		if c.Index != i || c.Text == "" {
			t.Errorf("chunk %d = %+v", i, c)
		}
		want := inference.FinishNone
		if i == 4 {
			want = inference.FinishLength
		}
		if c.FinishReason != want {
			t.Errorf("chunk %d finish = %q, want %q", i, c.FinishReason, want)
		}
	}
	if r := w.Snapshot().TokensReserved; r != 0 {
		t.Fatalf("capacity not freed after stream: reserved = %d", r)
	}
}

func TestExecuteStreamOnlyOnce(t *testing.T) {
	w := newWorker(t, nil)
	l, _ := w.AcquireLease(context.Background(), request(1, 1))
	_, wait := inference.StartStream(context.Background(), l, 1)
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	if err := l.ExecuteStream(context.Background(), make(chan inference.TokenChunk, 1)); !errors.Is(err, inference.ErrLeaseUsed) {
		t.Fatalf("second execute: err = %v, want ErrLeaseUsed", err)
	}
}

func TestExecuteStreamAfterRelease(t *testing.T) {
	w := newWorker(t, nil)
	l, _ := w.AcquireLease(context.Background(), request(1, 1))
	l.Release()
	if err := l.ExecuteStream(context.Background(), make(chan inference.TokenChunk, 1)); !errors.Is(err, inference.ErrLeaseReleased) {
		t.Fatalf("err = %v, want ErrLeaseReleased", err)
	}
}

func TestCancellationFreesCapacityImmediately(t *testing.T) {
	w := newWorker(t, func(c *Config) { c.InterTokenLatency = time.Hour })
	l, err := w.AcquireLease(context.Background(), request(10, 50))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()

	ctx, cancel := context.WithCancel(context.Background())
	chunks, wait := inference.StartStream(ctx, l, 0)
	<-chunks // first token (TTFT is zero), then the worker blocks on the 1h ITL
	if r := w.Snapshot().TokensReserved; r != 60 {
		t.Fatalf("reserved during stream = %d, want 60", r)
	}
	cancel()
	for range chunks {
	}
	if err := wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if r := w.Snapshot().TokensReserved; r != 0 {
		t.Fatalf("reserved after cancel = %d, want 0", r)
	}
}

func TestCancellationWhileConsumerIsStuck(t *testing.T) {
	// The consumer stops reading (slow client): the worker must not block
	// forever on the send once ctx is cancelled.
	w := newWorker(t, nil)
	l, _ := w.AcquireLease(context.Background(), request(1, 10))
	defer l.Release()
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan inference.TokenChunk) // nobody reads
	errc := make(chan error, 1)
	go func() { errc <- l.ExecuteStream(ctx, out) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ExecuteStream blocked on a stuck consumer")
	}
}

func TestLeaseExpiresWhenNotExecuted(t *testing.T) {
	w := newWorker(t, func(c *Config) { c.LeaseTTL = 10 * time.Millisecond })
	l, err := w.AcquireLease(context.Background(), request(10, 10))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "lease expiry", func() bool { return w.Snapshot().TokensReserved == 0 })
	if err := l.ExecuteStream(context.Background(), make(chan inference.TokenChunk, 10)); !errors.Is(err, inference.ErrLeaseExpired) {
		t.Fatalf("err = %v, want ErrLeaseExpired", err)
	}
	l.Release() // must not double-free
	if s := w.Snapshot(); s.TokensReserved != 0 || s.ActiveLeases != 0 {
		t.Fatalf("double free: %+v", s)
	}
}

func TestLeaseDoesNotExpireOnceExecuting(t *testing.T) {
	w := newWorker(t, func(c *Config) {
		c.LeaseTTL = 5 * time.Millisecond
		c.InterTokenLatency = 5 * time.Millisecond
	})
	l, _ := w.AcquireLease(context.Background(), request(1, 6))
	defer l.Release()
	chunks, wait := inference.StartStream(context.Background(), l, 0)
	n := 0
	for range chunks {
		n++
	}
	if err := wait(); err != nil || n != 6 {
		t.Fatalf("n = %d, err = %v", n, err)
	}
}

func TestConcurrentAcquireNeverExceedsBudget(t *testing.T) {
	const budget = 1000
	w := newWorker(t, func(c *Config) {
		c.TokenBudget = budget
		c.InterTokenLatency = 50 * time.Microsecond
	})

	var peak atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := request(10+i%40, 5+i%20)
			req.ID = fmt.Sprint(i)
			l, err := w.AcquireLease(context.Background(), req)
			if err != nil {
				if !errors.Is(err, inference.ErrAtCapacity) {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			defer l.Release()
			if r := int64(w.Snapshot().TokensReserved); r > peak.Load() {
				peak.Store(r)
			}
			chunks, wait := inference.StartStream(context.Background(), l, 4)
			for range chunks {
			}
			if err := wait(); err != nil {
				t.Errorf("stream: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if p := peak.Load(); p > budget {
		t.Fatalf("peak reserved %d exceeded budget %d", p, budget)
	}
	if s := w.Snapshot(); s.TokensReserved != 0 || s.ActiveLeases != 0 {
		t.Fatalf("leaked capacity: %+v", s)
	}
}

func TestNewRejectsNegativeRealismKnobs(t *testing.T) {
	for i, mutate := range []func(*Config){
		func(c *Config) { c.MeanOutputTokens = -1 },
		func(c *Config) { c.ITLLoadFactor = -0.1 },
		func(c *Config) { c.PrefixCacheBlocks = -1 },
	} {
		cfg := Config{ID: "w", Models: []string{model}, TokenBudget: 1}
		mutate(&cfg)
		if _, err := New(cfg); err == nil {
			t.Errorf("config %d: expected error", i)
		}
	}
}

func drain(t *testing.T, l inference.Lease) []inference.TokenChunk {
	t.Helper()
	chunks, wait := inference.StartStream(context.Background(), l, 0)
	var got []inference.TokenChunk
	for c := range chunks {
		got = append(got, c)
	}
	if err := wait(); err != nil {
		t.Fatalf("stream: %v", err)
	}
	return got
}

func TestVariableOutputLength(t *testing.T) {
	w := newWorker(t, func(c *Config) {
		c.TokenBudget = 100_000
		c.MeanOutputTokens = 20
	})
	stopped, lengths := 0, map[string]int{}
	for i := 0; i < 50; i++ {
		req := request(1, 200)
		req.ID = fmt.Sprint("req-", i)
		l, err := w.AcquireLease(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		got := drain(t, l)
		n := len(got)
		if n < 1 || n > 200 {
			t.Fatalf("%s: %d tokens, want 1..200", req.ID, n)
		}
		lengths[req.ID] = n
		finish := got[n-1].FinishReason
		switch {
		case n < 200 && finish != inference.FinishStop:
			t.Fatalf("%s: %d tokens with finish %q, want stop", req.ID, n, finish)
		case n == 200 && finish != inference.FinishLength:
			t.Fatalf("%s: capped output with finish %q, want length", req.ID, finish)
		}
		if finish == inference.FinishStop {
			stopped++
		}
	}
	if stopped == 0 {
		t.Fatal("no request stopped early with mean 20 and max 200")
	}
	// Same request ID → same length (deterministic for debugging and tests).
	req := request(1, 200)
	req.ID = "req-7"
	l, _ := w.AcquireLease(context.Background(), req)
	if n := len(drain(t, l)); n != lengths["req-7"] {
		t.Fatalf("non-deterministic length: %d then %d", lengths["req-7"], n)
	}
}

func TestITLGrowsWithLoad(t *testing.T) {
	w := newWorker(t, func(c *Config) {
		c.InterTokenLatency = 10 * time.Millisecond
		c.ITLLoadFactor = 0.5
	})
	if got := w.interTokenLatency(); got != 10*time.Millisecond {
		t.Fatalf("idle ITL = %v", got)
	}
	var leases []inference.Lease
	for i := 0; i < 3; i++ {
		l, err := w.AcquireLease(context.Background(), request(1, 1))
		if err != nil {
			t.Fatal(err)
		}
		leases = append(leases, l)
	}
	// 3 active leases: 10ms × (1 + 0.5 × 2) = 20ms.
	if got := w.interTokenLatency(); got != 20*time.Millisecond {
		t.Fatalf("ITL with 3 leases = %v, want 20ms", got)
	}
	for _, l := range leases {
		l.Release()
	}
}

func TestCapacityChangedSignal(t *testing.T) {
	w := newWorker(t, nil)
	select {
	case <-w.CapacityChanged():
		t.Fatal("signal before any release")
	default:
	}
	l1, _ := w.AcquireLease(context.Background(), request(1, 1))
	l2, _ := w.AcquireLease(context.Background(), request(1, 1))
	l1.Release()
	l2.Release() // coalesced with the first one
	l2.Release() // idempotent: no new free, no new signal
	select {
	case <-w.CapacityChanged():
	default:
		t.Fatal("no signal after releases")
	}
	select {
	case <-w.CapacityChanged():
		t.Fatal("signals were not coalesced")
	default:
	}

	// A finished stream and an expired lease also signal.
	l3, _ := w.AcquireLease(context.Background(), request(1, 2))
	drain(t, l3)
	select {
	case <-w.CapacityChanged():
	default:
		t.Fatal("no signal after stream end")
	}
	e := newWorker(t, func(c *Config) { c.LeaseTTL = 5 * time.Millisecond })
	if _, err := e.AcquireLease(context.Background(), request(1, 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.CapacityChanged():
	case <-time.After(2 * time.Second):
		t.Fatal("no signal after lease expiry")
	}
}
