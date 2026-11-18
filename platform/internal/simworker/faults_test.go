package simworker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"llm-inference-platform/internal/inference"
)

// run executes one request on w and returns the chunks and the stream error.
func run(t *testing.T, ctx context.Context, w *Worker, r inference.InferenceRequest) ([]inference.TokenChunk, error) {
	t.Helper()
	l, err := w.AcquireLease(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()
	chunks, wait := inference.StartStream(ctx, l, 0)
	var got []inference.TokenChunk
	for c := range chunks {
		got = append(got, c)
	}
	return got, wait()
}

func TestFaultsValidate(t *testing.T) {
	for i, f := range []Faults{{ErrorRate: -0.1}, {ErrorRate: 1.1}, {FailAfterTokens: -1}, {StallAfterTokens: -1}, {Slowdown: -1}} {
		if _, err := New(Config{ID: "w", Models: []string{model}, TokenBudget: 10, Faults: f}); err == nil {
			t.Errorf("faults %d: New accepted %+v", i, f)
		}
		if err := newWorker(t, nil).SetFaults(f); err == nil {
			t.Errorf("faults %d: SetFaults accepted %+v", i, f)
		}
	}
}

func TestDownFailsStreamsAndHealth(t *testing.T) {
	w := newWorker(t, nil)
	if _, err := w.Health(context.Background()); err != nil {
		t.Fatalf("healthy worker: %v", err)
	}
	if err := w.SetFaults(Faults{Down: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Health(context.Background()); !errors.Is(err, inference.ErrWorkerFailure) {
		t.Fatalf("health while down: %v", err)
	}
	got, err := run(t, context.Background(), w, request(10, 5))
	if len(got) != 0 || !errors.Is(err, inference.ErrWorkerFailure) {
		t.Fatalf("down stream: %d chunks, err %v", len(got), err)
	}
	if snap := w.Snapshot(); snap.TokensReserved != 0 {
		t.Fatalf("capacity leaked: %+v", snap)
	}
	_ = w.SetFaults(Faults{})
	if _, err := run(t, context.Background(), w, request(10, 5)); err != nil {
		t.Fatalf("after recovery: %v", err)
	}
}

func TestErrorRateIsSeededByRequestID(t *testing.T) {
	w := newWorker(t, func(c *Config) { c.TokenBudget = 1000; c.Faults = Faults{ErrorRate: 0.5} })
	failed := map[string]bool{}
	for i := range 200 {
		r := request(1, 1)
		r.ID = fmt.Sprint("req-", i)
		_, err := run(t, context.Background(), w, r)
		failed[r.ID] = err != nil
		if err != nil && !errors.Is(err, inference.ErrWorkerFailure) {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	n := 0
	for _, f := range failed {
		if f {
			n++
		}
	}
	if n < 70 || n > 130 {
		t.Fatalf("%d of 200 failed with error rate 0.5", n)
	}
	r := request(1, 1)
	r.ID = "req-7"
	if _, err := run(t, context.Background(), w, r); (err != nil) != failed["req-7"] {
		t.Fatal("the same request ID must draw the same outcome")
	}
}

func TestHangEmitsNothingUntilCancelled(t *testing.T) {
	w := newWorker(t, func(c *Config) { c.Faults = Faults{Hang: true} })
	if _, err := w.Health(context.Background()); err != nil {
		t.Fatalf("a hung worker still answers health checks: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	got, err := run(t, ctx, w, request(10, 5))
	if len(got) != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hang: %d chunks, err %v", len(got), err)
	}
}

func TestFailAndStallAfterTokens(t *testing.T) {
	w := newWorker(t, func(c *Config) { c.Faults = Faults{FailAfterTokens: 3} })
	got, err := run(t, context.Background(), w, request(10, 10))
	if len(got) != 3 || !errors.Is(err, inference.ErrWorkerFailure) {
		t.Fatalf("fail after 3: %d chunks, err %v", len(got), err)
	}

	_ = w.SetFaults(Faults{StallAfterTokens: 2})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	got, err = run(t, ctx, w, request(10, 10))
	if len(got) != 2 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stall after 2: %d chunks, err %v", len(got), err)
	}
}

func TestSlowdownMultipliesLatency(t *testing.T) {
	w := newWorker(t, func(c *Config) { c.BaseTTFT = 20 * time.Millisecond; c.Faults = Faults{Slowdown: 4} })
	start := time.Now()
	if _, err := run(t, context.Background(), w, request(1, 1)); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 75*time.Millisecond {
		t.Fatalf("TTFT %v, want >= 80ms (4 × 20ms)", d)
	}
}

func TestRestartCutsStreamsAndChangesInstance(t *testing.T) {
	w := newWorker(t, func(c *Config) { c.InterTokenLatency = time.Hour; c.PrefixCacheBlocks = 10 })
	before, _ := w.Health(context.Background())

	r := request(16, 5)
	r.PrefixHashes = []uint64{1}
	l, err := w.AcquireLease(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()
	chunks, wait := inference.StartStream(context.Background(), l, 0)
	<-chunks // first token: now waiting an hour for the second
	if snap := w.Snapshot(); snap.PrefixCacheUsed != 1 {
		t.Fatalf("cache used = %d, want 1", snap.PrefixCacheUsed)
	}

	w.Restart()
	for range chunks {
	}
	if err := wait(); !errors.Is(err, inference.ErrWorkerFailure) {
		t.Fatalf("stream after restart: %v", err)
	}
	after, _ := w.Health(context.Background())
	if after.InstanceID == before.InstanceID {
		t.Fatalf("instance ID did not change: %q", after.InstanceID)
	}
	if snap := w.Snapshot(); snap.TokensReserved != 0 || snap.PrefixCacheUsed != 0 {
		t.Fatalf("after restart: %+v", snap)
	}
}

func TestHealthHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := newWorker(t, nil).Health(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("health with cancelled ctx: %v", err)
	}
}
