// Package workertest is the contract battery every inference.Worker
// implementation must pass (Phase 6a, docs/servicio/13-fase-6a-decisiones.md,
// decision 3): the simulator and the vLLM adapter run the same tests, so the
// scheduler can treat them alike.
package workertest

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

// Options describe the worker a test needs.
type Options struct {
	Model       string
	TokenBudget int
	// TokenDelay is the delay between generated tokens; streams generate
	// exactly the request's MaxNewTokens tokens.
	TokenDelay time.Duration
	LeaseTTL   time.Duration
}

// Factory builds a fresh worker serving opts.Model. Cleanup is the
// factory's job (t.Cleanup).
type Factory func(t *testing.T, opts Options) inference.Worker

const model = "contract-model"

func request(id string, prompt, maxNew int) inference.InferenceRequest {
	return inference.InferenceRequest{ID: id, Model: model, Prompt: "contract test prompt", PromptTokens: prompt, MaxNewTokens: maxNew}
}

func reserved(w inference.Worker) int { return w.Snapshot().TokensReserved }

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func drain(t *testing.T, ctx context.Context, l inference.Lease) ([]inference.TokenChunk, error) {
	t.Helper()
	chunks, wait := inference.StartStream(ctx, l, 0)
	var got []inference.TokenChunk
	for c := range chunks {
		got = append(got, c)
	}
	return got, wait()
}

// Run runs the whole battery.
func Run(t *testing.T, newWorker Factory) {
	t.Run("admission", func(t *testing.T) { admission(t, newWorker) })
	t.Run("stream completes and frees capacity", func(t *testing.T) { completes(t, newWorker) })
	t.Run("lease used once", func(t *testing.T) { usedOnce(t, newWorker) })
	t.Run("cancellation frees capacity at once", func(t *testing.T) { cancellation(t, newWorker) })
	t.Run("lease TTL", func(t *testing.T) { ttl(t, newWorker) })
	t.Run("capacity signal", func(t *testing.T) { signal(t, newWorker) })
	t.Run("concurrent acquire never exceeds budget", func(t *testing.T) { concurrent(t, newWorker) })
}

func admission(t *testing.T, newWorker Factory) {
	w := newWorker(t, Options{Model: model, TokenBudget: 100})
	ctx := context.Background()
	l1, err := w.AcquireLease(ctx, request("a", 40, 20))
	if err != nil {
		t.Fatal(err)
	}
	if got := reserved(w); got != 60 {
		t.Fatalf("reserved = %d, want 60", got)
	}
	l2, err := w.AcquireLease(ctx, request("b", 30, 10)) // exactly fits
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AcquireLease(ctx, request("c", 1, 1)); !errors.Is(err, inference.ErrAtCapacity) {
		t.Fatalf("full worker: %v", err)
	}
	if _, err := w.AcquireLease(ctx, request("d", 100, 1)); !errors.Is(err, inference.ErrRequestTooLarge) {
		t.Fatalf("too large: %v", err)
	}
	other := request("e", 1, 1)
	other.Model = "other-model"
	if _, err := w.AcquireLease(ctx, other); !errors.Is(err, inference.ErrModelNotServed) {
		t.Fatalf("unknown model: %v", err)
	}
	if _, err := w.AcquireLease(ctx, request("f", 1, 0)); !errors.Is(err, inference.ErrInvalidRequest) {
		t.Fatalf("invalid: %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := w.AcquireLease(cancelled, request("g", 1, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context: %v", err)
	}
	l1.Release()
	l1.Release() // idempotent
	l2.Release()
	if got := reserved(w); got != 0 {
		t.Fatalf("reserved after release = %d", got)
	}
	if snap := w.Snapshot(); snap.ActiveLeases != 0 || snap.TokenBudget != 100 || snap.WorkerID != w.ID() {
		t.Fatalf("snapshot = %+v", snap)
	}
}

func completes(t *testing.T, newWorker Factory) {
	w := newWorker(t, Options{Model: model, TokenBudget: 100})
	l, err := w.AcquireLease(context.Background(), request("a", 10, 4))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()
	if l.WorkerID() != w.ID() {
		t.Fatalf("lease worker %q, want %q", l.WorkerID(), w.ID())
	}
	chunks, err := drain(t, context.Background(), l)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if len(chunks) == 0 || chunks[len(chunks)-1].FinishReason == inference.FinishNone {
		t.Fatalf("chunks = %+v: want at least one, the last with a finish reason", chunks)
	}
	text := ""
	for _, c := range chunks {
		text += c.Text
	}
	if text == "" {
		t.Fatal("no text generated")
	}
	waitFor(t, "capacity freed", func() bool { return reserved(w) == 0 })
}

func usedOnce(t *testing.T, newWorker Factory) {
	w := newWorker(t, Options{Model: model, TokenBudget: 100})
	l, err := w.AcquireLease(context.Background(), request("a", 10, 2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := drain(t, context.Background(), l); err != nil {
		t.Fatal(err)
	}
	if _, err := drain(t, context.Background(), l); !errors.Is(err, inference.ErrLeaseUsed) {
		t.Fatalf("second execution: %v", err)
	}
	l2, err := w.AcquireLease(context.Background(), request("b", 10, 2))
	if err != nil {
		t.Fatal(err)
	}
	l2.Release()
	if _, err := drain(t, context.Background(), l2); !errors.Is(err, inference.ErrLeaseReleased) {
		t.Fatalf("execution after release: %v", err)
	}
}

func cancellation(t *testing.T, newWorker Factory) {
	w := newWorker(t, Options{Model: model, TokenBudget: 100, TokenDelay: time.Hour})
	l, err := w.AcquireLease(context.Background(), request("a", 10, 5))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()
	ctx, cancel := context.WithCancel(context.Background())
	chunks, wait := inference.StartStream(ctx, l, 0)
	<-chunks // first token; the second is an hour away
	cancel()
	for range chunks {
	}
	if err := wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled stream: %v", err)
	}
	waitFor(t, "capacity freed after cancel", func() bool { return reserved(w) == 0 })
}

func ttl(t *testing.T, newWorker Factory) {
	w := newWorker(t, Options{Model: model, TokenBudget: 100, LeaseTTL: 20 * time.Millisecond})
	l, err := w.AcquireLease(context.Background(), request("a", 10, 2))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "lease expiry", func() bool { return reserved(w) == 0 })
	if _, err := drain(t, context.Background(), l); !errors.Is(err, inference.ErrLeaseExpired) {
		t.Fatalf("expired lease: %v", err)
	}
	l.Release() // must not free twice
	if got := reserved(w); got != 0 {
		t.Fatalf("reserved = %d after double free attempt", got)
	}
}

func signal(t *testing.T, newWorker Factory) {
	w := newWorker(t, Options{Model: model, TokenBudget: 100})
	select { // drain a stale signal, if any
	case <-w.CapacityChanged():
	default:
	}
	l, err := w.AcquireLease(context.Background(), request("a", 10, 2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := drain(t, context.Background(), l); err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.CapacityChanged():
	case <-time.After(5 * time.Second):
		t.Fatal("no CapacityChanged after a stream ended")
	}
}

func concurrent(t *testing.T, newWorker Factory) {
	const budget = 200
	w := newWorker(t, Options{Model: model, TokenBudget: budget, TokenDelay: time.Millisecond})
	var wg sync.WaitGroup
	var over, ok atomic.Int64
	for i := range 60 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := w.AcquireLease(context.Background(), request(fmt.Sprint("r", i), 20, 5))
			if err != nil {
				if !errors.Is(err, inference.ErrAtCapacity) {
					t.Errorf("acquire: %v", err)
				}
				return
			}
			defer l.Release()
			if reserved(w) > budget {
				over.Add(1)
			}
			if _, err := drain(t, context.Background(), l); err != nil {
				t.Errorf("stream: %v", err)
				return
			}
			ok.Add(1)
		}()
	}
	wg.Wait()
	if over.Load() > 0 {
		t.Fatalf("budget exceeded %d times", over.Load())
	}
	if ok.Load() == 0 {
		t.Fatal("no stream completed")
	}
	waitFor(t, "all capacity freed", func() bool { return reserved(w) == 0 })
}
