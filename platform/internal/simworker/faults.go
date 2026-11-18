package simworker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"

	"llm-inference-platform/internal/inference"
)

// Faults injects the failures of a real inference server, so that the
// control plane's health checks, circuit breaker and retries (Phase 4) have
// something to detect. The zero value injects nothing.
//
// ExecuteStream reads the faults once, when it starts: changing them with
// SetFaults affects new streams, not the ones already running (Restart is
// the way to cut those).
type Faults struct {
	// ErrorRate is the fraction of streams that fail at start with
	// inference.ErrWorkerFailure (connection refused, engine 5xx). The draw
	// is seeded by the request ID, so it is reproducible.
	ErrorRate float64 `json:"error_rate,omitempty"`
	// Down makes every stream fail at start and every health check fail
	// (process down). AcquireLease still works: with vLLM the ledger is in
	// the control plane, so a dead worker is only seen when it is called.
	Down bool `json:"down,omitempty"`
	// Hang makes streams emit nothing until cancelled (stuck GPU). Health
	// checks still pass, as a stuck engine's HTTP server may still answer:
	// only the control plane's TTFT timeout can detect it.
	Hang bool `json:"hang,omitempty"`
	// StallAfterTokens > 0 makes streams stop emitting, without failing,
	// after that many tokens (a hang in the middle of decode).
	StallAfterTokens int `json:"stall_after_tokens,omitempty"`
	// FailAfterTokens > 0 makes streams fail with ErrWorkerFailure after
	// that many tokens (connection cut mid-stream).
	FailAfterTokens int `json:"fail_after_tokens,omitempty"`
	// Slowdown > 1 multiplies TTFT and inter-token latency (thermal
	// throttling, a noisy neighbour).
	Slowdown float64 `json:"slowdown,omitempty"`
}

func (f Faults) validate() error {
	switch {
	case f.ErrorRate < 0 || f.ErrorRate > 1:
		return fmt.Errorf("simworker: error rate must be in [0, 1], got %v", f.ErrorRate)
	case f.StallAfterTokens < 0 || f.FailAfterTokens < 0 || f.Slowdown < 0:
		return errors.New("simworker: fault knobs must not be negative")
	}
	return nil
}

// SetFaults replaces the injected faults for new streams and health checks.
func (w *Worker) SetFaults(f Faults) error {
	if err := f.validate(); err != nil {
		return err
	}
	w.mu.Lock()
	w.faults = f
	w.mu.Unlock()
	return nil
}

// Faults returns the injected faults.
func (w *Worker) Faults() Faults {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.faults
}

// Restart simulates a restart of the inference server: running streams fail
// with ErrWorkerFailure, the prefix cache is emptied and the instance ID
// changes. Reserved capacity is freed as the cut streams return. Injected
// faults are kept.
func (w *Worker) Restart() {
	w.mu.Lock()
	w.instance++
	close(w.restarted)
	w.restarted = make(chan struct{})
	w.mu.Unlock()
	w.cache.clear()
}

// Health implements inference.Worker: it fails while the worker is Down.
func (w *Worker) Health(ctx context.Context) (inference.HealthStatus, error) {
	if err := ctx.Err(); err != nil {
		return inference.HealthStatus{}, err
	}
	w.mu.Lock()
	down, instance := w.faults.Down, w.instance
	w.mu.Unlock()
	if down {
		return inference.HealthStatus{}, fmt.Errorf("%s: %w: down", w.cfg.ID, inference.ErrWorkerFailure)
	}
	return inference.HealthStatus{InstanceID: fmt.Sprintf("%s#%d", w.cfg.ID, instance)}, nil
}

// streamFaults is what one stream sees: the faults when it started and the
// channel that Restart closes.
func (w *Worker) streamFaults() (Faults, <-chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.faults, w.restarted
}

// failsAtStart draws the ErrorRate fault for a request, deterministically.
func failsAtStart(f Faults, seed uint32) bool {
	if f.ErrorRate <= 0 {
		return false
	}
	r := rand.New(rand.NewPCG(uint64(seed), 0xfa17))
	return r.Float64() < f.ErrorRate
}

func (w *Worker) failure(reason string) error {
	return fmt.Errorf("%s: %w: %s", w.cfg.ID, inference.ErrWorkerFailure, reason)
}
