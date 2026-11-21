// Package simworker implements a simulated GPU worker: a token budget that
// stands in for KV-cache VRAM, and a streaming generator with artificial
// prefill and inter-token latencies.
package simworker

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/ledger"
	"llm-inference-platform/internal/prefix"
)

// Config describes one simulated worker.
type Config struct {
	ID     string
	Models []string
	// TokenBudget is the simulated KV-cache capacity, in tokens.
	TokenBudget int
	// BaseTTFT is the fixed latency before the first token.
	BaseTTFT time.Duration
	// PrefillPerToken is added to the TTFT for every prompt token.
	PrefillPerToken time.Duration
	// InterTokenLatency is the delay between two generated tokens.
	InterTokenLatency time.Duration
	// LeaseTTL is how long a lease may wait before ExecuteStream starts.
	// Zero disables expiry.
	LeaseTTL time.Duration
	// MeanOutputTokens makes the output length realistic: when positive, each
	// request generates a pseudo-random number of tokens drawn from an
	// exponential distribution with this mean, seeded by the request ID and
	// capped at MaxNewTokens. A request that stops before MaxNewTokens ends
	// with finish_reason "stop". Zero always generates MaxNewTokens.
	MeanOutputTokens int
	// ITLLoadFactor makes decode slower under load, like continuous batching:
	// ITL = InterTokenLatency × (1 + ITLLoadFactor × (active leases − 1)).
	// Zero keeps the ITL constant.
	ITLLoadFactor float64
	// PrefixCacheBlocks enables a simulated prefix cache of this many blocks
	// (prefix.BlockTokens tokens each). A request whose first k PrefixHashes
	// are cached skips their prefill: TTFT = BaseTTFT + PrefillPerToken ×
	// uncached prompt tokens. Zero disables the cache (every prompt token is
	// prefilled).
	PrefixCacheBlocks int
	// Faults are the failures injected from the start (see Faults); they
	// can be changed later with SetFaults.
	Faults Faults
}

// Worker is a simulated inference worker. Its capacity bookkeeping (budget,
// leases, TTL, CapacityChanged) is the shared internal/ledger (Phase 6a);
// this package only simulates how a stream runs. It is safe for concurrent
// use.
type Worker struct {
	cfg   Config
	led   *ledger.Ledger
	cache *prefixCache // nil when PrefixCacheBlocks is 0

	mu        sync.Mutex
	faults    Faults        // see faults.go
	instance  uint64        // bumped by Restart
	restarted chan struct{} // closed by Restart, then replaced
}

var _ inference.Worker = (*Worker)(nil)

// New validates cfg and returns a worker.
func New(cfg Config) (*Worker, error) {
	switch {
	case cfg.BaseTTFT < 0 || cfg.PrefillPerToken < 0 || cfg.InterTokenLatency < 0:
		return nil, errors.New("simworker: durations must not be negative")
	case cfg.MeanOutputTokens < 0 || cfg.ITLLoadFactor < 0 || cfg.PrefixCacheBlocks < 0:
		return nil, errors.New("simworker: output mean, ITL load factor and prefix cache size must not be negative")
	}
	if err := cfg.Faults.validate(); err != nil {
		return nil, err
	}
	led, err := ledger.New(ledger.Config{ID: cfg.ID, Models: cfg.Models, TokenBudget: cfg.TokenBudget, LeaseTTL: cfg.LeaseTTL})
	if err != nil {
		return nil, fmt.Errorf("simworker: %w", err)
	}
	cfg.Models = led.Models()
	w := &Worker{cfg: cfg, led: led, faults: cfg.Faults, restarted: make(chan struct{})}
	if cfg.PrefixCacheBlocks > 0 {
		w.cache = newPrefixCache(cfg.PrefixCacheBlocks)
	}
	return w, nil
}

func (w *Worker) ID() string { return w.led.ID() }

func (w *Worker) Models() []string { return w.led.Models() }

// AcquireLease reserves req.TokenCost() tokens of the budget, or fails without
// side effects.
func (w *Worker) AcquireLease(ctx context.Context, req inference.InferenceRequest) (inference.Lease, error) {
	return w.led.Acquire(ctx, req, w.execute)
}

// Snapshot returns the current state of the worker.
func (w *Worker) Snapshot() inference.WorkerSnapshot {
	s := w.led.Snapshot()
	s.PrefixCacheBlocks, s.PrefixCacheUsed, s.PrefixBlocksQueried, s.PrefixBlocksHit = w.cache.stats()
	return s
}

// CapacityChanged fires (coalesced) every time a lease gives its tokens back.
func (w *Worker) CapacityChanged() <-chan struct{} { return w.led.CapacityChanged() }

// interTokenLatency returns the ITL for the current load (see ITLLoadFactor).
func (w *Worker) interTokenLatency() time.Duration {
	base := w.cfg.InterTokenLatency
	if w.cfg.ITLLoadFactor == 0 || base == 0 {
		return base
	}
	active := w.led.Active()
	if active <= 1 {
		return base
	}
	return time.Duration(float64(base) * (1 + w.cfg.ITLLoadFactor*float64(active-1)))
}

// outputTokens returns how many tokens this request generates (see
// MeanOutputTokens). It is deterministic for a given request ID.
func (w *Worker) outputTokens(seed uint32, maxNew int) int {
	mean := w.cfg.MeanOutputTokens
	if mean <= 0 {
		return maxNew
	}
	r := rand.New(rand.NewPCG(uint64(seed), 0x9e3779b97f4a7c15))
	n := int(math.Ceil(r.ExpFloat64() * float64(mean)))
	return min(max(n, 1), maxNew)
}

// execute is the ledger.Executor of the simulator: it simulates prefill
// (TTFT, without the cached prefix blocks) then decodes up to MaxNewTokens
// tokens (fewer when MeanOutputTokens is set). The ledger frees the
// reservation when it returns.
func (w *Worker) execute(ctx context.Context, req inference.InferenceRequest, out chan<- inference.TokenChunk) error {
	cfg := w.cfg
	seed := seedFor(req.ID)
	faults, restarted := w.streamFaults()
	switch {
	case faults.Down:
		return w.failure("down")
	case failsAtStart(faults, seed):
		return w.failure("injected error")
	case faults.Hang:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-restarted:
			return w.failure("restarted")
		}
	}
	slow := max(faults.Slowdown, 1)

	cached := min(w.cache.use(req.PrefixHashes)*prefix.BlockTokens, req.PromptTokens)
	ttft := cfg.BaseTTFT + time.Duration(req.PromptTokens-cached)*cfg.PrefillPerToken
	ttft = time.Duration(float64(ttft) * slow)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()

	n := w.outputTokens(seed, req.MaxNewTokens)
	finish := inference.FinishLength
	if n < req.MaxNewTokens {
		finish = inference.FinishStop
	}
	last := n - 1
	for i := 0; i <= last; i++ {
		if i > 0 && i == faults.FailAfterTokens {
			return w.failure(fmt.Sprintf("stream cut after %d tokens", i))
		}
		if i > 0 && i == faults.StallAfterTokens {
			select { // stalled: nothing more until cancelled
			case <-ctx.Done():
				return ctx.Err()
			case <-restarted:
				return w.failure("restarted")
			}
		}
		delay := ttft
		if i > 0 {
			delay = time.Duration(float64(w.interTokenLatency()) * slow)
		}
		if delay > 0 {
			timer.Reset(delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-restarted:
				return w.failure("restarted")
			case <-timer.C:
			}
		} else if err := ctx.Err(); err != nil {
			return err
		}

		chunk := inference.TokenChunk{Index: i, Text: fakeToken(seed, i)}
		if i == last {
			chunk.FinishReason = finish
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-restarted:
			return w.failure("restarted")
		case out <- chunk:
		}
	}
	return nil
}

var vocabulary = []string{
	"the", "model", "token", "cache", "prefix", "lease", "worker", "stream",
	"latency", "budget", "router", "batch", "decode", "prefill", "GPU", "memory",
}

func seedFor(id string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(id))
	return h.Sum32()
}

// fakeToken returns a deterministic pseudo-word for position i.
func fakeToken(seed uint32, i int) string {
	word := vocabulary[(int(seed%997)+i*7)%len(vocabulary)]
	if i == 0 {
		return word
	}
	return " " + word
}
