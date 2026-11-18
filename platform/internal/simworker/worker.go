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
}

// Worker is a simulated inference worker. It is safe for concurrent use.
type Worker struct {
	cfg      Config
	models   map[string]struct{}
	capacity chan struct{} // buffer 1: CapacityChanged signal
	cache    *prefixCache  // nil when PrefixCacheBlocks is 0

	mu       sync.Mutex
	reserved int
	active   int
	granted  uint64
	rejected uint64
}

var _ inference.Worker = (*Worker)(nil)

// New validates cfg and returns a worker.
func New(cfg Config) (*Worker, error) {
	switch {
	case cfg.ID == "":
		return nil, errors.New("simworker: empty ID")
	case len(cfg.Models) == 0:
		return nil, errors.New("simworker: no models")
	case cfg.TokenBudget <= 0:
		return nil, fmt.Errorf("simworker: token budget must be positive, got %d", cfg.TokenBudget)
	case cfg.BaseTTFT < 0 || cfg.PrefillPerToken < 0 || cfg.InterTokenLatency < 0 || cfg.LeaseTTL < 0:
		return nil, errors.New("simworker: durations must not be negative")
	case cfg.MeanOutputTokens < 0 || cfg.ITLLoadFactor < 0 || cfg.PrefixCacheBlocks < 0:
		return nil, errors.New("simworker: output mean, ITL load factor and prefix cache size must not be negative")
	}
	models := make(map[string]struct{}, len(cfg.Models))
	for _, m := range cfg.Models {
		models[m] = struct{}{}
	}
	cfg.Models = append([]string(nil), cfg.Models...)
	w := &Worker{cfg: cfg, models: models, capacity: make(chan struct{}, 1)}
	if cfg.PrefixCacheBlocks > 0 {
		w.cache = newPrefixCache(cfg.PrefixCacheBlocks)
	}
	return w, nil
}

func (w *Worker) ID() string { return w.cfg.ID }

func (w *Worker) Models() []string { return append([]string(nil), w.cfg.Models...) }

// AcquireLease reserves req.TokenCost() tokens of the budget, or fails without
// side effects.
func (w *Worker) AcquireLease(ctx context.Context, req inference.InferenceRequest) (inference.Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, ok := w.models[req.Model]; !ok {
		return nil, fmt.Errorf("%s: %w: %q", w.cfg.ID, inference.ErrModelNotServed, req.Model)
	}
	if req.PromptTokens < 0 || req.MaxNewTokens <= 0 {
		return nil, fmt.Errorf("%w: prompt_tokens=%d max_new_tokens=%d",
			inference.ErrInvalidRequest, req.PromptTokens, req.MaxNewTokens)
	}
	cost := req.TokenCost()
	if cost > w.cfg.TokenBudget {
		return nil, fmt.Errorf("%s: %w: cost %d > budget %d", w.cfg.ID, inference.ErrRequestTooLarge, cost, w.cfg.TokenBudget)
	}

	w.mu.Lock()
	if w.reserved+cost > w.cfg.TokenBudget {
		w.rejected++
		w.mu.Unlock()
		return nil, fmt.Errorf("%s: %w", w.cfg.ID, inference.ErrAtCapacity)
	}
	w.reserved += cost
	w.active++
	w.granted++
	w.mu.Unlock()

	l := &lease{w: w, req: req, cost: cost}
	if w.cfg.LeaseTTL > 0 {
		l.expiry = time.AfterFunc(w.cfg.LeaseTTL, l.expire)
	}
	return l, nil
}

// Snapshot returns the current state of the worker.
func (w *Worker) Snapshot() inference.WorkerSnapshot {
	cacheCap, cacheUsed, queried, hit := w.cache.stats()
	w.mu.Lock()
	defer w.mu.Unlock()
	return inference.WorkerSnapshot{
		WorkerID:            w.cfg.ID,
		Models:              w.Models(),
		TokenBudget:         w.cfg.TokenBudget,
		TokensReserved:      w.reserved,
		ActiveLeases:        w.active,
		LeasesGranted:       w.granted,
		LeasesRejected:      w.rejected,
		PrefixCacheBlocks:   cacheCap,
		PrefixCacheUsed:     cacheUsed,
		PrefixBlocksQueried: queried,
		PrefixBlocksHit:     hit,
	}
}

// CapacityChanged fires (coalesced) every time a lease gives its tokens back.
func (w *Worker) CapacityChanged() <-chan struct{} { return w.capacity }

func (w *Worker) free(cost int) {
	w.mu.Lock()
	w.reserved -= cost
	w.active--
	w.mu.Unlock()
	// Non-blocking send on a buffer of 1: many frees before the dispatcher
	// wakes up collapse into a single signal.
	select {
	case w.capacity <- struct{}{}:
	default:
	}
}

// interTokenLatency returns the ITL for the current load (see ITLLoadFactor).
func (w *Worker) interTokenLatency() time.Duration {
	base := w.cfg.InterTokenLatency
	if w.cfg.ITLLoadFactor == 0 || base == 0 {
		return base
	}
	w.mu.Lock()
	active := w.active
	w.mu.Unlock()
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

type leaseState int

const (
	stateAcquired leaseState = iota
	stateExecuting
	stateReleased
)

type lease struct {
	w      *Worker
	req    inference.InferenceRequest
	cost   int
	expiry *time.Timer

	mu       sync.Mutex
	state    leaseState
	executed bool
	expired  bool
}

func (l *lease) WorkerID() string { return l.w.cfg.ID }

// begin moves the lease to the executing state, or says why it cannot.
func (l *lease) begin() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch {
	case l.executed:
		return inference.ErrLeaseUsed
	case l.state == stateReleased && l.expired:
		return inference.ErrLeaseExpired
	case l.state == stateReleased:
		return inference.ErrLeaseReleased
	}
	l.state = stateExecuting
	l.executed = true
	if l.expiry != nil {
		l.expiry.Stop()
	}
	return nil
}

// Release frees the reserved capacity exactly once.
func (l *lease) Release() {
	l.mu.Lock()
	if l.state == stateReleased {
		l.mu.Unlock()
		return
	}
	l.state = stateReleased
	if l.expiry != nil {
		l.expiry.Stop()
	}
	l.mu.Unlock()
	l.w.free(l.cost)
}

// expire reclaims the capacity of a lease that was never executed.
func (l *lease) expire() {
	l.mu.Lock()
	if l.state != stateAcquired {
		l.mu.Unlock()
		return
	}
	l.state = stateReleased
	l.expired = true
	l.mu.Unlock()
	l.w.free(l.cost)
}

// ExecuteStream simulates prefill (TTFT, without the cached prefix blocks)
// then decodes up to MaxNewTokens tokens (fewer when MeanOutputTokens is
// set).
func (l *lease) ExecuteStream(ctx context.Context, out chan<- inference.TokenChunk) error {
	if err := l.begin(); err != nil {
		return err
	}
	// Free the simulated VRAM as soon as generation stops, whatever the reason.
	defer l.Release()

	cfg := l.w.cfg
	cached := min(l.w.cache.use(l.req.PrefixHashes)*prefix.BlockTokens, l.req.PromptTokens)
	ttft := cfg.BaseTTFT + time.Duration(l.req.PromptTokens-cached)*cfg.PrefillPerToken
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()

	seed := seedFor(l.req.ID)
	n := l.w.outputTokens(seed, l.req.MaxNewTokens)
	finish := inference.FinishLength
	if n < l.req.MaxNewTokens {
		finish = inference.FinishStop
	}
	last := n - 1
	for i := 0; i <= last; i++ {
		delay := ttft
		if i > 0 {
			delay = l.w.interTokenLatency()
		}
		if delay > 0 {
			timer.Reset(delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
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
