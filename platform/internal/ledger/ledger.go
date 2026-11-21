// Package ledger is the capacity bookkeeping shared by every worker
// implementation (Phase 6a, docs/servicio/13-fase-6a-decisiones.md,
// decision 3): a token budget guarded by a mutex, atomic lease acquisition,
// the lease state machine with its TTL, and the coalesced CapacityChanged
// signal.
//
// A worker implementation (the simulator, the vLLM adapter) only supplies
// an Executor, the function that runs one stream; the Ledger wraps it in a
// lease that honours the inference.Lease contract (docs/servicio/03).
package ledger

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"llm-inference-platform/internal/inference"
)

// Config describes the bookkeeping of one worker.
type Config struct {
	ID     string
	Models []string
	// TokenBudget is the worker's capacity in tokens (its KV cache).
	TokenBudget int
	// MaxRequestTokens is the largest prompt + max_tokens a single request
	// may have (vLLM's max_model_len); 0 means TokenBudget.
	MaxRequestTokens int
	// LeaseTTL is how long a lease may wait before ExecuteStream starts;
	// zero disables expiry.
	LeaseTTL time.Duration
}

// Executor runs one admitted request, sending its chunks to out. It must
// honour ctx and must not close out. The ledger frees the reservation as
// soon as it returns.
type Executor func(ctx context.Context, req inference.InferenceRequest, out chan<- inference.TokenChunk) error

// Ledger is safe for concurrent use.
type Ledger struct {
	cfg      Config
	models   map[string]struct{}
	maxReq   int
	capacity chan struct{} // buffer 1: CapacityChanged signal

	mu       sync.Mutex
	reserved int
	active   int
	granted  uint64
	rejected uint64
}

// New validates cfg and returns a ledger.
func New(cfg Config) (*Ledger, error) {
	switch {
	case cfg.ID == "":
		return nil, errors.New("ledger: empty worker ID")
	case len(cfg.Models) == 0:
		return nil, errors.New("ledger: no models")
	case cfg.TokenBudget <= 0:
		return nil, fmt.Errorf("ledger: token budget must be positive, got %d", cfg.TokenBudget)
	case cfg.MaxRequestTokens < 0 || cfg.LeaseTTL < 0:
		return nil, errors.New("ledger: max request tokens and lease TTL must not be negative")
	}
	cfg.Models = append([]string(nil), cfg.Models...)
	models := make(map[string]struct{}, len(cfg.Models))
	for _, m := range cfg.Models {
		models[m] = struct{}{}
	}
	maxReq := cfg.TokenBudget
	if cfg.MaxRequestTokens > 0 {
		maxReq = min(cfg.MaxRequestTokens, cfg.TokenBudget)
	}
	return &Ledger{cfg: cfg, models: models, maxReq: maxReq, capacity: make(chan struct{}, 1)}, nil
}

func (l *Ledger) ID() string { return l.cfg.ID }

func (l *Ledger) Models() []string { return append([]string(nil), l.cfg.Models...) }

// CapacityChanged fires (coalesced) every time a lease gives its tokens back.
func (l *Ledger) CapacityChanged() <-chan struct{} { return l.capacity }

// Active returns the number of leases holding capacity now.
func (l *Ledger) Active() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.active
}

// Acquire reserves req.TokenCost() tokens, or fails without side effects.
// The returned lease runs exec when executed.
func (l *Ledger) Acquire(ctx context.Context, req inference.InferenceRequest, exec Executor) (inference.Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, ok := l.models[req.Model]; !ok {
		return nil, fmt.Errorf("%s: %w: %q", l.cfg.ID, inference.ErrModelNotServed, req.Model)
	}
	if req.PromptTokens < 0 || req.MaxNewTokens <= 0 {
		return nil, fmt.Errorf("%w: prompt_tokens=%d max_new_tokens=%d",
			inference.ErrInvalidRequest, req.PromptTokens, req.MaxNewTokens)
	}
	cost := req.TokenCost()
	if cost > l.maxReq {
		return nil, fmt.Errorf("%s: %w: cost %d > max %d", l.cfg.ID, inference.ErrRequestTooLarge, cost, l.maxReq)
	}

	l.mu.Lock()
	if l.reserved+cost > l.cfg.TokenBudget {
		l.rejected++
		l.mu.Unlock()
		return nil, fmt.Errorf("%s: %w", l.cfg.ID, inference.ErrAtCapacity)
	}
	l.reserved += cost
	l.active++
	l.granted++
	l.mu.Unlock()

	ls := &lease{l: l, req: req, cost: cost, exec: exec}
	if l.cfg.LeaseTTL > 0 {
		ls.expiry = time.AfterFunc(l.cfg.LeaseTTL, ls.expire)
	}
	return ls, nil
}

// Snapshot returns the bookkeeping part of a worker snapshot; workers add
// their own fields (prefix cache, engine metrics).
func (l *Ledger) Snapshot() inference.WorkerSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := inference.WorkerSnapshot{
		WorkerID:       l.cfg.ID,
		Models:         l.Models(),
		TokenBudget:    l.cfg.TokenBudget,
		TokensReserved: l.reserved,
		ActiveLeases:   l.active,
		LeasesGranted:  l.granted,
		LeasesRejected: l.rejected,
	}
	if l.maxReq < l.cfg.TokenBudget {
		s.MaxRequestTokens = l.maxReq
	}
	return s
}

func (l *Ledger) free(cost int) {
	l.mu.Lock()
	l.reserved -= cost
	l.active--
	l.mu.Unlock()
	// Non-blocking send on a buffer of 1: many frees before the dispatcher
	// wakes up collapse into a single signal.
	select {
	case l.capacity <- struct{}{}:
	default:
	}
}

type leaseState int

const (
	stateAcquired leaseState = iota
	stateExecuting
	stateReleased
)

type lease struct {
	l      *Ledger
	req    inference.InferenceRequest
	cost   int
	exec   Executor
	expiry *time.Timer

	mu       sync.Mutex
	state    leaseState
	executed bool
	expired  bool
}

func (ls *lease) WorkerID() string { return ls.l.cfg.ID }

// begin moves the lease to the executing state, or says why it cannot.
func (ls *lease) begin() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	switch {
	case ls.executed:
		return inference.ErrLeaseUsed
	case ls.state == stateReleased && ls.expired:
		return inference.ErrLeaseExpired
	case ls.state == stateReleased:
		return inference.ErrLeaseReleased
	}
	ls.state = stateExecuting
	ls.executed = true
	if ls.expiry != nil {
		ls.expiry.Stop()
	}
	return nil
}

// Release frees the reserved capacity exactly once.
func (ls *lease) Release() {
	ls.mu.Lock()
	if ls.state == stateReleased {
		ls.mu.Unlock()
		return
	}
	ls.state = stateReleased
	if ls.expiry != nil {
		ls.expiry.Stop()
	}
	ls.mu.Unlock()
	ls.l.free(ls.cost)
}

// expire reclaims the capacity of a lease that was never executed.
func (ls *lease) expire() {
	ls.mu.Lock()
	if ls.state != stateAcquired {
		ls.mu.Unlock()
		return
	}
	ls.state = stateReleased
	ls.expired = true
	ls.mu.Unlock()
	ls.l.free(ls.cost)
}

// ExecuteStream runs the executor once and frees the capacity as soon as it
// returns, whatever the reason.
func (ls *lease) ExecuteStream(ctx context.Context, out chan<- inference.TokenChunk) error {
	if err := ls.begin(); err != nil {
		return err
	}
	defer ls.Release()
	return ls.exec(ctx, ls.req, out)
}
