// Package scheduler admits requests and places them on workers.
//
// It is split in two halves behind a single Acquire method
// (docs/servicio/08-fase-2-decisiones.md, decision 3):
//
//   - Admission decides WHEN a request may try for capacity: one bounded FIFO
//     queue per model, a queue timeout, early rejection (load shedding) and a
//     single dispatcher woken by the workers' CapacityChanged signals.
//   - Placement decides WHERE it runs (see Placement; RoundRobin in Phase 2,
//     prefix affinity in Phase 3).
//
// Queue discipline (decision 4 and conflict C1): FIFO per model with bounded
// skip-ahead. While the head of a model's queue does not fit, the requests
// behind it that do fit may pass it, but only until the head is older than
// HeadMaxAge; from then on a FIFO barrier stops anyone else of that model
// from passing it until it is admitted.
//
// Phase 4 (docs/servicio/10-fase-4-decisiones.md) adds the worker lifecycle
// around both halves: workers are added and removed at run time (removal
// drains), health checks and a circuit breaker per worker decide which
// workers placement may use, slow workers are only used when the others are
// full, and the leases Acquire returns detect hangs and retry once on
// another worker when a worker fails before the first token (registry.go,
// lease.go).
//
// The scheduler assumes a single control-plane replica: the workers' ledgers
// are only consistent if it is the sole entry point to each worker (README
// §9, FI-2).
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"sort"
	"sync"
	"time"

	"llm-inference-platform/internal/inference"
)

// Config tunes admission and the worker lifecycle. Zero fields take the
// DefaultConfig value.
type Config struct {
	// QueueTimeout is the longest a request waits in the queue (capped by
	// the client's own deadline). Interactive traffic only (conflict C3).
	QueueTimeout time.Duration
	// HeadMaxAge is how long the head of a model's queue may be passed by
	// smaller requests before the FIFO barrier kicks in. Default:
	// QueueTimeout / 2.
	HeadMaxAge time.Duration
	// QueueTokenFactor bounds each model's queue: its queued tokens may not
	// exceed this factor × the total token budget of the model's workers
	// (conflict C2).
	QueueTokenFactor float64
	// MaxQueuedRequests caps the waiting requests across all models, to
	// protect the process (goroutines, memory).
	MaxQueuedRequests int
	// MaxScan bounds how many waiting requests of one model the dispatcher
	// tries per wake-up.
	MaxScan int
	// SafetyPoll wakes the dispatcher periodically in case a worker never
	// signals CapacityChanged (and so that open circuit breakers move to
	// half-open on time).
	SafetyPoll time.Duration
	// MinRetryAfter and MaxRetryAfter bound the Retry-After advertised on
	// queue rejections. The value follows the recent queue wait (EWMA) plus
	// up to MinRetryAfter of jitter, so that clients do not retry in sync.
	MinRetryAfter time.Duration
	MaxRetryAfter time.Duration
	// Placement chooses the worker. Default: &RoundRobin{}; cmd/platform
	// uses NewPrefixAffinity (Phase 3).
	Placement Placement

	// ---- Phase 4: health, circuit breaker, hangs, retries ----

	// HealthInterval is how often each worker's Health is polled; a
	// negative value disables health checks. HealthTimeout bounds each
	// check.
	HealthInterval time.Duration
	HealthTimeout  time.Duration
	// HealthFailThreshold consecutive failed checks mark a worker
	// unhealthy; HealthOKThreshold consecutive good ones mark it healthy.
	HealthFailThreshold int
	HealthOKThreshold   int
	// BreakerFailures consecutive worker failures open its circuit breaker.
	BreakerFailures int
	// BreakerCooldown is how long a breaker stays open before letting one
	// probe request through (half-open). It doubles on every reopening, up
	// to BreakerMaxCooldown.
	BreakerCooldown    time.Duration
	BreakerMaxCooldown time.Duration
	// A stream that emits no first token within TTFTTimeout +
	// TTFTTimeoutPerToken × prompt tokens, or no new token for StallTimeout,
	// is cancelled and counted as a worker failure (a hang).
	TTFTTimeout         time.Duration
	TTFTTimeoutPerToken time.Duration
	StallTimeout        time.Duration
	// A worker is degraded, and only used when the others have no room,
	// when its mean inter-token latency exceeds DegradedFactor × the median
	// of its peers. Only workers with at least DegradedMinSamples finished
	// streams take part.
	DegradedFactor     float64
	DegradedMinSamples int
	// MaxRetries is how many times a request that failed before its first
	// token is retried on another worker; a negative value disables
	// retries.
	MaxRetries int
}

// DefaultConfig returns the defaults (Phase 2 admission, Phase 4
// lifecycle).
func DefaultConfig() Config {
	return Config{
		QueueTimeout:      5 * time.Second,
		HeadMaxAge:        2500 * time.Millisecond,
		QueueTokenFactor:  2,
		MaxQueuedRequests: 1024,
		MaxScan:           64,
		SafetyPoll:        time.Second,
		MinRetryAfter:     time.Second,
		MaxRetryAfter:     30 * time.Second,

		HealthInterval:      2 * time.Second,
		HealthTimeout:       time.Second,
		HealthFailThreshold: 3,
		HealthOKThreshold:   2,
		BreakerFailures:     5,
		BreakerCooldown:     10 * time.Second,
		BreakerMaxCooldown:  2 * time.Minute,
		TTFTTimeout:         10 * time.Second,
		TTFTTimeoutPerToken: time.Millisecond,
		StallTimeout:        10 * time.Second,
		DegradedFactor:      2,
		DegradedMinSamples:  20,
		MaxRetries:          1,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	setDur := func(v *time.Duration, def time.Duration) {
		if *v <= 0 {
			*v = def
		}
	}
	setInt := func(v *int, def int) {
		if *v <= 0 {
			*v = def
		}
	}
	setDur(&c.QueueTimeout, d.QueueTimeout)
	if c.HeadMaxAge <= 0 {
		c.HeadMaxAge = c.QueueTimeout / 2
	}
	if c.QueueTokenFactor <= 0 {
		c.QueueTokenFactor = d.QueueTokenFactor
	}
	setInt(&c.MaxQueuedRequests, d.MaxQueuedRequests)
	setInt(&c.MaxScan, d.MaxScan)
	setDur(&c.SafetyPoll, d.SafetyPoll)
	setDur(&c.MinRetryAfter, d.MinRetryAfter)
	if c.MaxRetryAfter < c.MinRetryAfter {
		c.MaxRetryAfter = max(d.MaxRetryAfter, c.MinRetryAfter)
	}
	if c.Placement == nil {
		c.Placement = &RoundRobin{}
	}

	if c.HealthInterval == 0 {
		c.HealthInterval = d.HealthInterval
	}
	setDur(&c.HealthTimeout, d.HealthTimeout)
	setInt(&c.HealthFailThreshold, d.HealthFailThreshold)
	setInt(&c.HealthOKThreshold, d.HealthOKThreshold)
	setInt(&c.BreakerFailures, d.BreakerFailures)
	setDur(&c.BreakerCooldown, d.BreakerCooldown)
	if c.BreakerMaxCooldown < c.BreakerCooldown {
		c.BreakerMaxCooldown = max(d.BreakerMaxCooldown, c.BreakerCooldown)
	}
	setDur(&c.TTFTTimeout, d.TTFTTimeout)
	if c.TTFTTimeoutPerToken < 0 {
		c.TTFTTimeoutPerToken = 0
	} else if c.TTFTTimeoutPerToken == 0 {
		c.TTFTTimeoutPerToken = d.TTFTTimeoutPerToken
	}
	setDur(&c.StallTimeout, d.StallTimeout)
	if c.DegradedFactor <= 1 {
		c.DegradedFactor = d.DegradedFactor
	}
	setInt(&c.DegradedMinSamples, d.DegradedMinSamples)
	if c.MaxRetries == 0 {
		c.MaxRetries = d.MaxRetries
	}
	return c
}

// Scheduler is the control-plane scheduler. Create it with New and stop it
// with Close.
type Scheduler struct {
	cfg Config
	now func() time.Time

	kick chan struct{} // buffer 1: arrivals and membership changes wake the dispatcher
	stop chan struct{}
	wg   sync.WaitGroup

	mu      sync.Mutex
	members map[string]*member
	order   []*member // registration order: deterministic placement input
	version uint64    // bumped on membership changes: the dispatcher rebuilds its select
	models  []*modelQueue
	byModel map[string]*modelQueue
	waiting int
	stats   counters
	waitEMA time.Duration // EWMA of the queue wait of admitted requests
	waits   []time.Duration
	waitPos int
}

type modelQueue struct {
	name string
	// Derived from the active (non-draining) workers serving the model;
	// recomputed on every membership change.
	serving    int
	maxBudget  int // largest single worker budget: bigger never fits
	tokenLimit int // QueueTokenFactor × total budget

	queue        []*waiter
	queuedTokens int
}

type waiter struct {
	req      inference.InferenceRequest
	cost     int
	exclude  map[string]bool // workers a retry must avoid
	enqueued time.Time
	ready    chan *grant // buffer 1: the dispatcher never blocks
	granted  bool        // guarded by Scheduler.mu
}

// grant is a lease from one worker, as handed out by placeLocked.
type grant struct {
	lease inference.Lease
	m     *member
	probe bool // the half-open breaker's probe request
}

type counters struct {
	admittedImmediately  uint64
	admittedFromQueue    uint64
	rejectedQueueFull    uint64
	rejectedQueueTimeout uint64
	rejectedNoHealthy    uint64
	cancelledInQueue     uint64
	barrierWakeups       uint64
	workerFailures       uint64
	ttftTimeouts         uint64
	stallTimeouts        uint64
	retries              uint64
	retrySuccesses       uint64
	breakerOpens         uint64
	restartsDetected     uint64
}

const waitSamples = 1024

// New builds a scheduler over workers and starts its dispatcher and health
// checks. It panics on duplicate worker IDs (a configuration error); use
// AddWorker to add workers later.
func New(cfg Config, workers ...inference.Worker) *Scheduler {
	s := &Scheduler{
		cfg:     cfg.withDefaults(),
		now:     time.Now,
		kick:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		members: map[string]*member{},
		byModel: map[string]*modelQueue{},
		waits:   make([]time.Duration, 0, waitSamples),
	}
	for _, w := range workers {
		if err := s.AddWorker(w); err != nil {
			panic(err)
		}
	}
	s.wg.Add(1)
	go s.dispatcher()
	return s
}

// Close stops the dispatcher and the health checks. Requests still waiting
// time out normally.
func (s *Scheduler) Close() {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	s.wg.Wait()
}

// Acquire returns a lease for req, waiting in the model's queue when no
// worker has room. The lease detects hangs and, when its worker fails
// before the first token, transparently retries on another worker (see
// lease.go).
//
// Immediate errors (no waiting): inference.ErrInvalidRequest,
// ErrModelNotFound when no worker serves the model, ErrRequestTooLarge when
// it does not fit on any of them even empty, ErrNoHealthyWorkers when
// workers serve the model but none is healthy, and ErrQueueFull when the
// queue is full (the last two wrapped in an *inference.RetryAfterError).
// After waiting: ErrQueueTimeout (also with Retry-After), or the context's
// error when the client goes away.
func (s *Scheduler) Acquire(ctx context.Context, req inference.InferenceRequest) (inference.Lease, error) {
	g, err := s.acquire(ctx, req, nil)
	if err != nil {
		return nil, err
	}
	return &managedLease{s: s, req: req, cur: g}, nil
}

// acquire is Acquire without the lease decorator; exclude lists workers a
// retry must avoid.
func (s *Scheduler) acquire(ctx context.Context, req inference.InferenceRequest, exclude map[string]bool) (*grant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.PromptTokens < 0 || req.MaxNewTokens <= 0 {
		return nil, fmt.Errorf("%w: prompt_tokens=%d max_new_tokens=%d",
			inference.ErrInvalidRequest, req.PromptTokens, req.MaxNewTokens)
	}
	cost := req.TokenCost()

	s.mu.Lock()
	m := s.byModel[req.Model]
	if m == nil || m.serving == 0 {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", inference.ErrModelNotFound, req.Model)
	}
	if cost > m.maxBudget {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: cost %d > largest worker budget %d", inference.ErrRequestTooLarge, cost, m.maxBudget)
	}
	now := s.now()
	// Conflict C3: the queue is for waiting for capacity, not for recovery.
	if len(s.eligibleLocked(m.name, exclude, now)) == 0 {
		s.stats.rejectedNoHealthy++
		retry := s.retryAfterLocked()
		s.mu.Unlock()
		return nil, &inference.RetryAfterError{
			Err:        fmt.Errorf("%w: model %q", inference.ErrNoHealthyWorkers, req.Model),
			RetryAfter: retry,
		}
	}
	// Fast path, only when nobody of this model is waiting: never jump the
	// queue.
	if len(m.queue) == 0 {
		g, err := s.placeLocked(ctx, m.name, req, exclude, now)
		if err == nil {
			s.stats.admittedImmediately++
			s.mu.Unlock()
			return g, nil
		}
		if !errors.Is(err, inference.ErrAtCapacity) {
			s.mu.Unlock()
			return nil, err
		}
	}
	if s.waiting >= s.cfg.MaxQueuedRequests || m.queuedTokens+cost > m.tokenLimit {
		s.stats.rejectedQueueFull++
		retry := s.retryAfterLocked()
		err := fmt.Errorf("%w: model %q (%d tokens and %d requests queued; limits %d tokens, %d requests)",
			inference.ErrQueueFull, m.name, m.queuedTokens, s.waiting, m.tokenLimit, s.cfg.MaxQueuedRequests)
		s.mu.Unlock()
		return nil, &inference.RetryAfterError{Err: err, RetryAfter: retry}
	}
	w := &waiter{req: req, cost: cost, exclude: exclude, enqueued: now, ready: make(chan *grant, 1)}
	m.queue = append(m.queue, w)
	m.queuedTokens += cost
	s.waiting++
	s.mu.Unlock()
	s.wake()

	timeout := s.cfg.QueueTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, time.Until(deadline))
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case g := <-w.ready:
		return g, nil
	case <-ctx.Done():
		if g := s.leave(m, w, true); g != nil {
			s.drop(g) // granted just as the client left
		}
		return nil, ctx.Err()
	case <-timer.C:
		if g := s.leave(m, w, false); g != nil {
			return g, nil // granted just as the timeout fired: use it
		}
		s.mu.Lock()
		retry := s.retryAfterLocked()
		s.mu.Unlock()
		return nil, &inference.RetryAfterError{
			Err:        fmt.Errorf("%w: waited %v for model %q", inference.ErrQueueTimeout, timeout, m.name),
			RetryAfter: retry,
		}
	}
}

// leave removes a waiter that gave up. If the dispatcher granted it a lease
// in the meantime, leave returns that grant instead.
func (s *Scheduler) leave(m *modelQueue, w *waiter, cancelled bool) *grant {
	s.mu.Lock()
	if w.granted {
		s.mu.Unlock()
		return <-w.ready // already sent: never blocks
	}
	if i := slices.Index(m.queue, w); i >= 0 {
		s.removeLocked(m, i)
	}
	if cancelled {
		s.stats.cancelledInQueue++
	} else {
		s.stats.rejectedQueueTimeout++
		s.recordWaitLocked(s.now().Sub(w.enqueued))
	}
	s.mu.Unlock()
	return nil
}

func (s *Scheduler) removeLocked(m *modelQueue, i int) {
	m.queuedTokens -= m.queue[i].cost
	m.queue[i] = nil
	m.queue = slices.Delete(m.queue, i, i+1)
	s.waiting--
}

// wake nudges the dispatcher without blocking.
func (s *Scheduler) wake() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// dispatcher is the single consumer of every worker's CapacityChanged
// signal. Each wake-up triggers one pass over the queues, however many
// signals arrived, so its cost depends on how often it wakes, not on how
// many leases were freed. When the set of workers changes (the change
// kicks it) it rebuilds the list of signals it listens to.
func (s *Scheduler) dispatcher() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.SafetyPoll)
	defer ticker.Stop()

	var cases []reflect.SelectCase
	built := ^uint64(0)
	for {
		s.mu.Lock()
		if s.version != built {
			built = s.version
			cases = append(cases[:0],
				reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.stop)},
				reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.kick)},
				reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ticker.C)},
			)
			// Draining workers are still listened to until they are gone.
			for _, mem := range s.order {
				cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(mem.w.CapacityChanged())})
			}
		}
		s.mu.Unlock()
		if chosen, _, _ := reflect.Select(cases); chosen == 0 {
			return
		}
		s.dispatch()
	}
}

// dispatch tries to admit waiting requests, model by model.
func (s *Scheduler) dispatch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for _, m := range s.models {
		scanned := 0
		for i := 0; i < len(m.queue) && scanned < s.cfg.MaxScan; {
			w := m.queue[i]
			scanned++
			g, err := s.placeLocked(context.Background(), m.name, w.req, w.exclude, now)
			if err == nil {
				s.grantLocked(m, i, g, now)
				continue // index i now holds the next waiter
			}
			if i == 0 && now.Sub(w.enqueued) >= s.cfg.HeadMaxAge {
				// FIFO barrier (C1): the old head may not be passed.
				s.stats.barrierWakeups++
				break
			}
			i++
		}
	}
}

func (s *Scheduler) grantLocked(m *modelQueue, i int, g *grant, now time.Time) {
	w := m.queue[i]
	s.removeLocked(m, i)
	w.granted = true
	w.ready <- g // buffer 1, sent once: never blocks
	s.stats.admittedFromQueue++
	s.recordWaitLocked(now.Sub(w.enqueued))
}

func (s *Scheduler) recordWaitLocked(d time.Duration) {
	if s.waitEMA == 0 {
		s.waitEMA = d
	} else {
		s.waitEMA = (s.waitEMA*7 + d) / 8
	}
	if len(s.waits) < waitSamples {
		s.waits = append(s.waits, d)
	} else {
		s.waits[s.waitPos] = d
		s.waitPos = (s.waitPos + 1) % waitSamples
	}
}

// retryAfterLocked derives Retry-After from the recent queue wait, bounded
// and with jitter.
func (s *Scheduler) retryAfterLocked() time.Duration {
	d := min(max(s.waitEMA, s.cfg.MinRetryAfter), s.cfg.MaxRetryAfter)
	return d + time.Duration(rand.Int64N(int64(s.cfg.MinRetryAfter)))
}

// Models returns the sorted models served by the active workers.
func (s *Scheduler) Models() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.models))
	for _, m := range s.models {
		if m.serving > 0 {
			out = append(out, m.name)
		}
	}
	return out
}

// Stats is the scheduler state exposed on /debug/scheduler.
type Stats struct {
	Waiting              int          `json:"waiting"`
	MaxQueuedRequests    int          `json:"max_queued_requests"`
	AdmittedImmediately  uint64       `json:"admitted_immediately"`
	AdmittedFromQueue    uint64       `json:"admitted_from_queue"`
	RejectedQueueFull    uint64       `json:"rejected_queue_full"`
	RejectedQueueTimeout uint64       `json:"rejected_queue_timeout"`
	RejectedNoHealthy    uint64       `json:"rejected_no_healthy_workers"`
	CancelledInQueue     uint64       `json:"cancelled_in_queue"`
	BarrierWakeups       uint64       `json:"barrier_wakeups"`
	QueueWaitP50Ms       float64      `json:"queue_wait_p50_ms"`
	QueueWaitP99Ms       float64      `json:"queue_wait_p99_ms"`
	Models               []ModelStats `json:"models"`
	// Phase 4: failures attributed to workers (hangs included), hangs
	// detected, retries on another worker (and those that then completed),
	// breaker openings and worker restarts seen by the health checks.
	WorkerFailures   uint64 `json:"worker_failures"`
	TTFTTimeouts     uint64 `json:"ttft_timeouts"`
	StallTimeouts    uint64 `json:"stall_timeouts"`
	Retries          uint64 `json:"retries"`
	RetrySuccesses   uint64 `json:"retry_successes"`
	BreakerOpens     uint64 `json:"breaker_opens"`
	RestartsDetected uint64 `json:"restarts_detected"`
	// Placement holds the placement policy's own counters when it has
	// any (PrefixAffinity: AffinityStats).
	Placement any `json:"placement,omitempty"`
}

// placementStatser is implemented by placements that expose counters.
type placementStatser interface {
	DebugStats() any
}

// ModelStats is the queue state of one model.
type ModelStats struct {
	Model           string `json:"model"`
	Workers         int    `json:"workers"`
	QueuedRequests  int    `json:"queued_requests"`
	QueuedTokens    int    `json:"queued_tokens"`
	QueueTokenLimit int    `json:"queue_token_limit"`
	HeadAgeMs       int64  `json:"head_age_ms"`
}

// Stats returns a consistent snapshot of the queues and counters. Queue
// wait percentiles cover the last 1024 requests that left the queue
// (admitted or timed out).
func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	c := s.stats
	st := Stats{
		Waiting:              s.waiting,
		MaxQueuedRequests:    s.cfg.MaxQueuedRequests,
		AdmittedImmediately:  c.admittedImmediately,
		AdmittedFromQueue:    c.admittedFromQueue,
		RejectedQueueFull:    c.rejectedQueueFull,
		RejectedQueueTimeout: c.rejectedQueueTimeout,
		RejectedNoHealthy:    c.rejectedNoHealthy,
		CancelledInQueue:     c.cancelledInQueue,
		BarrierWakeups:       c.barrierWakeups,
		WorkerFailures:       c.workerFailures,
		TTFTTimeouts:         c.ttftTimeouts,
		StallTimeouts:        c.stallTimeouts,
		Retries:              c.retries,
		RetrySuccesses:       c.retrySuccesses,
		BreakerOpens:         c.breakerOpens,
		RestartsDetected:     c.restartsDetected,
	}
	if len(s.waits) > 0 {
		sorted := slices.Clone(s.waits)
		slices.Sort(sorted)
		pct := func(p float64) float64 {
			return float64(sorted[int(p*float64(len(sorted)-1))]) / float64(time.Millisecond)
		}
		st.QueueWaitP50Ms, st.QueueWaitP99Ms = pct(0.50), pct(0.99)
	}
	for _, m := range s.models {
		ms := ModelStats{Model: m.name, Workers: m.serving, QueuedRequests: len(m.queue), QueuedTokens: m.queuedTokens, QueueTokenLimit: m.tokenLimit}
		if len(m.queue) > 0 {
			ms.HeadAgeMs = now.Sub(m.queue[0].enqueued).Milliseconds()
		}
		st.Models = append(st.Models, ms)
	}
	if ps, ok := s.cfg.Placement.(placementStatser); ok {
		// Same lock order as Place: s.mu, then the placement's own locks.
		st.Placement = ps.DebugStats()
	}
	return st
}

// DebugStats exposes Stats to the HTTP layer without an import cycle.
func (s *Scheduler) DebugStats() any { return s.Stats() }

// modelQueueLocked returns the queue of a model, creating it (sorted by
// name, for a deterministic dispatch order) on first use.
func (s *Scheduler) modelQueueLocked(name string) *modelQueue {
	if m := s.byModel[name]; m != nil {
		return m
	}
	m := &modelQueue{name: name}
	s.byModel[name] = m
	s.models = append(s.models, m)
	sort.Slice(s.models, func(i, j int) bool { return s.models[i].name < s.models[j].name })
	return m
}
