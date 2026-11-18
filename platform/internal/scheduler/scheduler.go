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

// Config tunes admission. Zero fields take the DefaultConfig value.
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
	// signals CapacityChanged.
	SafetyPoll time.Duration
	// MinRetryAfter and MaxRetryAfter bound the Retry-After advertised on
	// queue rejections. The value follows the recent queue wait (EWMA) plus
	// up to MinRetryAfter of jitter, so that clients do not retry in sync.
	MinRetryAfter time.Duration
	MaxRetryAfter time.Duration
	// Placement chooses the worker. Default: &RoundRobin{}; cmd/platform
	// uses NewPrefixAffinity (Phase 3).
	Placement Placement
}

// DefaultConfig returns the Phase 2 defaults.
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
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.QueueTimeout <= 0 {
		c.QueueTimeout = d.QueueTimeout
	}
	if c.HeadMaxAge <= 0 {
		c.HeadMaxAge = c.QueueTimeout / 2
	}
	if c.QueueTokenFactor <= 0 {
		c.QueueTokenFactor = d.QueueTokenFactor
	}
	if c.MaxQueuedRequests <= 0 {
		c.MaxQueuedRequests = d.MaxQueuedRequests
	}
	if c.MaxScan <= 0 {
		c.MaxScan = d.MaxScan
	}
	if c.SafetyPoll <= 0 {
		c.SafetyPoll = d.SafetyPoll
	}
	if c.MinRetryAfter <= 0 {
		c.MinRetryAfter = d.MinRetryAfter
	}
	if c.MaxRetryAfter < c.MinRetryAfter {
		c.MaxRetryAfter = max(d.MaxRetryAfter, c.MinRetryAfter)
	}
	if c.Placement == nil {
		c.Placement = &RoundRobin{}
	}
	return c
}

// Scheduler is the Phase 2 control-plane scheduler. Create it with New and
// stop its dispatcher with Close.
type Scheduler struct {
	cfg     Config
	workers []inference.Worker
	models  []*modelQueue // sorted by name: deterministic dispatch order
	byModel map[string]*modelQueue
	now     func() time.Time

	kick chan struct{} // buffer 1: new arrivals wake the dispatcher
	stop chan struct{}
	wg   sync.WaitGroup

	mu      sync.Mutex
	waiting int
	stats   counters
	waitEMA time.Duration // EWMA of the queue wait of admitted requests
	waits   []time.Duration
	waitPos int
}

type modelQueue struct {
	name       string
	workers    []inference.Worker
	maxBudget  int // largest single worker budget: bigger never fits
	tokenLimit int // QueueTokenFactor × total budget

	queue        []*waiter
	queuedTokens int
}

type waiter struct {
	req      inference.InferenceRequest
	cost     int
	enqueued time.Time
	ready    chan inference.Lease // buffer 1: the dispatcher never blocks
	granted  bool                 // guarded by Scheduler.mu
}

type counters struct {
	admittedImmediately  uint64
	admittedFromQueue    uint64
	rejectedQueueFull    uint64
	rejectedQueueTimeout uint64
	cancelledInQueue     uint64
	barrierWakeups       uint64
}

const waitSamples = 1024

// New builds a scheduler over workers and starts its dispatcher.
func New(cfg Config, workers ...inference.Worker) *Scheduler {
	s := &Scheduler{
		cfg:     cfg.withDefaults(),
		workers: slices.Clone(workers),
		byModel: map[string]*modelQueue{},
		now:     time.Now,
		kick:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		waits:   make([]time.Duration, 0, waitSamples),
	}
	for _, w := range s.workers {
		budget := w.Snapshot().TokenBudget // static configuration
		for _, name := range w.Models() {
			m := s.byModel[name]
			if m == nil {
				m = &modelQueue{name: name}
				s.byModel[name] = m
				s.models = append(s.models, m)
			}
			m.workers = append(m.workers, w)
			m.maxBudget = max(m.maxBudget, budget)
			m.tokenLimit += budget
		}
	}
	for _, m := range s.models {
		m.tokenLimit = int(float64(m.tokenLimit) * s.cfg.QueueTokenFactor)
	}
	sort.Slice(s.models, func(i, j int) bool { return s.models[i].name < s.models[j].name })

	s.wg.Add(1)
	go s.dispatcher()
	return s
}

// Close stops the dispatcher. Requests still waiting time out normally.
func (s *Scheduler) Close() {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	s.wg.Wait()
}

// Acquire returns a lease for req, waiting in the model's queue when no
// worker has room.
//
// Immediate errors (no waiting): inference.ErrInvalidRequest,
// ErrModelNotFound when no worker serves the model, ErrRequestTooLarge when
// it does not fit on any of them even empty, and ErrQueueFull (wrapped in an
// *inference.RetryAfterError) when the queue is full. After waiting:
// ErrQueueTimeout (also with Retry-After), or the context's error when the
// client goes away.
func (s *Scheduler) Acquire(ctx context.Context, req inference.InferenceRequest) (inference.Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.PromptTokens < 0 || req.MaxNewTokens <= 0 {
		return nil, fmt.Errorf("%w: prompt_tokens=%d max_new_tokens=%d",
			inference.ErrInvalidRequest, req.PromptTokens, req.MaxNewTokens)
	}
	m := s.byModel[req.Model]
	if m == nil {
		return nil, fmt.Errorf("%w: %q", inference.ErrModelNotFound, req.Model)
	}
	cost := req.TokenCost()
	if cost > m.maxBudget {
		return nil, fmt.Errorf("%w: cost %d > largest worker budget %d", inference.ErrRequestTooLarge, cost, m.maxBudget)
	}

	s.mu.Lock()
	// Fast path, only when nobody of this model is waiting: never jump the
	// queue.
	if len(m.queue) == 0 {
		lease, err := s.cfg.Placement.Place(ctx, req, m.workers)
		if err == nil {
			s.stats.admittedImmediately++
			s.mu.Unlock()
			return lease, nil
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
	w := &waiter{req: req, cost: cost, enqueued: s.now(), ready: make(chan inference.Lease, 1)}
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
	case lease := <-w.ready:
		return lease, nil
	case <-ctx.Done():
		if lease := s.leave(m, w, true); lease != nil {
			lease.Release() // granted just as the client left
		}
		return nil, ctx.Err()
	case <-timer.C:
		if lease := s.leave(m, w, false); lease != nil {
			return lease, nil // granted just as the timeout fired: use it
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
// in the meantime, leave returns that lease instead.
func (s *Scheduler) leave(m *modelQueue, w *waiter, cancelled bool) inference.Lease {
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
// many leases were freed.
func (s *Scheduler) dispatcher() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.SafetyPoll)
	defer ticker.Stop()

	cases := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.stop)},
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.kick)},
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ticker.C)},
	}
	for _, w := range s.workers {
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(w.CapacityChanged())})
	}
	for {
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
			lease, err := s.cfg.Placement.Place(context.Background(), w.req, m.workers)
			if err == nil {
				s.grantLocked(m, i, lease, now)
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

func (s *Scheduler) grantLocked(m *modelQueue, i int, lease inference.Lease, now time.Time) {
	w := m.queue[i]
	s.removeLocked(m, i)
	w.granted = true
	w.ready <- lease // buffer 1, sent once: never blocks
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

// Models returns the sorted models served by the workers.
func (s *Scheduler) Models() []string {
	out := make([]string, 0, len(s.models))
	for _, m := range s.models {
		out = append(out, m.name)
	}
	return out
}

// Snapshots returns the state of every worker.
func (s *Scheduler) Snapshots() []inference.WorkerSnapshot {
	out := make([]inference.WorkerSnapshot, 0, len(s.workers))
	for _, w := range s.workers {
		out = append(out, w.Snapshot())
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
	CancelledInQueue     uint64       `json:"cancelled_in_queue"`
	BarrierWakeups       uint64       `json:"barrier_wakeups"`
	QueueWaitP50Ms       float64      `json:"queue_wait_p50_ms"`
	QueueWaitP99Ms       float64      `json:"queue_wait_p99_ms"`
	Models               []ModelStats `json:"models"`
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
	st := Stats{
		Waiting:              s.waiting,
		MaxQueuedRequests:    s.cfg.MaxQueuedRequests,
		AdmittedImmediately:  s.stats.admittedImmediately,
		AdmittedFromQueue:    s.stats.admittedFromQueue,
		RejectedQueueFull:    s.stats.rejectedQueueFull,
		RejectedQueueTimeout: s.stats.rejectedQueueTimeout,
		CancelledInQueue:     s.stats.cancelledInQueue,
		BarrierWakeups:       s.stats.barrierWakeups,
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
		ms := ModelStats{Model: m.name, QueuedRequests: len(m.queue), QueuedTokens: m.queuedTokens, QueueTokenLimit: m.tokenLimit}
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
