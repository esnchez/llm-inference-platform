package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"llm-inference-platform/internal/inference"
)

// This file is the worker registry of Phase 4
// (docs/servicio/10-fase-4-decisiones.md, decisions 1–3): adding and
// draining workers, health checks, the circuit breaker and the "degraded"
// mark that only deprioritises slow workers. All member state is guarded by
// Scheduler.mu.

type breakerState int

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

func (b breakerState) String() string {
	switch b {
	case breakerOpen:
		return "open"
	case breakerHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// member is a registered worker and the scheduler's view of it.
type member struct {
	w      inference.Worker
	id     string
	budget int
	maxReq int // largest single request (0: budget): vLLM's max_model_len
	models []string
	stop   chan struct{} // closed on removal: stops the health loop

	draining bool
	inFlight int // grants handed out and not finished yet

	// Health checks (decision 2).
	healthy    bool
	healthFail int
	healthOK   int
	instanceID string

	// Circuit breaker (decision 3).
	breaker      breakerState
	openUntil    time.Time
	reopens      int // consecutive openings without a close: cooldown backoff
	probing      bool
	consecFail   int
	failures     uint64
	breakerOpens uint64

	// Inter-token latency, for the degraded mark.
	itlEWMA    time.Duration
	itlSamples int
}

// outcome classifies how a stream ended, for the breaker.
type outcome int

const (
	outcomeNeutral outcome = iota // client cancelled, request error, never executed
	outcomeSuccess
	outcomeFailure // inference.ErrWorkerFailure or a hang
)

// forgetter is implemented by placements that remember per-worker state
// (PrefixAffinity's radix tree).
type forgetter interface {
	RemoveWorker(id string)
}

// AddWorker registers a worker at run time. It serves new requests at once
// (healthy until its health checks say otherwise) and wakes the queue.
func (s *Scheduler) AddWorker(w inference.Worker) error {
	snap := w.Snapshot() // static configuration: budget
	mem := &member{
		w: w, id: w.ID(), budget: snap.TokenBudget, maxReq: snap.MaxRequestTokens, models: w.Models(),
		stop: make(chan struct{}), healthy: true,
	}
	s.mu.Lock()
	if _, dup := s.members[mem.id]; dup {
		s.mu.Unlock()
		return fmt.Errorf("scheduler: worker %q already registered", mem.id)
	}
	s.members[mem.id] = mem
	s.order = append(s.order, mem)
	for _, name := range mem.models {
		s.modelQueueLocked(name)
	}
	s.recomputeLocked()
	s.version++
	if s.cfg.HealthInterval > 0 {
		s.wg.Add(1)
		go s.healthLoop(mem)
	}
	s.mu.Unlock()
	s.cfg.Logger.Info("worker added", "worker_id", mem.id, "models", mem.models, "token_budget", mem.budget)
	s.wake()
	return nil
}

// RemoveWorker drains a worker: it gets no new requests, its streams in
// flight finish normally, and once the last one ends it is forgotten (also
// by the placement). It returns an error for unknown workers.
func (s *Scheduler) RemoveWorker(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	mem := s.members[id]
	if mem == nil {
		return fmt.Errorf("scheduler: unknown worker %q", id)
	}
	if mem.draining {
		return nil
	}
	mem.draining = true
	close(mem.stop)
	s.recomputeLocked()
	s.cfg.Logger.Info("worker draining", "worker_id", id, "in_flight", mem.inFlight)
	if mem.inFlight == 0 {
		s.finalizeLocked(mem)
	}
	return nil
}

func (s *Scheduler) finalizeLocked(mem *member) {
	delete(s.members, mem.id)
	if i := slices.Index(s.order, mem); i >= 0 {
		s.order = slices.Delete(s.order, i, i+1)
	}
	s.version++
	s.forgetLocked(mem.id)
	s.cfg.Logger.Info("worker removed", "worker_id", mem.id)
	s.wake()
}

func (s *Scheduler) forgetLocked(id string) {
	if f, ok := s.cfg.Placement.(forgetter); ok {
		f.RemoveWorker(id) // lock order: s.mu, then the placement's locks
	}
}

// recomputeLocked derives each model's limits from its active workers.
func (s *Scheduler) recomputeLocked() {
	for _, m := range s.models {
		m.serving, m.maxBudget, m.tokenLimit = 0, 0, 0
	}
	for _, mem := range s.order {
		if mem.draining {
			continue
		}
		for _, name := range mem.models {
			m := s.byModel[name]
			m.serving++
			largest := mem.budget
			if mem.maxReq > 0 {
				largest = min(largest, mem.maxReq) // conflict C3 of Phase 6a
			}
			m.maxBudget = max(m.maxBudget, largest)
			m.tokenLimit += mem.budget
		}
	}
	for _, m := range s.models {
		m.tokenLimit = int(float64(m.tokenLimit) * s.cfg.QueueTokenFactor)
	}
}

// ---- health checks ----

func (s *Scheduler) healthLoop(mem *member) {
	defer s.wg.Done()
	// Jitter the first check so that workers are not all polled at once.
	timer := time.NewTimer(time.Duration(rand.Int64N(int64(s.cfg.HealthInterval))))
	defer timer.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-mem.stop:
			return
		case <-timer.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.HealthTimeout)
		st, err := mem.w.Health(ctx) // never under s.mu: it may be a network call
		cancel()
		s.cfg.Observer.HealthCheck(mem.id, err == nil)
		s.mu.Lock()
		s.recordHealthLocked(mem, st, err)
		s.mu.Unlock()
		timer.Reset(s.cfg.HealthInterval)
	}
}

// recordHealthLocked applies one health-check result, with hysteresis.
func (s *Scheduler) recordHealthLocked(mem *member, st inference.HealthStatus, err error) {
	if err != nil {
		mem.healthOK = 0
		mem.healthFail++
		if mem.healthy && mem.healthFail >= s.cfg.HealthFailThreshold {
			mem.healthy = false
			s.cfg.Logger.Warn("worker unhealthy", "worker_id", mem.id, "consecutive_failures", mem.healthFail, "error", err)
			s.openLocked(mem, s.now(), "unhealthy")
		}
		return
	}
	mem.healthFail = 0
	mem.healthOK++
	if st.InstanceID != "" {
		if mem.instanceID != "" && st.InstanceID != mem.instanceID {
			// Restarted: its prefix cache is empty (conflict C2).
			s.stats.restartsDetected++
			s.cfg.Observer.RestartDetected(mem.id)
			s.cfg.Logger.Warn("worker restart detected", "worker_id", mem.id, "old_instance", mem.instanceID, "new_instance", st.InstanceID)
			s.forgetLocked(mem.id)
		}
		mem.instanceID = st.InstanceID
	}
	if !mem.healthy && mem.healthOK >= s.cfg.HealthOKThreshold {
		mem.healthy = true
		s.cfg.Logger.Info("worker healthy", "worker_id", mem.id)
		s.wake()
	}
}

// ---- circuit breaker ----

// openLocked opens mem's breaker, with an exponential cooldown. reason is
// "failures", "unhealthy" or "probe_failed" (for the log).
func (s *Scheduler) openLocked(mem *member, now time.Time, reason string) {
	if mem.breaker == breakerOpen {
		return
	}
	cooldown := s.cfg.BreakerCooldown
	for i := 0; i < mem.reopens && cooldown < s.cfg.BreakerMaxCooldown; i++ {
		cooldown *= 2
	}
	cooldown = min(cooldown, s.cfg.BreakerMaxCooldown)
	mem.breaker = breakerOpen
	mem.openUntil = now.Add(cooldown)
	mem.reopens++
	mem.breakerOpens++
	s.stats.breakerOpens++
	s.cfg.Observer.BreakerTransition(mem.id, "open")
	s.cfg.Logger.Warn("breaker opened", "worker_id", mem.id, "reason", reason, "cooldown", cooldown.String())
}

// usableLocked says whether placement may use mem now. An open breaker
// whose cooldown is over becomes half-open: one probe at a time.
func (s *Scheduler) usableLocked(mem *member, now time.Time) bool {
	if mem.draining || !mem.healthy {
		return false
	}
	switch mem.breaker {
	case breakerOpen:
		if now.Before(mem.openUntil) {
			return false
		}
		mem.breaker = breakerHalfOpen
		s.cfg.Observer.BreakerTransition(mem.id, "half_open")
		s.cfg.Logger.Info("breaker half-open", "worker_id", mem.id)
		fallthrough
	case breakerHalfOpen:
		return !mem.probing
	default:
		return true
	}
}

// eligibleLocked returns the usable workers of a model, in registration
// order, minus the excluded ones.
func (s *Scheduler) eligibleLocked(model string, exclude map[string]bool, now time.Time) []*member {
	var out []*member
	for _, mem := range s.order {
		if exclude[mem.id] || !slices.Contains(mem.models, model) || !s.usableLocked(mem, now) {
			continue
		}
		out = append(out, mem)
	}
	return out
}

// degradedLocked marks the members whose mean ITL exceeds DegradedFactor ×
// the median of their peers' (the other members with enough samples). The
// worker itself is left out of the median: with two workers, a median that
// included the slow one could never be exceeded by a factor of 2. A lone
// worker has no peer to be slower than.
func (s *Scheduler) degradedLocked(ms []*member) map[*member]bool {
	var sampled []*member
	for _, mem := range ms {
		if mem.itlSamples >= s.cfg.DegradedMinSamples {
			sampled = append(sampled, mem)
		}
	}
	if len(sampled) < 2 {
		return nil
	}
	out := map[*member]bool{}
	peers := make([]time.Duration, 0, len(sampled)-1)
	for _, mem := range sampled {
		peers = peers[:0]
		for _, other := range sampled {
			if other != mem {
				peers = append(peers, other.itlEWMA)
			}
		}
		slices.Sort(peers)
		median := peers[len(peers)/2]
		if len(peers)%2 == 0 {
			median = (peers[len(peers)/2-1] + peers[len(peers)/2]) / 2
		}
		if float64(mem.itlEWMA) > float64(median)*s.cfg.DegradedFactor {
			out[mem] = true
		}
	}
	return out
}

// placeLocked asks the placement for a lease among the usable workers:
// first the healthy ones, then, if none of them has room, the degraded
// ones. It returns inference.ErrAtCapacity when nobody has room.
func (s *Scheduler) placeLocked(ctx context.Context, model string, req inference.InferenceRequest, exclude map[string]bool, now time.Time) (*grant, error) {
	eligible := s.eligibleLocked(model, exclude, now)
	degraded := s.degradedLocked(eligible)
	var normal, slow []inference.Worker
	for _, mem := range eligible {
		if degraded[mem] {
			slow = append(slow, mem.w)
		} else {
			normal = append(normal, mem.w)
		}
	}
	var lease inference.Lease
	err := inference.ErrAtCapacity
	if len(normal) > 0 {
		lease, err = s.cfg.Placement.Place(ctx, req, normal)
	}
	if errors.Is(err, inference.ErrAtCapacity) && len(slow) > 0 {
		lease, err = s.cfg.Placement.Place(ctx, req, slow)
	}
	if err != nil {
		return nil, err
	}
	mem := s.members[lease.WorkerID()]
	mem.inFlight++
	g := &grant{lease: lease, m: mem}
	if mem.breaker == breakerHalfOpen {
		g.probe = true
		mem.probing = true
	}
	return g, nil
}

// finish records how a grant ended: it updates the breaker and the ITL,
// and completes a pending removal. The grant's lease must already be
// released.
func (s *Scheduler) finish(g *grant, o outcome, itl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mem := g.m
	mem.inFlight--
	if g.probe {
		mem.probing = false
	}
	switch o {
	case outcomeSuccess:
		mem.consecFail = 0
		if itl > 0 {
			if mem.itlSamples == 0 {
				mem.itlEWMA = itl
			} else {
				mem.itlEWMA = (mem.itlEWMA*7 + itl) / 8
			}
			mem.itlSamples++
		}
		if g.probe && mem.breaker == breakerHalfOpen && mem.healthy {
			mem.breaker = breakerClosed
			mem.reopens = 0
			s.cfg.Observer.BreakerTransition(mem.id, "closed")
			s.cfg.Logger.Info("breaker closed", "worker_id", mem.id)
			s.wake()
		}
	case outcomeFailure:
		mem.consecFail++
		mem.failures++
		s.stats.workerFailures++
		switch {
		case g.probe && mem.breaker == breakerHalfOpen:
			s.openLocked(mem, s.now(), "probe_failed")
		case mem.breaker == breakerClosed && mem.consecFail >= s.cfg.BreakerFailures:
			s.openLocked(mem, s.now(), "failures")
		}
	}
	if g.probe && o == outcomeNeutral {
		s.wake() // the probe slot is free again
	}
	if mem.draining && mem.inFlight == 0 && s.members[mem.id] == mem {
		s.finalizeLocked(mem)
	}
}

// drop releases a grant that will never be executed.
func (s *Scheduler) drop(g *grant) {
	g.lease.Release()
	s.finish(g, outcomeNeutral, 0)
}

// Snapshots returns the state of every registered worker (draining ones
// included), with the scheduler's view in Status.
func (s *Scheduler) Snapshots() []inference.WorkerSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	degraded := s.degradedLocked(s.order)
	out := make([]inference.WorkerSnapshot, 0, len(s.order))
	for _, mem := range s.order {
		snap := mem.w.Snapshot() // lock order: s.mu, then the worker's
		state := "active"
		if mem.draining {
			state = "draining"
		}
		snap.Status = &inference.WorkerStatus{
			State:               state,
			Healthy:             mem.healthy,
			Breaker:             mem.breaker.String(),
			Degraded:            degraded[mem],
			ITLMs:               float64(mem.itlEWMA) / float64(time.Millisecond),
			ITLSamples:          mem.itlSamples,
			InFlight:            mem.inFlight,
			ConsecutiveFailures: mem.consecFail,
			Failures:            mem.failures,
			BreakerOpens:        mem.breakerOpens,
			InstanceID:          mem.instanceID,
		}
		out = append(out, snap)
	}
	return out
}
