package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"llm-inference-platform/internal/inference"
)

// managedLease is the lease Acquire returns (Phase 4, decisions 3 and 4).
// It wraps the worker's lease to:
//
//   - see how each stream ends, to feed the worker's circuit breaker and
//     its inter-token latency;
//   - cancel a stream that emits no first token within the TTFT timeout, or
//     no new token within StallTimeout (a hang), whatever the worker type;
//   - retry, at most MaxRetries times and on another worker, a stream whose
//     worker failed before the first token. Nothing has reached the client
//     yet, so the retry is invisible to it; WorkerID reports the worker
//     that finally serves the request.
//
// It honours the inference.Lease contract: ExecuteStream at most once,
// Release idempotent and safe concurrently.
type managedLease struct {
	s   *Scheduler
	req inference.InferenceRequest

	mu         sync.Mutex
	cur        *grant
	curDone    bool // cur's lease was released and reported
	executed   bool
	released   bool
	queueWait  time.Duration
	retries    int
	workerTTFT time.Duration // of the attempt that emitted the first token
}

var _ inference.Lease = (*managedLease)(nil)

// QueueWait is how long the request waited in the queue before its first
// lease (Phase 5: the per-request log).
func (l *managedLease) QueueWait() time.Duration { return l.queueWait }

// ReservedTokens is what the request reserves in the ledger, after any
// clamping of max_tokens to the context length (Phase 6b-CPU).
func (l *managedLease) ReservedTokens() int { return l.req.TokenCost() }

// Retries is how many times the request moved to another worker.
func (l *managedLease) Retries() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.retries
}

// WorkerTTFT is the time from the start of the stream on the worker that
// served the request to its first token (0 if none came).
func (l *managedLease) WorkerTTFT() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.workerTTFT
}

func (l *managedLease) WorkerID() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cur.lease.WorkerID()
}

// Release frees the current worker lease (if ExecuteStream is not using
// it). Idempotent.
func (l *managedLease) Release() {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return
	}
	l.released = true
	g, done := l.cur, l.curDone
	l.curDone = true
	l.mu.Unlock()
	if !done {
		l.s.drop(g)
	}
}

// end releases and reports grant g, unless Release already did.
func (l *managedLease) end(g *grant, o outcome, itl time.Duration) {
	l.mu.Lock()
	if g == l.cur {
		if l.curDone {
			l.mu.Unlock()
			return
		}
		l.curDone = true
	}
	l.mu.Unlock()
	g.lease.Release()
	l.s.finish(g, o, itl)
}

func (l *managedLease) ExecuteStream(ctx context.Context, out chan<- inference.TokenChunk) error {
	l.mu.Lock()
	switch {
	case l.executed:
		l.mu.Unlock()
		return inference.ErrLeaseUsed
	case l.released:
		l.mu.Unlock()
		return inference.ErrLeaseReleased
	}
	l.executed = true
	g := l.cur
	l.mu.Unlock()

	s := l.s
	var exclude map[string]bool
	retried := func(r streamRun) {
		result := "failed"
		if r.outcome == outcomeSuccess {
			result = "succeeded"
			s.count(func(c *counters) { c.retrySuccesses++ })
		}
		s.cfg.Observer.Retried(l.req.Model, result)
	}
	for attempt := 0; ; attempt++ {
		r := s.runStream(ctx, l.req, g.lease, out)
		if r.sent > 0 {
			l.mu.Lock()
			l.workerTTFT = r.ttft
			l.mu.Unlock()
		}
		l.end(g, r.outcome, r.itl)
		retry := r.outcome == outcomeFailure && r.sent == 0 && attempt < s.cfg.MaxRetries && ctx.Err() == nil
		if retry {
			l.mu.Lock()
			retry = !l.released
			l.mu.Unlock()
		}
		if !retry {
			if attempt > 0 {
				retried(r)
			}
			return r.err
		}

		if exclude == nil {
			exclude = map[string]bool{}
		}
		failed := g.lease.WorkerID()
		exclude[failed] = true
		s.count(func(c *counters) { c.retries++ })
		s.cfg.Logger.Warn("request retried", "request_id", l.req.ID, "failed_worker", failed, "error", r.err)
		l.mu.Lock()
		l.retries++
		l.mu.Unlock()
		ng, err := s.acquire(ctx, l.req, exclude)
		if err != nil {
			retried(streamRun{outcome: outcomeFailure})
			return r.err // nowhere else to go: report the original failure
		}
		l.mu.Lock()
		if l.released {
			l.mu.Unlock()
			s.drop(ng)
			return r.err
		}
		l.cur, l.curDone = ng, false
		l.mu.Unlock()
		g = ng
	}
}

func (s *Scheduler) count(f func(*counters)) {
	s.mu.Lock()
	f(&s.stats)
	s.mu.Unlock()
}

type streamRun struct {
	sent    int
	err     error
	outcome outcome
	ttft    time.Duration // from the start of the stream to its first token
	itl     time.Duration // mean inter-token latency, successful streams with >= 2 tokens
}

// runStream executes one worker lease, forwarding its chunks to out and
// enforcing the hang timeouts, and classifies how it ended.
func (s *Scheduler) runStream(ctx context.Context, req inference.InferenceRequest, lease inference.Lease, out chan<- inference.TokenChunk) streamRun {
	inner, cancel := context.WithCancel(ctx)
	defer cancel()
	worker := lease.WorkerID()
	obs := s.cfg.Observer
	start := time.Now()
	chunks, wait := inference.StartStream(inner, lease, 1)

	ttftTimeout := s.cfg.TTFTTimeout + time.Duration(req.PromptTokens)*s.cfg.TTFTTimeoutPerToken
	timer := time.NewTimer(ttftTimeout)
	defer timer.Stop()

	var (
		r           streamRun
		first, last time.Time
		hang        string
	)
loop:
	for {
		select {
		case c, ok := <-chunks:
			if !ok {
				break loop
			}
			now := time.Now()
			if r.sent == 0 {
				first = now
				r.ttft = now.Sub(start)
				obs.WorkerFirstToken(worker, r.ttft)
			} else {
				obs.WorkerTokenGap(worker, now.Sub(last))
			}
			last = now
			select {
			case out <- c:
				r.sent++
			case <-ctx.Done():
				cancel()
				break loop
			}
			timer.Reset(s.cfg.StallTimeout) // the worker is only timed while we wait for it
		case <-timer.C:
			if r.sent == 0 {
				hang = fmt.Sprintf("no first token within %v", ttftTimeout)
			} else {
				hang = fmt.Sprintf("no token for %v after %d tokens", s.cfg.StallTimeout, r.sent)
			}
			cancel()
			break loop
		}
	}
	for range chunks { // the worker returns soon after cancel
	}
	r.err = wait()

	switch {
	case hang != "":
		r.outcome = outcomeFailure
		r.err = fmt.Errorf("%s: %w: %s", worker, inference.ErrWorkerFailure, hang)
		kind := "ttft_timeout"
		if r.sent > 0 {
			kind = "stall_timeout"
		}
		s.count(func(c *counters) {
			if r.sent == 0 {
				c.ttftTimeouts++
			} else {
				c.stallTimeouts++
			}
		})
		obs.WorkerFailure(worker, kind)
		s.cfg.Logger.Warn("worker hung", "worker_id", worker, "request_id", req.ID, "kind", kind, "tokens_sent", r.sent)
	case r.err == nil:
		r.outcome = outcomeSuccess
		if r.sent >= 2 {
			r.itl = last.Sub(first) / time.Duration(r.sent-1)
		}
	case ctx.Err() != nil:
		r.outcome = outcomeNeutral // the client went away
	case errors.Is(r.err, inference.ErrWorkerFailure):
		r.outcome = outcomeFailure
		obs.WorkerFailure(worker, "error")
	default:
		r.outcome = outcomeNeutral // a problem with the request, not the worker
	}
	return r
}
