package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"llm-inference-platform/internal/inference"
)

// Placement decides WHERE a request runs: it tries the given workers (all of
// them serve the request's model) and returns the first lease granted, or
// inference.ErrAtCapacity when none has room right now.
//
// The dispatcher calls Place on every wake-up for every waiting request it
// examines, so implementations must be cheap and free of side effects other
// than the AcquireLease that succeeds. Phase 2 uses RoundRobin; Phase 3 adds
// prefix affinity behind this same interface.
type Placement interface {
	Place(ctx context.Context, req inference.InferenceRequest, workers []inference.Worker) (inference.Lease, error)
}

// RoundRobin tries the workers starting at a rotating offset and keeps the
// first lease granted (first fit). It is safe for concurrent use.
type RoundRobin struct {
	next atomic.Uint64
}

// Place implements Placement.
func (p *RoundRobin) Place(ctx context.Context, req inference.InferenceRequest, workers []inference.Worker) (inference.Lease, error) {
	n := len(workers)
	if n == 0 {
		return nil, inference.ErrAtCapacity
	}
	start := int(p.next.Add(1)-1) % n
	ordered := make([]inference.Worker, n)
	for k := range n {
		ordered[k] = workers[(start+k)%n]
	}
	return acquireFirst(ctx, req, ordered)
}

// acquireFirst tries the workers in order and returns the first lease
// granted. Errors that no other worker can fix (cancelled context, invalid
// request) stop the search; a full worker, one too small for the request or
// a failing one moves on to the next. With no lease it returns
// inference.ErrAtCapacity.
func acquireFirst(ctx context.Context, req inference.InferenceRequest, ordered []inference.Worker) (inference.Lease, error) {
	var lastErr error
	for _, w := range ordered {
		lease, err := w.AcquireLease(ctx, req)
		switch {
		case err == nil:
			return lease, nil
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
			errors.Is(err, inference.ErrInvalidRequest):
			return nil, err
		case errors.Is(err, inference.ErrAtCapacity), errors.Is(err, inference.ErrRequestTooLarge):
			// Too large here may still fit on a bigger worker: keep trying.
		default:
			lastErr = err // a worker failure: try the next one
		}
	}
	if lastErr != nil {
		return nil, fmt.Errorf("%w: last worker error: %v", inference.ErrAtCapacity, lastErr)
	}
	return nil, inference.ErrAtCapacity
}
