package workertest

import (
	"context"

	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/ledger"
)

// Stub is a minimal inference.Worker over the shared ledger whose streams
// run a caller-supplied executor. Tests use it to give a worker behaviours
// the simulator does not have (engine usage, max_model_len).
type Stub struct {
	*ledger.Ledger
	exec ledger.Executor
}

var _ inference.Worker = (*Stub)(nil)

// NewStub returns a stub worker; it panics on an invalid config (tests).
func NewStub(cfg ledger.Config, exec ledger.Executor) *Stub {
	l, err := ledger.New(cfg)
	if err != nil {
		panic(err)
	}
	return &Stub{Ledger: l, exec: exec}
}

func (s *Stub) AcquireLease(ctx context.Context, req inference.InferenceRequest) (inference.Lease, error) {
	return s.Acquire(ctx, req, s.exec)
}

func (s *Stub) Health(context.Context) (inference.HealthStatus, error) {
	return inference.HealthStatus{InstanceID: s.ID() + "#0"}, nil
}

// Words returns an executor that streams the given words and, on the last
// chunk, the given usage (nil: none).
func Words(usage *inference.Usage, words ...string) ledger.Executor {
	return func(ctx context.Context, _ inference.InferenceRequest, out chan<- inference.TokenChunk) error {
		for i, w := range words {
			c := inference.TokenChunk{Index: i, Text: w}
			if i == len(words)-1 {
				c.FinishReason = inference.FinishStop
				c.Usage = usage
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- c:
			}
		}
		return nil
	}
}
