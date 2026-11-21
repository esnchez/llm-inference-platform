package vllm

import (
	"context"
	"log/slog"
	"time"

	"llm-inference-platform/internal/inference"
)

// Registry is the part of the scheduler the connector needs.
type Registry interface {
	AddWorker(inference.Worker) error
	RemoveWorker(id string) error
}

// Connector registers a vLLM server once it is ready (Phase 6a, decision 5
// and conflict C2): it retries Discover with backoff, then calls AddWorker
// with the discovered budget, models and max_model_len. Afterwards it
// rediscovers every Recheck; if the server now reports a different budget or
// models (it restarted with other settings), the worker is drained and
// registered again.
type Connector struct {
	Config   Config
	Registry Registry
	Log      *slog.Logger
	// MinBackoff, MaxBackoff bound the retry delay; Recheck is the
	// rediscovery period. Defaults: 1s, 30s, 30s.
	MinBackoff, MaxBackoff, Recheck time.Duration
	// RemoveOnStop removes (drains) the registered worker when Run returns.
	// The DNS watcher sets it: its connectors stop when the server leaves
	// the DNS answer (Phase 7).
	RemoveOnStop bool
}

// Run blocks until ctx is cancelled.
func (c *Connector) Run(ctx context.Context) {
	if c.MinBackoff <= 0 {
		c.MinBackoff = time.Second
	}
	if c.MaxBackoff < c.MinBackoff {
		c.MaxBackoff = max(30*time.Second, c.MinBackoff)
	}
	if c.Recheck <= 0 {
		c.Recheck = 30 * time.Second
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
	var current *Worker
	defer func() {
		if c.RemoveOnStop && current != nil {
			_ = c.Registry.RemoveWorker(current.ID())
		}
	}()
	backoff := c.MinBackoff
	for {
		info, err := Discover(ctx, c.Config)
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			c.Log.Info("vllm not ready", "url", c.Config.BaseURL, "error", err, "retry_in", backoff.String())
		case current != nil && info.equal(current.Info()):
			backoff = c.Recheck // registered and unchanged
		default:
			if current != nil {
				c.Log.Warn("vllm changed: re-registering", "worker_id", current.ID(),
					"old_budget", current.Info().TokenBudget, "new_budget", info.TokenBudget)
				_ = c.Registry.RemoveWorker(current.ID())
				current = nil
			}
			w, err := New(c.Config, info)
			if err == nil {
				err = c.Registry.AddWorker(w) // fails while the old one still drains: retried
			}
			if err != nil {
				c.Log.Warn("vllm registration failed", "url", c.Config.BaseURL, "error", err)
				break
			}
			current = w
			c.Log.Info("vllm registered", "worker_id", w.ID(), "url", c.Config.BaseURL, "models", info.Models,
				"token_budget", info.TokenBudget, "max_model_len", info.MaxModelLen,
				"num_gpu_blocks", info.NumGPUBlocks, "block_size", info.BlockSize)
			backoff = c.Recheck
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if current == nil {
			backoff = min(backoff*2, c.MaxBackoff)
		}
	}
}
