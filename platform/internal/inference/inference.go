// Package inference defines the core data-plane contract shared by the control
// plane and every worker implementation (simulated, vLLM, ...).
package inference

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Errors returned by workers and by the control plane. Callers compare them
// with errors.Is.
var (
	// ErrAtCapacity means the worker (or the whole pool) has no free token
	// budget right now. The request may succeed later.
	ErrAtCapacity = errors.New("at capacity")
	// ErrRequestTooLarge means the request's token cost exceeds the total
	// budget of the worker, so it can never be admitted there.
	ErrRequestTooLarge = errors.New("request exceeds worker token budget")
	// ErrModelNotServed means this worker does not serve the requested model.
	ErrModelNotServed = errors.New("model not served by worker")
	// ErrModelNotFound means no worker in the pool serves the requested model.
	ErrModelNotFound = errors.New("model not found")
	// ErrInvalidRequest means the request is malformed (e.g. non-positive cost).
	ErrInvalidRequest = errors.New("invalid inference request")

	// ErrLeaseReleased means ExecuteStream was called on a released lease.
	ErrLeaseReleased = errors.New("lease already released")
	// ErrLeaseUsed means ExecuteStream was called more than once.
	ErrLeaseUsed = errors.New("lease already executed")
	// ErrLeaseExpired means the lease TTL elapsed before ExecuteStream started,
	// so the worker reclaimed the capacity.
	ErrLeaseExpired = errors.New("lease expired before execution")

	// ErrQueueFull means the scheduler's wait queue for the model is full, so
	// the request is shed without waiting. The request may succeed later.
	ErrQueueFull = errors.New("queue full")
	// ErrQueueTimeout means the request waited in the queue for the maximum
	// time without capacity being freed. The request may succeed later.
	ErrQueueTimeout = errors.New("queue timeout")

	// ErrWorkerFailure marks a failure of the worker itself (connection
	// refused, engine 5xx, stream cut, hang), as opposed to a problem with
	// the request or a client cancellation. Worker implementations wrap
	// their own failures with it; only these count against the worker's
	// circuit breaker and allow a retry on another worker (Phase 4).
	ErrWorkerFailure = errors.New("worker failure")
	// ErrNoHealthyWorkers means workers serve the model but none of them is
	// healthy right now (failed health checks or open circuit breaker). The
	// request may succeed later.
	ErrNoHealthyWorkers = errors.New("no healthy workers")
)

// HealthStatus is what a worker reports to a health check.
type HealthStatus struct {
	// InstanceID identifies the running process. A different value means
	// the worker restarted, so its caches (prefix cache) are empty.
	InstanceID string
}

// RetryAfterError decorates a retryable admission error (ErrQueueFull,
// ErrQueueTimeout, ErrAtCapacity) with the delay the client should wait before
// retrying. Callers read it with errors.As; errors.Is still sees the wrapped
// sentinel.
type RetryAfterError struct {
	Err        error
	RetryAfter time.Duration
}

func (e *RetryAfterError) Error() string { return e.Err.Error() }
func (e *RetryAfterError) Unwrap() error { return e.Err }

// FinishReason explains why generation stopped. It is only set on the last
// chunk of a stream.
type FinishReason string

const (
	FinishNone   FinishReason = ""
	FinishStop   FinishReason = "stop"
	FinishLength FinishReason = "length"
)

// Message is one chat message, as the client sent it (Phase 6a, FI-7): the
// engine applies the model's chat template to these.
type Message struct {
	Role    string
	Content string
	Name    string // optional
}

// InferenceRequest is the unit of work routed by the control plane.
type InferenceRequest struct {
	ID     string
	Model  string
	Prompt string
	// PromptTokens is the (estimated) number of input tokens.
	PromptTokens int
	// MaxNewTokens is the upper bound of generated tokens.
	MaxNewTokens int
	// PrefixHashes are the chained hashes of the prompt's full blocks
	// (prefix.Hashes, prefix.BlockTokens tokens each). The control plane
	// uses them to route by prefix affinity (Phase 3); a worker with a
	// prefix cache may use them to skip the prefill of cached blocks. Nil
	// disables both.
	PrefixHashes []uint64
	// Messages are the client's chat messages, for engines that apply a
	// chat template (vLLM). Prompt is their flattened form, used for token
	// estimation and prefix hashing (Phase 6a, FI-7).
	Messages []Message
	// Params are the client's generation parameters that the control plane
	// does not interpret (temperature, top_p, stop, seed, ...), forwarded
	// to the engine as they came. Fields the platform controls (model,
	// max_tokens, stream, ...) or rejects (n > 1, tools, logprobs, ...)
	// never appear here (Phase 6a, decision 1).
	Params map[string]json.RawMessage
}

// TokenCost is the capacity a request reserves on a worker: its input tokens
// plus the worst-case number of generated tokens. This pessimistic reservation
// is a deliberate choice for now (README §9, FI-1).
func (r InferenceRequest) TokenCost() int {
	return r.PromptTokens + r.MaxNewTokens
}

// Usage is the engine's own token count for a request (Phase 6a, FI-8).
type Usage struct {
	PromptTokens     int
	CompletionTokens int
}

// TokenChunk is one streamed piece of generated output.
type TokenChunk struct {
	// Index is the 0-based position of the chunk in the stream. A chunk is
	// usually one token, but an engine may group several (FI-8): count
	// tokens with Usage when it is present.
	Index        int
	Text         string
	FinishReason FinishReason
	// Usage is set on the last chunk by workers that know the real token
	// counts (vLLM); nil otherwise (the simulator).
	Usage *Usage
}

// WorkerSnapshot is a point-in-time, read-only view of a worker's state. It is
// informational only: admission decisions must go through AcquireLease.
type WorkerSnapshot struct {
	WorkerID       string   `json:"worker_id"`
	Models         []string `json:"models"`
	TokenBudget    int      `json:"token_budget"`
	TokensReserved int      `json:"tokens_reserved"`
	ActiveLeases   int      `json:"active_leases"`
	LeasesGranted  uint64   `json:"leases_granted"`
	LeasesRejected uint64   `json:"leases_rejected"`
	// MaxRequestTokens is the largest prompt + max_tokens one request may
	// have when it is below TokenBudget (vLLM's max_model_len); 0 means
	// TokenBudget (Phase 6a).
	MaxRequestTokens int `json:"max_request_tokens,omitempty"`
	// Prefix cache (Phase 3), the worker's own ground truth. Zero when the
	// worker has no prefix cache. PrefixBlocksHit / PrefixBlocksQueried is
	// the real cache hit rate, to compare with the router's prediction.
	PrefixCacheBlocks   int    `json:"prefix_cache_blocks,omitempty"`
	PrefixCacheUsed     int    `json:"prefix_cache_used,omitempty"`
	PrefixBlocksQueried uint64 `json:"prefix_blocks_queried,omitempty"`
	PrefixBlocksHit     uint64 `json:"prefix_blocks_hit,omitempty"`
	// Engine is what a real engine reports about itself (vLLM's /metrics,
	// Phase 6a); nil for workers without one (the simulator).
	Engine *EngineStats `json:"engine,omitempty"`
	// Status is the control plane's view of the worker (Phase 4). Workers
	// leave it nil; the scheduler fills it in its own snapshots.
	Status *WorkerStatus `json:"status,omitempty"`
}

// EngineStats is the engine's own view, read periodically (never per
// request): the real KV-cache occupancy, to compare with the ledger's
// pessimistic reservation (FI-1).
type EngineStats struct {
	KVCacheUsage     float64 `json:"kv_cache_usage"` // 0–1
	RequestsRunning  int     `json:"requests_running"`
	RequestsWaiting  int     `json:"requests_waiting"`
	ScrapedAtUnixSec int64   `json:"scraped_at"`
}

// WorkerStatus is the scheduler's view of a worker's lifecycle and health.
type WorkerStatus struct {
	// State is "active" or "draining" (removed, waiting for its leases).
	State string `json:"state"`
	// Healthy is the health-check verdict (with hysteresis).
	Healthy bool `json:"healthy"`
	// Breaker is "closed", "open" or "half-open".
	Breaker string `json:"breaker"`
	// Degraded workers are only used when the others have no room.
	Degraded            bool    `json:"degraded"`
	ITLMs               float64 `json:"itl_ms"`
	ITLSamples          int     `json:"itl_samples"`
	InFlight            int     `json:"in_flight"`
	ConsecutiveFailures int     `json:"consecutive_failures"`
	Failures            uint64  `json:"failures"`
	BreakerOpens        uint64  `json:"breaker_opens"`
	InstanceID          string  `json:"instance_id,omitempty"`
}

// Worker is a data-plane node able to run inference.
type Worker interface {
	ID() string
	Models() []string
	// AcquireLease atomically reserves the request's token cost. It returns
	// ErrAtCapacity when the worker is saturated, ErrRequestTooLarge when the
	// request can never fit and ErrModelNotServed for unknown models.
	AcquireLease(ctx context.Context, req InferenceRequest) (Lease, error)
	Snapshot() WorkerSnapshot
	// CapacityChanged signals that the worker's reserved capacity went down
	// (a lease ended, was released or expired), so a waiting request may now
	// fit. It is a level-triggered hint without data: it fires at most once
	// per finished lease, coalesces (implementations use a channel with
	// buffer 1 and a non-blocking send) and may be lost or duplicated without
	// breaking correctness, because admission always goes through
	// AcquireLease. It must never fire per token or per KV-cache block.
	// Exactly one consumer (the scheduler's dispatcher) reads it.
	CapacityChanged() <-chan struct{}
	// Health checks that the worker can serve (vLLM: GET /health). It must
	// honour ctx's deadline. An error, wrapped in ErrWorkerFailure, means
	// not healthy. The control plane polls it periodically (Phase 4).
	Health(ctx context.Context) (HealthStatus, error)
}

// Lease is a capacity reservation on one worker.
//
// Contract:
//   - ExecuteStream may be called at most once. It sends the generated chunks
//     to out and returns when generation ends, fails or ctx is cancelled.
//   - ExecuteStream never closes out: the caller owns the channel (see
//     StartStream for the usual pattern).
//   - The reserved capacity is freed as soon as ExecuteStream returns, so a
//     cancelled client frees VRAM immediately.
//   - Release is idempotent and safe to call concurrently. Callers must
//     always `defer lease.Release()` to cover the paths where ExecuteStream is
//     never called.
//   - A lease that is not executed within the worker's TTL expires and its
//     capacity is reclaimed; ExecuteStream then returns ErrLeaseExpired.
type Lease interface {
	WorkerID() string
	ExecuteStream(ctx context.Context, out chan<- TokenChunk) error
	Release()
}

// StartStream runs lease.ExecuteStream in its own goroutine. The returned
// channel is closed once the stream ends; wait then returns the stream error.
// Consumers that stop reading early must cancel ctx and keep draining the
// channel until it is closed.
func StartStream(ctx context.Context, lease Lease, buffer int) (chunks <-chan TokenChunk, wait func() error) {
	out := make(chan TokenChunk, buffer)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		errc <- lease.ExecuteStream(ctx, out)
	}()
	return out, func() error { return <-errc }
}
