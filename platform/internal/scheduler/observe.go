package scheduler

import (
	"context"
	"log/slog"
	"time"
)

// Observer receives the scheduler's events, for metrics (Phase 5,
// docs/servicio/12-fase-5-decisiones.md, decision 2). internal/metrics
// implements it with Prometheus; the scheduler does not depend on any
// metrics library. State that can be read at any time (queues, reserved
// tokens, breaker states) is not reported here: collectors read it from
// Stats and Snapshots.
//
// Methods may be called with the scheduler's lock held and from many
// goroutines: implementations must be fast, safe for concurrent use and
// must not call back into the scheduler.
type Observer interface {
	// Admitted: a request got a lease, at once ("immediate") or after
	// waiting in the queue ("queued").
	Admitted(model, path string)
	// QueueLeft: a request left the queue: "admitted", "timeout" or
	// "cancelled", after waiting wait.
	QueueLeft(model, outcome string, wait time.Duration)
	// WorkerFirstToken: a worker emitted its first token ttft after its
	// stream started (prefill, without queue or retries).
	WorkerFirstToken(worker string, ttft time.Duration)
	// WorkerTokenGap: time between two consecutive tokens of a stream.
	WorkerTokenGap(worker string, gap time.Duration)
	// WorkerFailure: a failure attributed to the worker: "error",
	// "ttft_timeout" or "stall_timeout".
	WorkerFailure(worker, kind string)
	// Retried: a request was retried on another worker; result is
	// "succeeded" or "failed".
	Retried(model, result string)
	// BreakerTransition: a worker's breaker moved to "open", "half_open"
	// or "closed".
	BreakerTransition(worker, to string)
	// HealthCheck: a health check finished.
	HealthCheck(worker string, ok bool)
	// RestartDetected: a worker's instance ID changed.
	RestartDetected(worker string)
}

// NopObserver ignores every event. It is the default.
type NopObserver struct{}

func (NopObserver) Admitted(string, string)                 {}
func (NopObserver) QueueLeft(string, string, time.Duration) {}
func (NopObserver) WorkerFirstToken(string, time.Duration)  {}
func (NopObserver) WorkerTokenGap(string, time.Duration)    {}
func (NopObserver) WorkerFailure(string, string)            {}
func (NopObserver) Retried(string, string)                  {}
func (NopObserver) BreakerTransition(string, string)        {}
func (NopObserver) HealthCheck(string, bool)                {}
func (NopObserver) RestartDetected(string)                  {}

var _ Observer = NopObserver{}

// discardLogger drops everything: the default when Config.Logger is nil.
// (slog.DiscardHandler needs Go 1.24; the module supports 1.23.)
var discardLogger = slog.New(discardHandler{})

type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h discardHandler) WithGroup(string) slog.Handler           { return h }
