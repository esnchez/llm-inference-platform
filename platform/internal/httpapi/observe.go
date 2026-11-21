package httpapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"llm-inference-platform/internal/inference"
)

// Observer receives one record per request, for metrics (Phase 5,
// docs/servicio/11-fase-5-decisiones.md, decision 2). internal/metrics
// implements it with Prometheus. Implementations must be fast and safe for
// concurrent use.
type Observer interface {
	// RequestStarted: a request was admitted (got a lease) and is now being
	// answered.
	RequestStarted(model string)
	// RequestFinished: a request ended, admitted or not. If rec.Admitted,
	// RequestStarted was called for it before.
	RequestFinished(rec RequestRecord)
}

// RequestRecord describes how one request went.
type RequestRecord struct {
	// Model is the requested model, or "unknown" when no worker serves it
	// (so that clients cannot create metric series at will).
	Model string
	// Outcome is one of the Outcome* constants.
	Outcome  string
	Admitted bool
	// FirstToken says whether any token was sent; TTFT is then the time
	// from the arrival of the request to the first one. (A flag, not
	// TTFT > 0: with a coarse clock a fast TTFT can measure 0.)
	FirstToken bool
	TTFT       time.Duration
	Duration   time.Duration
	// Token counts of admitted requests: prompt, generated and reserved in
	// the worker's ledger (estimated prompt + max_tokens). Prompt and
	// completion are the engine's own counts when UsageReported (vLLM,
	// FI-8); otherwise the estimate and the number of chunks.
	PromptTokens, CompletionTokens, ReservedTokens int
	UsageReported                                  bool
	// EstimatedPromptTokens is tokens.Estimate's count, set when
	// UsageReported, to measure the estimation error (FI-3).
	EstimatedPromptTokens int
}

// Outcomes of a request: each request ends in exactly one.
const (
	OutcomeCompleted         = "completed"
	OutcomeCancelled         = "cancelled"
	OutcomeRejectedInvalid   = "rejected_invalid"
	OutcomeQueueFull         = "rejected_queue_full"
	OutcomeQueueTimeout      = "rejected_queue_timeout"
	OutcomeNoHealthyWorkers  = "rejected_no_healthy_workers"
	OutcomeWorkerUnavailable = "worker_unavailable"
	OutcomeFailedMidstream   = "failed_midstream"
	OutcomeError             = "error"
)

// UnknownModel labels requests for models that no worker serves.
const UnknownModel = "unknown"

type nopObserver struct{}

func (nopObserver) RequestStarted(string)         {}
func (nopObserver) RequestFinished(RequestRecord) {}

// failureOutcome classifies an error that ended a request before any token
// reached the client (at admission or before the first token).
func failureOutcome(ctx context.Context, err error) string {
	switch {
	case ctx.Err() != nil, errors.Is(err, context.Canceled):
		return OutcomeCancelled
	case errors.Is(err, inference.ErrQueueFull), errors.Is(err, inference.ErrAtCapacity):
		return OutcomeQueueFull
	case errors.Is(err, inference.ErrQueueTimeout):
		return OutcomeQueueTimeout
	case errors.Is(err, inference.ErrNoHealthyWorkers):
		return OutcomeNoHealthyWorkers
	case errors.Is(err, inference.ErrWorkerFailure):
		return OutcomeWorkerUnavailable
	case errors.Is(err, inference.ErrModelNotFound), errors.Is(err, inference.ErrRequestTooLarge),
		errors.Is(err, inference.ErrInvalidRequest):
		return OutcomeRejectedInvalid
	default:
		return OutcomeError
	}
}

// leaseDetails is implemented by the scheduler's lease (Phase 5): what the
// per-request log reports about the queue, retries and the worker's TTFT.
type leaseDetails interface {
	ReservedTokens() int
	QueueWait() time.Duration
	Retries() int
	WorkerTTFT() time.Duration
}

// statusWriter records the status code written, for the log. It unwraps
// to the original writer, so http.ResponseController can still flush.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// clientRequestID returns the client's X-Request-Id if it is safe to log and
// echo (1–64 characters of [A-Za-z0-9._-]), or "".
func clientRequestID(v string) string {
	if len(v) == 0 || len(v) > 64 {
		return ""
	}
	for _, c := range v {
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-'
		if !ok {
			return ""
		}
	}
	return v
}
