// Package metrics exposes the platform's Prometheus metrics (Phase 5,
// docs/servicio/11-fase-5-decisiones.md and the catalogue in
// docs/servicio/12-observabilidad.md).
//
// It is the only package, with cmd/platform, that imports Prometheus. Two
// mechanisms, by kind of data (decision 2):
//
//   - Events (a request finished, a token arrived, a breaker opened) arrive
//     through the httpapi.Observer and scheduler.Observer interfaces, which
//     Metrics implements with counters and histograms.
//   - State (queue depth, reserved tokens, breaker states, prefix tree) is
//     read from the scheduler on every scrape by a collector, from the same
//     Stats and Snapshots that /debug serves, so both always agree.
package metrics

import (
	"net/http"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"llm-inference-platform/internal/httpapi"
	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/scheduler"
)

const ns = "llmplatform"

// Histogram buckets, aligned with vLLM's in the useful range so that
// percentiles can be compared with the engine's in Phase 6.
var (
	ttftBuckets  = []float64{0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 20, 40}
	itlBuckets   = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.75, 1, 2.5}
	e2eBuckets   = []float64{0.25, 0.5, 1, 2.5, 5, 10, 15, 20, 30, 40, 50, 60, 120}
	queueBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
)

// Source is what the state collector reads: *scheduler.Scheduler.
type Source interface {
	Stats() scheduler.Stats
	Snapshots() []inference.WorkerSnapshot
}

// Metrics owns a Prometheus registry with every platform metric. Create it
// with New, pass it as the Observer of the scheduler and of the HTTP API,
// attach the scheduler with Watch, and serve Handler on /metrics.
type Metrics struct {
	reg *prometheus.Registry

	// Requests (httpapi.Observer).
	requests   *prometheus.CounterVec
	inFlight   *prometheus.GaugeVec
	ttft       *prometheus.HistogramVec
	e2e        *prometheus.HistogramVec
	promptTok  *prometheus.CounterVec
	genTok     *prometheus.CounterVec
	reservedTk *prometheus.CounterVec

	// Scheduler events (scheduler.Observer).
	admissions  *prometheus.CounterVec
	queueWait   *prometheus.HistogramVec
	workerTTFT  *prometheus.HistogramVec
	workerITL   *prometheus.HistogramVec
	failures    *prometheus.CounterVec
	retries     *prometheus.CounterVec
	transitions *prometheus.CounterVec
	health      *prometheus.CounterVec
	restarts    *prometheus.CounterVec
}

var (
	_ httpapi.Observer   = (*Metrics)(nil)
	_ scheduler.Observer = (*Metrics)(nil)
)

// New creates the registry with the event metrics, the Go runtime and
// process collectors, and llmplatform_build_info.
func New() *Metrics {
	m := &Metrics{reg: prometheus.NewRegistry()}
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: name, Help: help}, labels)
	}
	hist := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: name, Help: help, Buckets: buckets}, labels)
	}

	m.requests = counter("requests_total", "Requests finished, by outcome (each request ends in exactly one).", "model", "outcome")
	m.inFlight = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: "requests_in_flight",
		Help: "Admitted requests being answered now."}, []string{"model"})
	m.ttft = hist("time_to_first_token_seconds", "From the arrival of the request to the first token sent to the client (includes queue and retries).", ttftBuckets, "model")
	m.e2e = hist("e2e_request_latency_seconds", "From the arrival of the request to its last byte, completed requests only.", e2eBuckets, "model")
	m.promptTok = counter("prompt_tokens_total", "Prompt tokens (estimated) of admitted requests.", "model")
	m.genTok = counter("generation_tokens_total", "Tokens generated and sent to clients.", "model")
	m.reservedTk = counter("reserved_tokens_total", "Tokens reserved in worker ledgers by admitted requests (prompt + max_tokens).", "model")

	m.admissions = counter("admissions_total", "Requests that got a lease, at once or after queueing.", "model", "path")
	m.queueWait = hist("queue_wait_seconds", "Time spent in the queue by requests leaving it.", queueBuckets, "model", "outcome")
	m.workerTTFT = hist("worker_time_to_first_token_seconds", "From the start of a stream on a worker to its first token (prefill, no queue).", ttftBuckets, "worker")
	m.workerITL = hist("worker_inter_token_latency_seconds", "Time between consecutive tokens of a stream.", itlBuckets, "worker")
	m.failures = counter("worker_failures_total", "Failures attributed to a worker: error, ttft_timeout or stall_timeout.", "worker", "kind")
	m.retries = counter("retries_total", "Requests retried on another worker, by final result.", "model", "result")
	m.transitions = counter("breaker_transitions_total", "Circuit breaker state changes.", "worker", "to")
	m.health = counter("health_checks_total", "Worker health checks, by result.", "worker", "result")
	m.restarts = counter("worker_restarts_detected_total", "Worker restarts detected through a new instance ID.", "worker")

	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: "build_info",
		Help: "Always 1; the labels say which build is running."}, []string{"version", "go_version"})
	version := "devel"
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		version = bi.Main.Version
	}
	buildInfo.WithLabelValues(version, runtime.Version()).Set(1)

	m.reg.MustRegister(
		m.requests, m.inFlight, m.ttft, m.e2e, m.promptTok, m.genTok, m.reservedTk,
		m.admissions, m.queueWait, m.workerTTFT, m.workerITL, m.failures, m.retries,
		m.transitions, m.health, m.restarts, buildInfo,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Watch registers the collector that reads the scheduler's state on every
// scrape. Call it once, after creating the scheduler.
func (m *Metrics) Watch(src Source) {
	m.reg.MustRegister(&stateCollector{src: src, m: m})
}

// Event counters are created at 0 for every known worker and model (on each
// scrape; idempotent). Otherwise a series would be born at 1 with its first
// event, and increase()/rate() would never see that first event: an
// annotation for the first breaker opening of a worker would stay empty.
var (
	outcomes       = []string{"completed", "cancelled", "rejected_invalid", "rejected_queue_full", "rejected_queue_timeout", "rejected_no_healthy_workers", "worker_unavailable", "failed_midstream", "error"}
	failureKinds   = []string{"error", "ttft_timeout", "stall_timeout"}
	breakerTargets = []string{"open", "half_open", "closed"}
)

func (m *Metrics) initModel(model string) {
	for _, o := range outcomes {
		m.requests.WithLabelValues(model, o)
	}
	for _, r := range []string{"succeeded", "failed"} {
		m.retries.WithLabelValues(model, r)
	}
	for _, p := range []string{"immediate", "queued"} {
		m.admissions.WithLabelValues(model, p)
	}
	m.promptTok.WithLabelValues(model)
	m.genTok.WithLabelValues(model)
	m.reservedTk.WithLabelValues(model)
	m.inFlight.WithLabelValues(model)
}

func (m *Metrics) initWorker(worker string) {
	for _, k := range failureKinds {
		m.failures.WithLabelValues(worker, k)
	}
	for _, to := range breakerTargets {
		m.transitions.WithLabelValues(worker, to)
	}
	for _, r := range []string{"ok", "failed"} {
		m.health.WithLabelValues(worker, r)
	}
	m.restarts.WithLabelValues(worker)
}

// Handler serves the metrics in the Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

// Registry exposes the registry (tests, extra collectors).
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// ---- httpapi.Observer ----

func (m *Metrics) RequestStarted(model string) { m.inFlight.WithLabelValues(model).Inc() }

func (m *Metrics) RequestFinished(r httpapi.RequestRecord) {
	m.requests.WithLabelValues(r.Model, r.Outcome).Inc()
	if r.Admitted {
		m.inFlight.WithLabelValues(r.Model).Dec()
		m.promptTok.WithLabelValues(r.Model).Add(float64(r.PromptTokens))
		m.reservedTk.WithLabelValues(r.Model).Add(float64(r.ReservedTokens))
	}
	if r.CompletionTokens > 0 {
		m.genTok.WithLabelValues(r.Model).Add(float64(r.CompletionTokens))
	}
	if r.FirstToken {
		m.ttft.WithLabelValues(r.Model).Observe(r.TTFT.Seconds())
	}
	if r.Outcome == httpapi.OutcomeCompleted {
		m.e2e.WithLabelValues(r.Model).Observe(r.Duration.Seconds())
	}
}

// ---- scheduler.Observer ----

func (m *Metrics) Admitted(model, path string) { m.admissions.WithLabelValues(model, path).Inc() }

func (m *Metrics) QueueLeft(model, outcome string, wait time.Duration) {
	m.queueWait.WithLabelValues(model, outcome).Observe(wait.Seconds())
}

func (m *Metrics) WorkerFirstToken(worker string, ttft time.Duration) {
	m.workerTTFT.WithLabelValues(worker).Observe(ttft.Seconds())
}

func (m *Metrics) WorkerTokenGap(worker string, gap time.Duration) {
	m.workerITL.WithLabelValues(worker).Observe(gap.Seconds())
}

func (m *Metrics) WorkerFailure(worker, kind string) { m.failures.WithLabelValues(worker, kind).Inc() }

func (m *Metrics) Retried(model, result string) { m.retries.WithLabelValues(model, result).Inc() }

func (m *Metrics) BreakerTransition(worker, to string) {
	m.transitions.WithLabelValues(worker, to).Inc()
}

func (m *Metrics) HealthCheck(worker string, ok bool) {
	result := "ok"
	if !ok {
		result = "failed"
	}
	m.health.WithLabelValues(worker, result).Inc()
}

func (m *Metrics) RestartDetected(worker string) { m.restarts.WithLabelValues(worker).Inc() }
