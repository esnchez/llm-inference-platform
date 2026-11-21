package metrics

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"llm-inference-platform/internal/httpapi"
	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/scheduler"
	"llm-inference-platform/internal/simworker"
)

const model = "m"

// platform wires the real pieces as cmd/platform does: simulated workers,
// a scheduler and the HTTP API, all reporting to one Metrics.
func platform(t *testing.T, workers ...*simworker.Worker) (*Metrics, *httptest.Server) {
	t.Helper()
	m := New()
	ws := make([]inference.Worker, len(workers))
	for i, w := range workers {
		ws[i] = w
	}
	s := scheduler.New(scheduler.Config{
		HealthInterval:  2 * time.Millisecond,
		BreakerFailures: 1,
		Placement:       scheduler.NewPrefixAffinity(scheduler.AffinityConfig{}),
		Observer:        m,
	}, ws...)
	t.Cleanup(s.Close)
	m.Watch(s)
	cfg := httpapi.DefaultConfig()
	cfg.Observer = m
	mux := http.NewServeMux()
	mux.Handle("/", httpapi.New(s, cfg, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	mux.Handle("GET /metrics", m.Handler())
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return m, ts
}

func sim(t *testing.T, id string, f simworker.Faults) *simworker.Worker {
	t.Helper()
	w, err := simworker.New(simworker.Config{ID: id, Models: []string{model}, TokenBudget: 10_000,
		InterTokenLatency: time.Millisecond, PrefixCacheBlocks: 100, Faults: f})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func chat(t *testing.T, url, body string) {
	t.Helper()
	resp, err := http.Post(url+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// waitRecorded waits until n requests were recorded: the handler records a
// request just after the client has read its last byte.
func waitRecorded(t *testing.T, m *Metrics, n float64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		total := 0.0
		families, _ := m.reg.Gather()
		for _, f := range families {
			if f.GetName() == "llmplatform_requests_total" {
				for _, s := range f.GetMetric() {
					total += s.GetCounter().GetValue()
				}
			}
		}
		if total >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%v of %v requests recorded", total, n)
		}
		time.Sleep(time.Millisecond)
	}
}

var system = strings.Repeat("Eres un asistente de soporte. ", 20)

func goodChat(maxTokens string) string {
	return `{"model":"m","stream":true,"max_tokens":` + maxTokens + `,"messages":[{"role":"system","content":"` + system + `"},{"role":"user","content":"hola"}]}`
}

// TestCatalogue drives traffic that touches every area (success, invalid
// requests, a failing worker with retries, breaker and health checks) and
// checks that every metric of docs/operaciones/03-observabilidad.md exists,
// and that the names follow Prometheus conventions.
func TestCatalogue(t *testing.T) {
	bad := sim(t, "bad", simworker.Faults{ErrorRate: 1}) // fails streams, passes health checks
	m, ts := platform(t, bad, sim(t, "good", simworker.Faults{}))
	for range 4 {
		chat(t, ts.URL, goodChat("4"))
	}
	chat(t, ts.URL, `{"model":"nope","max_tokens":4,"messages":[{"role":"user","content":"hola"}]}`)
	chat(t, ts.URL, `not json`)
	waitRecorded(t, m, 6)
	deadline := time.Now().Add(3 * time.Second)
	for testutil.ToFloat64(m.health.WithLabelValues("bad", "ok")) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	families, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, f := range families {
		names = append(names, f.GetName())
	}
	want := []string{
		// Requests.
		"llmplatform_requests_total", "llmplatform_requests_in_flight", "llmplatform_time_to_first_token_seconds",
		"llmplatform_e2e_request_latency_seconds", "llmplatform_prompt_tokens_total", "llmplatform_generation_tokens_total",
		"llmplatform_reserved_tokens_total",
		// Admission and queue.
		"llmplatform_admissions_total", "llmplatform_queue_requests", "llmplatform_queue_tokens", "llmplatform_queue_token_limit",
		"llmplatform_queue_head_age_seconds", "llmplatform_queue_barrier_wakeups_total", "llmplatform_model_workers",
		// Capacity.
		"llmplatform_worker_token_budget", "llmplatform_worker_tokens_reserved", "llmplatform_worker_leases_active",
		// Prefix routing.
		"llmplatform_placements_total", "llmplatform_prefix_blocks_queried_total", "llmplatform_prefix_blocks_predicted_hit_total",
		"llmplatform_prefix_tree_nodes", "llmplatform_prefix_tree_blocks", "llmplatform_prefix_tree_evicted_blocks_total",
		"llmplatform_worker_prefix_cache_queried_blocks_total", "llmplatform_worker_prefix_cache_hit_blocks_total",
		"llmplatform_worker_prefix_cache_used_blocks", "llmplatform_worker_prefix_cache_capacity_blocks",
		// Health and resilience.
		"llmplatform_worker_time_to_first_token_seconds", "llmplatform_worker_inter_token_latency_seconds",
		"llmplatform_worker_failures_total", "llmplatform_retries_total", "llmplatform_breaker_transitions_total",
		"llmplatform_health_checks_total", "llmplatform_worker_healthy", "llmplatform_worker_breaker_state",
		"llmplatform_worker_degraded", "llmplatform_worker_draining", "llmplatform_worker_itl_ewma_seconds",
		// Process.
		"llmplatform_build_info", "go_goroutines", "process_cpu_seconds_total",
	}
	for _, n := range want {
		if !slices.Contains(names, n) {
			t.Errorf("metric %s missing", n)
		}
	}

	problems, err := testutil.GatherAndLint(m.reg)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		if strings.HasPrefix(p.Metric, "llmplatform_") {
			t.Errorf("lint: %s: %s", p.Metric, p.Text)
		}
	}

	// Spot checks of the values.
	if got := testutil.ToFloat64(m.requests.WithLabelValues(model, "completed")); got != 4 {
		t.Errorf("completed = %v, want 4 (retries are invisible to the client)", got)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues(httpapi.UnknownModel, "rejected_invalid")); got != 2 {
		t.Errorf("rejected_invalid for unknown model = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.genTok.WithLabelValues(model)); got != 16 {
		t.Errorf("generation tokens = %v, want 16", got)
	}
	if got := testutil.ToFloat64(m.inFlight.WithLabelValues(model)); got != 0 {
		t.Errorf("in flight = %v after all requests ended", got)
	}
	if testutil.ToFloat64(m.failures.WithLabelValues("bad", "error")) == 0 ||
		testutil.ToFloat64(m.retries.WithLabelValues(model, "succeeded")) == 0 ||
		testutil.ToFloat64(m.transitions.WithLabelValues("bad", "open")) == 0 {
		t.Error("the failing worker left no trace in failures, retries or breaker transitions")
	}

	// The endpoint serves the text format.
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), `llmplatform_worker_breaker_state{state="open",worker="bad"} 1`) {
		t.Errorf("/metrics does not show the open breaker:\n%s", body)
	}
}

func TestHistogramsObserveBothTTFTs(t *testing.T) {
	m, ts := platform(t, sim(t, "w", simworker.Faults{}))
	chat(t, ts.URL, goodChat("5"))
	waitRecorded(t, m, 1)
	if n := testutil.CollectAndCount(m.ttft); n != 1 {
		t.Fatalf("client TTFT series = %d", n)
	}
	h := func(name string) uint64 {
		families, _ := m.reg.Gather()
		for _, f := range families {
			if f.GetName() == name {
				return f.GetMetric()[0].GetHistogram().GetSampleCount()
			}
		}
		return 0
	}
	if h("llmplatform_time_to_first_token_seconds") != 1 || h("llmplatform_worker_time_to_first_token_seconds") != 1 {
		t.Fatal("one request must give one client TTFT and one worker TTFT")
	}
	if got := h("llmplatform_worker_inter_token_latency_seconds"); got != 4 {
		t.Fatalf("ITL observations = %d, want 4 (5 tokens)", got)
	}
	if h("llmplatform_e2e_request_latency_seconds") != 1 {
		t.Fatal("e2e not observed")
	}
}

func TestQueueMetrics(t *testing.T) {
	w, err := simworker.New(simworker.Config{ID: "w", Models: []string{model}, TokenBudget: 100, InterTokenLatency: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	m, ts := platform(t, w)
	done := make(chan struct{})
	go func() { // holds the whole budget for ~1s
		chat(t, ts.URL, `{"model":"m","stream":true,"max_tokens":50,"messages":[{"role":"user","content":"`+strings.Repeat("x", 190)+`"}]}`)
		close(done)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for testutil.ToFloat64(m.inFlight.WithLabelValues(model)) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	chat(t, ts.URL, `{"model":"m","stream":true,"max_tokens":20,"messages":[{"role":"user","content":"hola"}]}`)
	<-done
	waitRecorded(t, m, 2)
	if got := testutil.ToFloat64(m.admissions.WithLabelValues(model, "queued")); got != 1 {
		t.Fatalf("queued admissions = %v, want 1", got)
	}
	if n := testutil.CollectAndCount(m.queueWait); n != 1 {
		t.Fatalf("queue wait series = %d", n)
	}
}

// TestEventCountersStartAtZero: the first breaker opening of a worker must
// be visible to increase(), so its series exists at 0 from the first scrape.
func TestEventCountersStartAtZero(t *testing.T) {
	m, _ := platform(t, sim(t, "w", simworker.Faults{}))
	if _, err := m.reg.Gather(); err != nil { // a scrape
		t.Fatal(err)
	}
	for name, c := range map[string]interface {
		Collect(chan<- prometheus.Metric)
	}{
		"breaker transitions": m.transitions, "worker failures": m.failures, "restarts": m.restarts,
		"requests": m.requests, "retries": m.retries,
	} {
		if n := testutil.CollectAndCount(c.(prometheus.Collector)); n == 0 {
			t.Errorf("%s: no series before the first event", name)
		}
	}
	if got := testutil.ToFloat64(m.transitions.WithLabelValues("w", "open")); got != 0 {
		t.Fatalf("breaker open = %v", got)
	}
}
