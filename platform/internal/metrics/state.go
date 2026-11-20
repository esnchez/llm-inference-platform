package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"llm-inference-platform/internal/scheduler"
)

// stateCollector reads the scheduler's current state on every scrape and
// exposes it as gauges (and as counters for the cumulative totals the
// scheduler already keeps). It is "unchecked" (Describe sends nothing)
// because the set of workers and models changes at run time.
type stateCollector struct {
	src Source
	m   *Metrics
}

func desc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(ns, "", name), help, labels, nil)
}

var (
	// Admission and queue.
	dQueueRequests   = desc("queue_requests", "Requests waiting in the model's queue.", "model")
	dQueueTokens     = desc("queue_tokens", "Tokens waiting in the model's queue.", "model")
	dQueueTokenLimit = desc("queue_token_limit", "Limit of tokens in the model's queue.", "model")
	dQueueHeadAge    = desc("queue_head_age_seconds", "Age of the request at the head of the model's queue.", "model")
	dModelWorkers    = desc("model_workers", "Active workers serving the model.", "model")
	dBarrierWakeups  = desc("queue_barrier_wakeups_total", "Dispatcher wake-ups held back by the FIFO barrier.")

	// Capacity.
	dBudget   = desc("worker_token_budget", "Worker token budget (simulated KV cache).", "worker")
	dReserved = desc("worker_tokens_reserved", "Tokens reserved now in the worker's ledger.", "worker")
	dLeases   = desc("worker_leases_active", "Active leases on the worker.", "worker")

	// Prefix routing: the router's view (PrefixAffinity) ...
	dPlacements    = desc("placements_total", "Placements by kind: affinity, overflow or load.", "kind")
	dPrefixQueried = desc("prefix_blocks_queried_total", "Prefix blocks of the placed requests.")
	dPrefixPredict = desc("prefix_blocks_predicted_hit_total", "Prefix blocks the router expected to be cached on the chosen worker.")
	dTreeNodes     = desc("prefix_tree_nodes", "Nodes of the router's radix tree.")
	dTreeBlocks    = desc("prefix_tree_blocks", "Blocks the radix tree attributes to each worker.", "worker")
	dTreeEvicted   = desc("prefix_tree_evicted_blocks_total", "Blocks evicted from the radix tree.")
	// ... and the workers' ground truth.
	dCacheQueried  = desc("worker_prefix_cache_queried_blocks_total", "Blocks looked up in the worker's prefix cache.", "worker")
	dCacheHit      = desc("worker_prefix_cache_hit_blocks_total", "Blocks found in the worker's prefix cache.", "worker")
	dCacheUsed     = desc("worker_prefix_cache_used_blocks", "Blocks held in the worker's prefix cache.", "worker")
	dCacheCapacity = desc("worker_prefix_cache_capacity_blocks", "Capacity of the worker's prefix cache.", "worker")

	// Health and resilience.
	dHealthy  = desc("worker_healthy", "1 if the health checks consider the worker healthy.", "worker")
	dBreaker  = desc("worker_breaker_state", "1 for the worker's current circuit breaker state, 0 for the others.", "worker", "state")
	dDegraded = desc("worker_degraded", "1 if the worker's inter-token latency exceeds 2x its peers' median.", "worker")
	dDraining = desc("worker_draining", "1 while the worker drains after removal.", "worker")
	dITLEWMA  = desc("worker_itl_ewma_seconds", "Mean inter-token latency the scheduler uses for the degraded mark.", "worker")
)

var breakerStates = []string{"closed", "open", "half_open"}

func (c *stateCollector) Describe(chan<- *prometheus.Desc) {}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	counter := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, labels...)
	}
	b2f := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}

	st := c.src.Stats()
	for _, m := range st.Models {
		c.m.initModel(m.Model)
		gauge(dQueueRequests, float64(m.QueuedRequests), m.Model)
		gauge(dQueueTokens, float64(m.QueuedTokens), m.Model)
		gauge(dQueueTokenLimit, float64(m.QueueTokenLimit), m.Model)
		gauge(dQueueHeadAge, float64(m.HeadAgeMs)/1000, m.Model)
		gauge(dModelWorkers, float64(m.Workers), m.Model)
	}
	counter(dBarrierWakeups, float64(st.BarrierWakeups))

	if a, ok := st.Placement.(scheduler.AffinityStats); ok {
		load := a.Placements - a.AffinityHits - a.AffinityOverflows
		counter(dPlacements, float64(a.AffinityHits), "affinity")
		counter(dPlacements, float64(a.AffinityOverflows), "overflow")
		counter(dPlacements, float64(load), "load")
		counter(dPrefixQueried, float64(a.QueriedBlocks))
		counter(dPrefixPredict, float64(a.PredictedHitBlocks))
		gauge(dTreeNodes, float64(a.Tree.Nodes))
		for w, b := range a.Tree.BlocksPerWorker {
			gauge(dTreeBlocks, float64(b), w)
		}
		counter(dTreeEvicted, float64(a.Tree.EvictedBlocks))
	}

	for _, s := range c.src.Snapshots() {
		w := s.WorkerID
		c.m.initWorker(w)
		gauge(dBudget, float64(s.TokenBudget), w)
		gauge(dReserved, float64(s.TokensReserved), w)
		gauge(dLeases, float64(s.ActiveLeases), w)
		if s.PrefixCacheBlocks > 0 {
			counter(dCacheQueried, float64(s.PrefixBlocksQueried), w)
			counter(dCacheHit, float64(s.PrefixBlocksHit), w)
			gauge(dCacheUsed, float64(s.PrefixCacheUsed), w)
			gauge(dCacheCapacity, float64(s.PrefixCacheBlocks), w)
		}
		if st := s.Status; st != nil {
			gauge(dHealthy, b2f(st.Healthy), w)
			state := st.Breaker
			if state == "half-open" {
				state = "half_open"
			}
			for _, b := range breakerStates {
				gauge(dBreaker, b2f(b == state), w, b)
			}
			gauge(dDegraded, b2f(st.Degraded), w)
			gauge(dDraining, b2f(st.State == "draining"), w)
			gauge(dITLEWMA, st.ITLMs/1000, w)
		}
	}
}
