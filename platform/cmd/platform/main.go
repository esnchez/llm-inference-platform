// Command platform runs the control plane with a pool of simulated workers.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"llm-inference-platform/internal/httpapi"
	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/metrics"
	"llm-inference-platform/internal/prefix"
	"llm-inference-platform/internal/scheduler"
	"llm-inference-platform/internal/simworker"
	"llm-inference-platform/internal/vllm"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "platform:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		addr          = flag.String("addr", ":8080", "HTTP listen address")
		numWorkers    = flag.Int("workers", 3, "number of simulated workers")
		models        = flag.String("models", "sim-llama-3-8b", "comma-separated models served by every worker")
		budget        = flag.Int("token-budget", 16384, "simulated KV-cache budget per worker, in tokens")
		baseTTFT      = flag.Duration("ttft", 50*time.Millisecond, "simulated base time to first token")
		prefill       = flag.Duration("prefill-per-token", 20*time.Microsecond, "simulated prefill cost per prompt token")
		itl           = flag.Duration("itl", 20*time.Millisecond, "simulated inter-token latency")
		leaseTTL      = flag.Duration("lease-ttl", 5*time.Second, "max time between AcquireLease and ExecuteStream")
		meanOutput    = flag.Int("mean-output-tokens", 200, "mean simulated output length (exponential, capped at max_tokens); 0 = always max_tokens")
		itlLoad       = flag.Float64("itl-load-factor", 0.05, "ITL grows by this fraction per extra active lease; 0 = constant ITL")
		queueTimeout  = flag.Duration("queue-timeout", 5*time.Second, "max time a request waits in the queue (interactive traffic)")
		headMaxAge    = flag.Duration("head-max-age", 0, "age after which the queue head may not be passed (FIFO barrier); 0 = queue-timeout/2")
		queueFactor   = flag.Float64("queue-token-factor", 2, "per-model queue limit, as a multiple of the model's total token budget")
		maxQueued     = flag.Int("max-queued", 1024, "max waiting requests across all models")
		maxTokens     = flag.Int("max-tokens-limit", 4096, "largest max_tokens accepted from clients")
		placement     = flag.String("placement", "prefix", "worker choice: prefix (KV-cache prefix affinity) or round-robin")
		affinityLoad  = flag.Float64("affinity-max-load", 0.8, "ledger load above which a worker stops getting requests for prefix affinity alone")
		affinityMin   = flag.Int("affinity-min-blocks", 4, "shortest cached prefix that counts as affinity, in blocks of 16 tokens")
		prefixCache   = flag.Int("prefix-cache-blocks", -1, "simulated prefix cache per worker, in blocks of 16 tokens; -1 = token-budget/16, 0 = disabled")
		healthEvery   = flag.Duration("health-interval", 2*time.Second, "how often each worker is health-checked; negative = no health checks")
		breakerFails  = flag.Int("breaker-failures", 5, "consecutive worker failures that open its circuit breaker")
		breakerWait   = flag.Duration("breaker-cooldown", 10*time.Second, "how long a breaker stays open before a probe (doubles on each reopening, max 2m)")
		ttftTimeout   = flag.Duration("ttft-timeout", 10*time.Second, "a stream with no first token after this (+ -ttft-timeout-per-token per prompt token) is a hung worker")
		ttftPerToken  = flag.Duration("ttft-timeout-per-token", time.Millisecond, "extra TTFT timeout per prompt token; on CPU vLLM use ~25ms (prefill is ~15ms/token)")
		stallTimeout  = flag.Duration("stall-timeout", 10*time.Second, "a stream with no new token for this long is a hung worker")
		maxRetries    = flag.Int("max-retries", 1, "retries on another worker when one fails before the first token; -1 = none")
		shutdownGrace = flag.Duration("shutdown-grace", 15*time.Second, "time given to in-flight streams on shutdown")
		vllmURLs      = flag.String("vllm", "", "comma-separated vLLM server URLs (e.g. http://gpu-1:8000); each is registered once it is ready. Combine with -workers 0 for vLLM only")
		vllmKey       = flag.String("vllm-api-key", os.Getenv("VLLM_API_KEY"), "API key sent to vLLM as a Bearer token (default $VLLM_API_KEY)")
		vllmBudget    = flag.Int("vllm-token-budget", 0, "token budget of each vLLM worker; 0 = discover it from vllm:cache_config_info")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	modelList := strings.Split(*models, ",")
	cacheBlocks := *prefixCache
	if cacheBlocks < 0 {
		cacheBlocks = *budget / prefix.BlockTokens
	}
	pool := newSimPool(simworker.Config{
		Models:            modelList,
		TokenBudget:       *budget,
		BaseTTFT:          *baseTTFT,
		PrefillPerToken:   *prefill,
		InterTokenLatency: *itl,
		LeaseTTL:          *leaseTTL,
		MeanOutputTokens:  *meanOutput,
		ITLLoadFactor:     *itlLoad,
		PrefixCacheBlocks: cacheBlocks,
	})
	workers := make([]inference.Worker, 0, *numWorkers)
	for range *numWorkers {
		w, err := pool.newWorker()
		if err != nil {
			return err
		}
		workers = append(workers, w)
	}

	var place scheduler.Placement
	switch *placement {
	case "prefix":
		place = scheduler.NewPrefixAffinity(scheduler.AffinityConfig{
			MaxLoad:        *affinityLoad,
			MinMatchBlocks: *affinityMin,
		})
	case "round-robin":
		place = &scheduler.RoundRobin{}
	default:
		return fmt.Errorf("unknown -placement %q (want prefix or round-robin)", *placement)
	}

	// Phase 5: one Prometheus registry, fed by the API and the scheduler.
	mtr := metrics.New()
	cfg := httpapi.DefaultConfig()
	cfg.MaxTokensLimit = *maxTokens
	cfg.Observer = mtr
	sched := scheduler.New(scheduler.Config{
		QueueTimeout:      *queueTimeout,
		HeadMaxAge:        *headMaxAge,
		QueueTokenFactor:  *queueFactor,
		MaxQueuedRequests: *maxQueued,
		Placement:         place,

		HealthInterval:      *healthEvery,
		BreakerFailures:     *breakerFails,
		BreakerCooldown:     *breakerWait,
		TTFTTimeout:         *ttftTimeout,
		TTFTTimeoutPerToken: *ttftPerToken,
		StallTimeout:        *stallTimeout,
		MaxRetries:          *maxRetries,

		Observer: mtr,
		Logger:   log.With("component", "scheduler"),
	}, workers...)
	defer sched.Close()
	mtr.Watch(sched)
	pool.sched = sched
	api := httpapi.New(sched, cfg, log)

	mux := http.NewServeMux()
	mux.Handle("/", api.Handler())
	mux.Handle("GET /metrics", mtr.Handler()) // same port as the API for now (README §9, FI-10)
	pool.register(mux)                        // POST/DELETE /debug/workers...: add, drain, break, restart

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: it would cut long-lived SSE streams.
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Phase 6a: one connector per vLLM server; it registers the worker in
	// the scheduler when the server is ready (it may be loading the model).
	for _, u := range strings.Split(*vllmURLs, ",") {
		if u = strings.TrimSpace(u); u == "" {
			continue
		}
		c := &vllm.Connector{
			Config:   vllm.Config{BaseURL: u, APIKey: *vllmKey, TokenBudget: *vllmBudget, LeaseTTL: *leaseTTL},
			Registry: sched,
			Log:      log.With("component", "vllm-connector"),
		}
		go c.Run(ctx)
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("platform listening", "addr", *addr, "workers", *numWorkers, "vllm", *vllmURLs, "models", modelList,
			"token_budget", *budget, "placement", *placement, "prefix_cache_blocks", cacheBlocks)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down", "grace", shutdownGrace.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		// Grace period over: drop the remaining streams (their contexts are
		// cancelled, which releases their leases).
		_ = srv.Close()
		if !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
	}
	return nil
}
