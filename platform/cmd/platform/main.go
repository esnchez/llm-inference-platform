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
	"llm-inference-platform/internal/prefix"
	"llm-inference-platform/internal/scheduler"
	"llm-inference-platform/internal/simworker"
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
		shutdownGrace = flag.Duration("shutdown-grace", 15*time.Second, "time given to in-flight streams on shutdown")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	modelList := strings.Split(*models, ",")
	cacheBlocks := *prefixCache
	if cacheBlocks < 0 {
		cacheBlocks = *budget / prefix.BlockTokens
	}
	workers := make([]inference.Worker, 0, *numWorkers)
	for i := 0; i < *numWorkers; i++ {
		w, err := simworker.New(simworker.Config{
			ID:                fmt.Sprintf("sim-worker-%d", i+1),
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

	cfg := httpapi.DefaultConfig()
	cfg.MaxTokensLimit = *maxTokens
	sched := scheduler.New(scheduler.Config{
		QueueTimeout:      *queueTimeout,
		HeadMaxAge:        *headMaxAge,
		QueueTokenFactor:  *queueFactor,
		MaxQueuedRequests: *maxQueued,
		Placement:         place,
	}, workers...)
	defer sched.Close()
	api := httpapi.New(sched, cfg, log)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: it would cut long-lived SSE streams.
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 1)
	go func() {
		log.Info("platform listening", "addr", *addr, "workers", *numWorkers, "models", modelList,
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
