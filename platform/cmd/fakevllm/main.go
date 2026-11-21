// Command fakevllm runs the fake vLLM server (internal/vllm/fakevllm) on its
// own: the platform talks to it over HTTP exactly as it would to vLLM
// (Phase 6a). Break it from outside with POST /fake/faults (JSON body, e.g.
// {"status":500} or {"hang":true}; {} clears) and POST /fake/restart.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"llm-inference-platform/internal/vllm/fakevllm"
)

func main() {
	var (
		addr       = flag.String("addr", ":8000", "listen address")
		model      = flag.String("model", "sim-llama-3-8b", "served model name")
		maxLen     = flag.Int("max-model-len", 4096, "max_model_len reported in /v1/models")
		blocks     = flag.Int("num-gpu-blocks", 1024, "num_gpu_blocks reported in vllm:cache_config_info")
		blockSize  = flag.Int("block-size", 16, "block_size reported in vllm:cache_config_info")
		ttft       = flag.Duration("ttft", 50*time.Millisecond, "time to first token")
		itl        = flag.Duration("itl", 20*time.Millisecond, "time between tokens")
		output     = flag.Int("output-tokens", 0, "cap on generated tokens; 0 = max_tokens")
		apiKey     = flag.String("api-key", "", "required Bearer token, if set")
		noProcess  = flag.Bool("no-process-metrics", false, "omit process_start_time_seconds (vLLM in multiprocess mode)")
		loadingFor = flag.Duration("loading-for", 0, "answer 503 for this long at start, like vLLM loading a model")
		grace      = flag.Duration("shutdown-grace", 50*time.Second, "on SIGTERM: time given to the streams in flight")
	)
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	f := fakevllm.New(fakevllm.Config{Model: *model, MaxModelLen: *maxLen, NumGPUBlocks: *blocks, BlockSize: *blockSize,
		TTFT: *ttft, TokenDelay: *itl, OutputTokens: *output, APIKey: *apiKey, NoProcessMetrics: *noProcess})
	if *loadingFor > 0 {
		f.SetFaults(fakevllm.Faults{Loading: true})
		time.AfterFunc(*loadingFor, func() { f.SetFaults(fakevllm.Faults{}); log.Info("model loaded") })
	}
	log.Info("fake vLLM listening", "addr", *addr, "model", *model, "token_budget", *blocks**blockSize)
	srv := &http.Server{Addr: *addr, Handler: f.Handler(), ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()

	// Orderly scale-down (Phase 7, decision 4): on SIGTERM, /health stops
	// answering 200 so the platform sends nothing new, and the streams in
	// flight finish before the process exits (Kubernetes waits for
	// terminationGracePeriodSeconds, which must exceed -shutdown-grace).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-errc:
		log.Error("fake vLLM stopped", "error", err)
		os.Exit(1)
	case <-ctx.Done():
	}
	log.Info("SIGTERM: unhealthy, finishing streams", "grace", grace.String())
	f.SetFaults(fakevllm.Faults{Unhealthy: true})
	sctx, cancel := context.WithTimeout(context.Background(), *grace)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Warn("grace period over: streams cut", "error", err)
		return
	}
	log.Info("fake vLLM stopped cleanly")
}
