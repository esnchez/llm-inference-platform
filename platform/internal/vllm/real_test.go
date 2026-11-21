package vllm

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/scheduler"
)

// TestRealVLLM is the smoke test against a real vLLM server (Phase 6b-CPU,
// decision 7). It only runs when VLLM_URL is set, e.g. with
// `task vllm-cpu:up` then `task vllm-cpu:smoke`; it is slow (CPU) and needs
// a multi-GB image, so it stays out of `task check`.
func TestRealVLLM(t *testing.T) {
	url := os.Getenv("VLLM_URL")
	if url == "" {
		t.Skip("VLLM_URL not set: no real vLLM to test against")
	}
	ctx := context.Background()
	info, err := Discover(ctx, Config{BaseURL: url})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	t.Logf("discovered %+v", info)
	if info.BlockSize <= 0 || info.NumGPUBlocks <= 0 || info.TokenBudget != info.BlockSize*info.NumGPUBlocks || info.MaxModelLen <= 0 {
		t.Fatalf("incomplete discovery: %+v", info)
	}
	w, err := New(Config{BaseURL: url}, info)
	if err != nil {
		t.Fatal(err)
	}
	model := info.Models[0]
	msg := func(text string) []inference.Message { return []inference.Message{{Role: "user", Content: text}} }

	t.Run("stream with the engine's usage", func(t *testing.T) {
		l, err := w.AcquireLease(ctx, inference.InferenceRequest{ID: "smoke-1", Model: model, Prompt: "Di hola",
			PromptTokens: 5, MaxNewTokens: 4, Messages: msg("Di hola")})
		if err != nil {
			t.Fatal(err)
		}
		chunks, wait := inference.StartStream(ctx, l, 8)
		var last inference.TokenChunk
		n := 0
		for c := range chunks {
			last = c
			n++
		}
		if err := wait(); err != nil {
			t.Fatal(err)
		}
		if n < 2 || last.Usage == nil || last.Usage.PromptTokens == 0 || last.Usage.CompletionTokens == 0 {
			t.Fatalf("%d chunks, last %+v", n, last)
		}
		t.Logf("usage: %+v (our estimate was 5 prompt tokens)", *last.Usage)
	})

	t.Run("a prompt over the context is a request error", func(t *testing.T) {
		long := strings.Repeat("palabra ", info.MaxModelLen+10)
		// Our estimate says it fits (we lie on purpose): only vLLM's tokenizer knows.
		l, err := w.AcquireLease(ctx, inference.InferenceRequest{ID: "smoke-2", Model: model, Prompt: "x",
			PromptTokens: 10, MaxNewTokens: 5, Messages: msg(long)})
		if err != nil {
			t.Fatal(err)
		}
		chunks, wait := inference.StartStream(ctx, l, 8)
		for range chunks {
		}
		if err := wait(); !errors.Is(err, inference.ErrRequestTooLarge) {
			t.Fatalf("want ErrRequestTooLarge, got %v", err)
		}
	})

	t.Run("max_tokens over the context is clamped by the scheduler", func(t *testing.T) {
		s := scheduler.New(scheduler.Config{HealthInterval: -1, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, w)
		defer s.Close()
		l, err := s.Acquire(ctx, inference.InferenceRequest{ID: "smoke-3", Model: model, Prompt: "Di hola",
			PromptTokens: 5, MaxNewTokens: info.MaxModelLen * 2, Messages: msg("Di hola")})
		if err != nil {
			t.Fatalf("a too large max_tokens must be clamped, not rejected: %v", err)
		}
		defer l.Release()
		if r := l.(interface{ ReservedTokens() int }).ReservedTokens(); r != info.MaxModelLen {
			t.Fatalf("reserved %d, want the context length %d", r, info.MaxModelLen)
		}
	})

	t.Run("cancelling stops the generation in vLLM", func(t *testing.T) {
		l, err := w.AcquireLease(ctx, inference.InferenceRequest{ID: "smoke-4", Model: model, Prompt: "Escribe un cuento",
			PromptTokens: 5, MaxNewTokens: 300, Messages: msg("Escribe un cuento muy largo")})
		if err != nil {
			t.Fatal(err)
		}
		cctx, cancel := context.WithCancel(ctx)
		chunks, wait := inference.StartStream(cctx, l, 0)
		<-chunks
		cancel()
		for range chunks {
		}
		_ = wait()
		// vLLM 0.34 does not count client disconnects in
		// request_success_total{finished_reason="abort"}: watch the running
		// requests instead.
		deadline := time.Now().Add(15 * time.Second)
		for {
			ms := realMetrics(t, url)
			if v, _ := ms.sum(metricRunning...); v == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("vLLM kept generating after the client left")
			}
			time.Sleep(500 * time.Millisecond)
		}
	})

	t.Run("metric names the adapter reads (FI-4)", func(t *testing.T) {
		ms := realMetrics(t, url)
		for _, names := range [][]string{metricCacheConfig, metricKVUsage, metricRunning, metricWaiting,
			metricPrefixQueries, metricPrefixHits, metricSuccesses} {
			if _, ok := ms.first(names...); !ok {
				t.Errorf("none of %v in /metrics", names)
			}
		}
		if _, ok := ms.first(metricProcessStart...); !ok {
			t.Log("process_start_time_seconds absent: restarts are detected by counter resets")
		}
	})
}

func realMetrics(t *testing.T, url string) metricSet {
	t.Helper()
	resp, err := http.Get(url + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return indexMetrics(parseMetrics(resp.Body))
}
