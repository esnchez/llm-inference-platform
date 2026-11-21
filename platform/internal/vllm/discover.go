package vllm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"llm-inference-platform/internal/inference"
)

// vLLM metric names the adapter reads, in one place (FI-4): each list goes
// from the current name to older ones. To be checked against the vLLM
// version fixed in Phase 6b.
var (
	metricCacheConfig   = []string{"vllm:cache_config_info"}
	metricKVUsage       = []string{"vllm:kv_cache_usage_perc", "vllm:gpu_cache_usage_perc"}
	metricRunning       = []string{"vllm:num_requests_running"}
	metricWaiting       = []string{"vllm:num_requests_waiting"}
	metricPrefixQueries = []string{"vllm:prefix_cache_queries_total", "vllm:prefix_cache_queries"}
	metricPrefixHits    = []string{"vllm:prefix_cache_hits_total", "vllm:prefix_cache_hits"}
	metricSuccesses     = []string{"vllm:request_success_total", "vllm:request_success"}
	metricProcessStart  = []string{"process_start_time_seconds"}
)

// Info is what Discover learns about a server.
type Info struct {
	Models       []string
	MaxModelLen  int // largest prompt + max_tokens per request; 0 if unknown
	BlockSize    int // KV-cache block size, tokens
	NumGPUBlocks int
	// TokenBudget = NumGPUBlocks × BlockSize: the whole KV cache, in tokens.
	TokenBudget int
}

func (i Info) equal(o Info) bool {
	if len(i.Models) != len(o.Models) || i.MaxModelLen != o.MaxModelLen || i.TokenBudget != o.TokenBudget {
		return false
	}
	for k := range i.Models {
		if i.Models[k] != o.Models[k] {
			return false
		}
	}
	return true
}

// Discover asks a server what it serves (/v1/models) and how big its KV
// cache is (/metrics, vllm:cache_config_info). It fails while the server is
// down or still loading the model; the connector retries.
func Discover(ctx context.Context, cfg Config) (Info, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return Info{}, err
	}
	w := &Worker{cfg: cfg}
	var info Info

	body, err := w.get(ctx, "/v1/models")
	if err != nil {
		return Info{}, err
	}
	var models struct {
		Data []struct {
			ID          string `json:"id"`
			MaxModelLen int    `json:"max_model_len"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &models); err != nil || len(models.Data) == 0 {
		return Info{}, fmt.Errorf("vllm %s: /v1/models lists no model (%v)", cfg.BaseURL, err)
	}
	for _, m := range models.Data {
		info.Models = append(info.Models, m.ID)
		if m.MaxModelLen > 0 && (info.MaxModelLen == 0 || m.MaxModelLen < info.MaxModelLen) {
			info.MaxModelLen = m.MaxModelLen
		}
	}

	if cfg.TokenBudget > 0 {
		info.TokenBudget = cfg.TokenBudget
	}
	body, err = w.get(ctx, "/metrics")
	if err != nil && info.TokenBudget == 0 {
		return Info{}, err
	}
	if err == nil {
		ms := indexMetrics(parseMetrics(bytes.NewReader(body)))
		if s, ok := ms.first(metricCacheConfig...); ok {
			info.BlockSize, _ = strconv.Atoi(s.labels["block_size"])
			info.NumGPUBlocks, _ = strconv.Atoi(s.labels["num_gpu_blocks"])
		}
	}
	if info.TokenBudget == 0 {
		if info.BlockSize <= 0 || info.NumGPUBlocks <= 0 {
			return Info{}, fmt.Errorf("vllm %s: no usable vllm:cache_config_info (block_size, num_gpu_blocks) in /metrics; set -vllm-token-budget", cfg.BaseURL)
		}
		info.TokenBudget = info.BlockSize * info.NumGPUBlocks
	}
	if info.BlockSize <= 0 {
		info.BlockSize = 16 // vLLM's default, for prefix-cache counters in blocks
	}
	return info, nil
}

// get fetches a small endpoint; any non-200 is a worker failure.
func (w *Worker) get(ctx context.Context, path string) ([]byte, error) {
	req, err := w.newRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, transportError(w.cfg.ID, err)
	}
	resp, err := w.cfg.Client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, transportError(w.cfg.ID, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		return nil, transportError(w.cfg.ID, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %w: %s: HTTP %d", w.cfg.ID, inference.ErrWorkerFailure, path, resp.StatusCode)
	}
	return body, nil
}

// Health implements inference.Worker: /health decides; /metrics, read in
// the same check, refreshes the engine's view (decision 7). A failing
// /metrics does not make the worker unhealthy.
func (w *Worker) Health(ctx context.Context) (inference.HealthStatus, error) {
	if _, err := w.get(ctx, "/health"); err != nil {
		return inference.HealthStatus{}, err
	}
	if body, err := w.get(ctx, "/metrics"); err == nil {
		w.updateEngine(indexMetrics(parseMetrics(bytes.NewReader(body))))
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.instance == "" {
		w.instance = fmt.Sprintf("%s#0", w.ID())
	}
	return inference.HealthStatus{InstanceID: w.instance}, nil
}

// updateEngine stores the engine's stats and detects restarts: a new
// process_start_time_seconds or, when vLLM does not export it (multiprocess
// mode), a counter that went down.
func (w *Worker) updateEngine(ms metricSet) {
	w.mu.Lock()
	defer w.mu.Unlock()
	e := &inference.EngineStats{ScrapedAtUnixSec: time.Now().Unix()}
	if s, ok := ms.first(metricKVUsage...); ok {
		e.KVCacheUsage = s.value
	}
	if v, ok := ms.sum(metricRunning...); ok {
		e.RequestsRunning = int(v)
	}
	if v, ok := ms.sum(metricWaiting...); ok {
		e.RequestsWaiting = int(v)
	}
	w.engine = e

	block := float64(max(w.info.BlockSize, 1))
	if v, ok := ms.sum(metricPrefixQueries...); ok {
		w.cacheQueried = uint64(v / block)
	}
	if v, ok := ms.sum(metricPrefixHits...); ok {
		w.cacheHit = uint64(v / block)
	}

	restarted := false
	if s, ok := ms.first(metricProcessStart...); ok {
		if w.lastProcessTime != 0 && s.value != w.lastProcessTime {
			restarted = true
		}
		w.lastProcessTime = s.value
	} else if v, ok := ms.sum(metricSuccesses...); ok {
		if w.sawSuccesses && v < w.lastSuccesses {
			restarted = true
		}
		w.lastSuccesses, w.sawSuccesses = v, true
	}
	if restarted || w.instance == "" {
		if restarted {
			w.generation++
		}
		w.instance = fmt.Sprintf("%s#%d", w.ID(), w.generation)
	}
}
