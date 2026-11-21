// Package vllm is the VLLMAdapter (Phase 6a, docs/servicio/13-fase-6a-decisiones.md):
// an inference.Worker backed by a vLLM server reached over HTTP.
//
// The capacity bookkeeping stays in the control plane (internal/ledger):
// vLLM has no reservation API, so AcquireLease never touches the network.
// ExecuteStream sends the request to vLLM's OpenAI-compatible
// /v1/chat/completions and relays its SSE stream as TokenChunks; cancelling
// the context closes the connection, and vLLM aborts the request and frees
// its KV cache. Health reads /health and, in the same check, /metrics: the
// restarts, the real prefix-cache hits and the real KV-cache occupancy.
package vllm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/ledger"
)

// Config describes one vLLM server.
type Config struct {
	// BaseURL is the server root, e.g. http://gpu-1:8000.
	BaseURL string
	// ID defaults to "vllm-" + host:port.
	ID     string
	APIKey string
	// LeaseTTL is how long a lease may wait before ExecuteStream starts.
	LeaseTTL time.Duration
	// TokenBudget overrides the discovered budget; 0 = discover it from
	// vllm:cache_config_info (decision 5).
	TokenBudget int
	// Client defaults to NewHTTPClient().
	Client *http.Client
}

func (c Config) withDefaults() (Config, error) {
	u, err := url.Parse(strings.TrimRight(c.BaseURL, "/"))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return c, fmt.Errorf("vllm: invalid base URL %q", c.BaseURL)
	}
	c.BaseURL = u.String()
	if c.ID == "" {
		c.ID = "vllm-" + u.Host
	}
	if c.Client == nil {
		c.Client = NewHTTPClient()
	}
	return c, nil
}

// NewHTTPClient returns the client the adapter uses: a short connect
// timeout and no overall timeout, because streams last minutes and hangs
// are detected by the scheduler (Phase 4).
func NewHTTPClient() *http.Client {
	return &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   256,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
	}}
}

// Worker is an inference.Worker for one vLLM server.
type Worker struct {
	cfg  Config
	info Info
	led  *ledger.Ledger

	mu     sync.Mutex
	engine *inference.EngineStats
	// Prefix cache counters, in blocks of info.BlockSize tokens.
	cacheQueried, cacheHit uint64
	// Restart detection (decision 7).
	instance        string
	generation      int
	lastSuccesses   float64
	sawSuccesses    bool
	lastProcessTime float64
}

var _ inference.Worker = (*Worker)(nil)

// New returns a worker for a server whose Info was discovered (Discover).
func New(cfg Config, info Info) (*Worker, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	budget := info.TokenBudget
	if cfg.TokenBudget > 0 {
		budget = cfg.TokenBudget
	}
	led, err := ledger.New(ledger.Config{ID: cfg.ID, Models: info.Models, TokenBudget: budget,
		MaxRequestTokens: info.MaxModelLen, LeaseTTL: cfg.LeaseTTL})
	if err != nil {
		return nil, fmt.Errorf("vllm %s: %w", cfg.BaseURL, err)
	}
	return &Worker{cfg: cfg, info: info, led: led}, nil
}

func (w *Worker) ID() string                       { return w.cfg.ID }
func (w *Worker) Models() []string                 { return w.led.Models() }
func (w *Worker) CapacityChanged() <-chan struct{} { return w.led.CapacityChanged() }
func (w *Worker) Info() Info                       { return w.info }

// AcquireLease reserves capacity in the control plane's ledger: no network.
func (w *Worker) AcquireLease(ctx context.Context, req inference.InferenceRequest) (inference.Lease, error) {
	return w.led.Acquire(ctx, req, w.execute)
}

// Snapshot is the ledger plus what the engine last reported.
func (w *Worker) Snapshot() inference.WorkerSnapshot {
	s := w.led.Snapshot()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.engine != nil {
		e := *w.engine
		s.Engine = &e
		// vLLM's prefix cache lives in the whole KV cache.
		s.PrefixCacheBlocks = w.info.NumGPUBlocks
		s.PrefixCacheUsed = int(e.KVCacheUsage * float64(w.info.NumGPUBlocks))
		s.PrefixBlocksQueried, s.PrefixBlocksHit = w.cacheQueried, w.cacheHit
	}
	return s
}

func (w *Worker) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	r, err := http.NewRequestWithContext(ctx, method, w.cfg.BaseURL+path, body)
	if err != nil {
		return nil, err
	}
	if w.cfg.APIKey != "" {
		r.Header.Set("Authorization", "Bearer "+w.cfg.APIKey)
	}
	return r, nil
}

// requestBody builds the body for vLLM: the client's forwarded parameters,
// then the fields the platform controls (decision 1), which win.
func requestBody(req inference.InferenceRequest) ([]byte, error) {
	body := make(map[string]any, len(req.Params)+6)
	for k, v := range req.Params {
		body[k] = v
	}
	msgs := make([]map[string]string, len(req.Messages))
	for i, m := range req.Messages {
		msgs[i] = map[string]string{"role": m.Role, "content": m.Content}
		if m.Name != "" {
			msgs[i]["name"] = m.Name
		}
	}
	if len(msgs) == 0 && req.Prompt != "" { // a request built without messages
		msgs = []map[string]string{{"role": "user", "content": req.Prompt}}
	}
	body["model"] = req.Model
	body["messages"] = msgs
	body["max_tokens"] = req.MaxNewTokens
	body["stream"] = true
	body["stream_options"] = map[string]bool{"include_usage": true} // FI-8
	return json.Marshal(body)
}

// execute is the ledger.Executor: one streaming request to vLLM.
func (w *Worker) execute(ctx context.Context, req inference.InferenceRequest, out chan<- inference.TokenChunk) error {
	payload, err := requestBody(req)
	if err != nil {
		return fmt.Errorf("%w: %v", inference.ErrInvalidRequest, err)
	}
	httpReq, err := w.newRequest(ctx, http.MethodPost, "/v1/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return transportError(w.ID(), err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	if req.ID != "" {
		httpReq.Header.Set("X-Request-Id", req.ID)
	}
	resp, err := w.cfg.Client.Do(httpReq)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return transportError(w.ID(), err)
	}
	defer resp.Body.Close() // closing early is what makes vLLM abort
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		return statusError(w.ID(), resp.StatusCode, body)
	}
	return w.relay(ctx, resp.Body, out)
}

// streamEvent is the part of a vLLM chat.completion.chunk the adapter uses.
type streamEvent struct {
	Object  string `json:"object"`
	Message string `json:"message"`
	Error   *struct {
		Message string `json:"message"`
	} `json:"error"`
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

// relay reads vLLM's SSE stream and sends one TokenChunk per non-empty
// content delta, then a last chunk (empty text) with the finish reason and
// the engine's usage, as OpenAI-compatible servers do. A chunk is sent as
// soon as it arrives: holding one back to attach the finish reason would
// delay every token, the first one included.
func (w *Worker) relay(ctx context.Context, body io.Reader, out chan<- inference.TokenChunk) error {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	var (
		index  int
		finish = inference.FinishNone
		usage  *inference.Usage
		done   bool
	)
	send := func(c inference.TokenChunk) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- c:
			return nil
		}
	}
	for !done && sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data:") {
			continue // blank separators, ": keep-alive" comments, other SSE fields
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			done = true
			break
		}
		var ev streamEvent
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			return fmt.Errorf("%s: %w: unreadable stream event: %v", w.ID(), inference.ErrWorkerFailure, err)
		}
		if ev.Error != nil || ev.Object == "error" {
			msg := ev.Message
			if ev.Error != nil {
				msg = ev.Error.Message
			}
			return fmt.Errorf("%s: %w: engine error in stream: %s", w.ID(), inference.ErrWorkerFailure, msg)
		}
		if ev.Usage != nil {
			usage = &inference.Usage{PromptTokens: ev.Usage.PromptTokens, CompletionTokens: ev.Usage.CompletionTokens}
		}
		for _, c := range ev.Choices {
			if c.Delta.Content != "" {
				if err := send(inference.TokenChunk{Index: index, Text: c.Delta.Content}); err != nil {
					return err
				}
				index++
			}
			if c.FinishReason != nil && *c.FinishReason != "" {
				finish = inference.FinishReason(*c.FinishReason)
			}
		}
	}
	if !done {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("%s: %w: stream read: %v", w.ID(), inference.ErrWorkerFailure, err)
		}
		return fmt.Errorf("%s: %w: stream ended without [DONE]", w.ID(), inference.ErrWorkerFailure)
	}
	if finish == inference.FinishNone {
		finish = inference.FinishStop
	}
	return send(inference.TokenChunk{Index: index, FinishReason: finish, Usage: usage})
}
