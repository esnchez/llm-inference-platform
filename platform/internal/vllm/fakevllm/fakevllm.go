// Package fakevllm is an HTTP server that imitates the parts of vLLM's
// OpenAI-compatible server the platform uses (Phase 6a,
// docs/servicio/13-fase-6a-decisiones.md, decision 9): streaming chat
// completions with usage, /v1/models with max_model_len, /health and the
// Prometheus /metrics the adapter reads. It can inject failures.
//
// The formats are written from vLLM's documented behaviour and must be
// checked against the real server in Phase 6b (FI-4): the chunk with the
// assistant role first, content chunks, a last choice chunk with
// finish_reason and empty content, a usage chunk with no choices, then
// [DONE]; errors as {"error": {...}} (newer vLLM) or {"object": "error", ...}
// (older vLLM, selectable with LegacyErrors).
package fakevllm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// Config describes the fake engine.
type Config struct {
	Model        string
	MaxModelLen  int           // default 4096
	NumGPUBlocks int           // default 1024
	BlockSize    int           // default 16
	TTFT         time.Duration // before the first token
	TokenDelay   time.Duration // between tokens
	APIKey       string        // required as a Bearer token if set
	// OutputTokens caps the generated tokens; 0 = max_tokens.
	OutputTokens int
	// LegacyErrors uses vLLM's older error body {"object":"error",...}.
	LegacyErrors bool
	// NoProcessMetrics omits process_start_time_seconds (vLLM in
	// multiprocess mode), to exercise the adapter's fallback.
	NoProcessMetrics bool
}

// Faults are injected failures; the zero value injects nothing.
type Faults struct {
	Loading      bool `json:"loading,omitempty"`        // model still loading: 503 everywhere but /metrics
	Unhealthy    bool `json:"unhealthy,omitempty"`      // /health answers 500
	Status       int  `json:"status,omitempty"`         // completions answer this HTTP status (e.g. 500)
	Hang         bool `json:"hang,omitempty"`           // completions send nothing until the client leaves
	CutAfter     int  `json:"cut_after,omitempty"`      // end the stream without [DONE] after N tokens
	ErrorEventAt int  `json:"error_event_at,omitempty"` // send an in-band error event after N tokens
	SlowFactor   int  `json:"slow_factor,omitempty"`    // multiply TTFT and token delay
}

// Server is the fake engine. Use Handler with httptest or net/http.
type Server struct {
	cfg Config

	mu        sync.Mutex
	faults    Faults
	started   time.Time
	successes int64
	queries   int64 // prefix cache, in tokens
	hits      int64
	seen      map[string]bool
	lastBody  map[string]json.RawMessage

	running  atomic.Int64
	aborted  atomic.Int64
	requests atomic.Int64
}

// New returns a fake engine.
func New(cfg Config) *Server {
	if cfg.Model == "" {
		cfg.Model = "fake-model"
	}
	if cfg.MaxModelLen == 0 {
		cfg.MaxModelLen = 4096
	}
	if cfg.NumGPUBlocks == 0 {
		cfg.NumGPUBlocks = 1024
	}
	if cfg.BlockSize == 0 {
		cfg.BlockSize = 16
	}
	return &Server{cfg: cfg, started: time.Now(), seen: map[string]bool{}}
}

// SetFaults replaces the injected faults.
func (s *Server) SetFaults(f Faults) {
	s.mu.Lock()
	s.faults = f
	s.mu.Unlock()
}

// Restart simulates a restart: new process start time, counters and prefix
// cache reset. Streams in flight are not cut (use CutAfter for that).
func (s *Server) Restart() {
	s.mu.Lock()
	s.started = time.Now()
	s.successes, s.queries, s.hits = 0, 0, 0
	s.seen = map[string]bool{}
	s.mu.Unlock()
}

// Aborted counts streams whose client went away before the end (vLLM
// aborts those requests and frees their KV cache).
func (s *Server) Aborted() int64 { return s.aborted.Load() }

// Requests counts chat completion requests received.
func (s *Server) Requests() int64 { return s.requests.Load() }

// LastRequest returns the body of the last chat completion request.
func (s *Server) LastRequest() map[string]json.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastBody
}

func (s *Server) currentFaults() Faults {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.faults
}

// Handler serves the fake engine, plus /fake/faults and /fake/restart to
// break it from outside (cmd/fakevllm, the stack's chaos).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /v1/models", s.handleModels)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("POST /v1/chat/completions", s.handleChat)
	mux.HandleFunc("POST /fake/faults", func(w http.ResponseWriter, r *http.Request) {
		var f Faults
		if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.SetFaults(f)
		_ = json.NewEncoder(w).Encode(f)
	})
	mux.HandleFunc("POST /fake/restart", func(w http.ResponseWriter, _ *http.Request) {
		s.Restart()
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	f := s.currentFaults()
	if f.Loading {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if f.Unhealthy {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleModels(w http.ResponseWriter, _ *http.Request) {
	if s.currentFaults().Loading {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data": []map[string]any{{
			"id": s.cfg.Model, "object": "model", "created": time.Now().Unix(), "owned_by": "vllm",
			"root": s.cfg.Model, "parent": nil, "max_model_len": s.cfg.MaxModelLen,
		}},
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	started, successes, queries, hits := s.started, s.successes, s.queries, s.hits
	s.mu.Unlock()
	running := s.running.Load()
	usage := min(1, float64(running*256)/float64(s.cfg.NumGPUBlocks*s.cfg.BlockSize))
	var b strings.Builder
	m := fmt.Sprintf("model_name=%q", s.cfg.Model)
	fmt.Fprintf(&b, "# HELP vllm:cache_config_info Information of the LLMEngine CacheConfig\n# TYPE vllm:cache_config_info gauge\n")
	fmt.Fprintf(&b, "vllm:cache_config_info{block_size=%q,cache_dtype=\"auto\",enable_prefix_caching=\"True\",gpu_memory_utilization=\"0.9\",num_cpu_blocks=\"None\",num_gpu_blocks=%q} 1.0\n",
		fmt.Sprint(s.cfg.BlockSize), fmt.Sprint(s.cfg.NumGPUBlocks))
	fmt.Fprintf(&b, "# TYPE vllm:num_requests_running gauge\nvllm:num_requests_running{%s} %d.0\n", m, running)
	fmt.Fprintf(&b, "# TYPE vllm:num_requests_waiting gauge\nvllm:num_requests_waiting{%s} 0.0\n", m)
	fmt.Fprintf(&b, "# TYPE vllm:kv_cache_usage_perc gauge\nvllm:kv_cache_usage_perc{%s} %g\n", m, usage)
	fmt.Fprintf(&b, "# TYPE vllm:prefix_cache_queries counter\nvllm:prefix_cache_queries_total{%s} %d.0\n", m, queries)
	fmt.Fprintf(&b, "# TYPE vllm:prefix_cache_hits counter\nvllm:prefix_cache_hits_total{%s} %d.0\n", m, hits)
	fmt.Fprintf(&b, "# TYPE vllm:request_success counter\nvllm:request_success_total{finished_reason=\"stop\",%s} %d.0\n", m, successes)
	if !s.cfg.NoProcessMetrics {
		fmt.Fprintf(&b, "# TYPE process_start_time_seconds gauge\nprocess_start_time_seconds %.2f\n", float64(started.UnixNano())/1e9)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(b.String()))
}

type chatRequest struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
	MaxTokens     int  `json:"max_tokens"`
	Stream        bool `json:"stream"`
	StreamOptions struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
}

func (s *Server) writeError(w http.ResponseWriter, status int, typ, msg string) {
	if s.cfg.LegacyErrors {
		writeJSON(w, status, map[string]any{"object": "error", "message": msg, "type": typ, "param": nil, "code": status})
		return
	}
	writeJSON(w, status, map[string]any{"error": map[string]any{"message": msg, "type": typ, "param": nil, "code": status}})
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	s.requests.Add(1)
	if s.cfg.APIKey != "" && r.Header.Get("Authorization") != "Bearer "+s.cfg.APIKey {
		s.writeError(w, http.StatusUnauthorized, "Unauthorized", "invalid API key")
		return
	}
	f := s.currentFaults()
	if f.Loading {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	var raw map[string]json.RawMessage
	var req chatRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&raw); err != nil {
		s.writeError(w, http.StatusBadRequest, "BadRequestError", err.Error())
		return
	}
	s.mu.Lock()
	s.lastBody = raw
	s.mu.Unlock()
	b, _ := json.Marshal(raw)
	if err := json.Unmarshal(b, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "BadRequestError", err.Error())
		return
	}
	if f.Status != 0 {
		s.writeError(w, f.Status, "InternalServerError", "injected failure")
		return
	}
	if req.Model != s.cfg.Model {
		s.writeError(w, http.StatusNotFound, "NotFoundError", fmt.Sprintf("The model `%s` does not exist.", req.Model))
		return
	}
	if !req.Stream {
		s.writeError(w, http.StatusBadRequest, "BadRequestError", "fakevllm only implements stream=true")
		return
	}
	if len(req.Messages) == 0 {
		s.writeError(w, http.StatusBadRequest, "BadRequestError", "messages must not be empty")
		return
	}
	promptTokens := 0
	prompt := ""
	for _, m := range req.Messages {
		promptTokens += 3 + utf8.RuneCountInString(m.Content)/4 // a tokenizer that is not ours
		prompt += m.Role + ":" + m.Content + "\n"
	}
	maxTokens := req.MaxTokens
	if maxTokens == 0 {
		maxTokens = s.cfg.MaxModelLen - promptTokens
	}
	if promptTokens+maxTokens > s.cfg.MaxModelLen {
		s.writeError(w, http.StatusBadRequest, "BadRequestError", fmt.Sprintf(
			"This model's maximum context length is %d tokens. However, you requested %d tokens (%d in the messages, %d in the completion). Please reduce the length of the messages or completion.",
			s.cfg.MaxModelLen, promptTokens+maxTokens, promptTokens, maxTokens))
		return
	}
	s.prefixCache(req.Messages[0].Content, promptTokens)
	s.stream(w, r, req, promptTokens, maxTokens, f)
}

// prefixCache counts the first message as cacheable, like a system prompt.
func (s *Server) prefixCache(first string, promptTokens int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries += int64(promptTokens)
	if s.seen[first] {
		s.hits += int64(3 + utf8.RuneCountInString(first)/4)
	}
	s.seen[first] = true
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request, req chatRequest, promptTokens, maxTokens int, f Faults) {
	s.running.Add(1)
	defer s.running.Add(-1)
	ctx := r.Context()
	flusher, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)

	id := "chatcmpl-fake" + fmt.Sprint(time.Now().UnixNano())
	created := time.Now().Unix()
	send := func(v any) bool {
		b, _ := json.Marshal(v)
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}
	chunk := func(delta map[string]any, finish any) map[string]any {
		return map[string]any{"id": id, "object": "chat.completion.chunk", "created": created, "model": req.Model,
			"choices": []map[string]any{{"index": 0, "delta": delta, "logprobs": nil, "finish_reason": finish}}}
	}
	wait := func(d time.Duration) bool {
		if f.SlowFactor > 1 {
			d *= time.Duration(f.SlowFactor)
		}
		if d <= 0 {
			return ctx.Err() == nil
		}
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			return true
		}
	}
	aborted := func() { s.aborted.Add(1) }

	if f.Hang {
		<-ctx.Done()
		aborted()
		return
	}
	if !send(chunk(map[string]any{"role": "assistant", "content": ""}, nil)) {
		aborted()
		return
	}
	n := maxTokens
	if s.cfg.OutputTokens > 0 {
		n = min(n, s.cfg.OutputTokens)
	}
	words := []string{"Hola", " desde", " un", " vLLM", " falso", "."}
	for i := range n {
		d := s.cfg.TokenDelay
		if i == 0 {
			d = s.cfg.TTFT
		}
		if !wait(d) {
			aborted()
			return
		}
		if f.CutAfter > 0 && i == f.CutAfter {
			return // the response ends without [DONE]
		}
		if f.ErrorEventAt > 0 && i == f.ErrorEventAt {
			send(map[string]any{"error": map[string]any{"message": "injected engine error", "type": "InternalServerError", "code": 500}})
			fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		if !send(chunk(map[string]any{"content": words[i%len(words)]}, nil)) {
			aborted()
			return
		}
	}
	finish := "length"
	if n < maxTokens {
		finish = "stop"
	}
	send(chunk(map[string]any{"content": ""}, finish))
	if req.StreamOptions.IncludeUsage {
		send(map[string]any{"id": id, "object": "chat.completion.chunk", "created": created, "model": req.Model,
			"choices": []any{}, "usage": map[string]int{"prompt_tokens": promptTokens, "completion_tokens": n, "total_tokens": promptTokens + n}})
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
	if flusher != nil {
		flusher.Flush()
	}
	s.mu.Lock()
	s.successes++
	s.mu.Unlock()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
