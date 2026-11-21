// Package httpapi exposes the platform over an OpenAI-compatible HTTP API
// (POST /v1/chat/completions), streaming with Server-Sent Events.
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/prefix"
	"llm-inference-platform/internal/tokens"
)

// Scheduler is what the API needs from the control plane. Acquire may wait
// in a queue; it must return before the response is started, so that a
// rejection can still use a proper HTTP status.
type Scheduler interface {
	Acquire(ctx context.Context, req inference.InferenceRequest) (inference.Lease, error)
	Models() []string
	Snapshots() []inference.WorkerSnapshot
}

// debugStatser is implemented by schedulers that expose their queue state on
// /debug/scheduler.
type debugStatser interface {
	DebugStats() any
}

// Config tunes the API.
type Config struct {
	// DefaultMaxTokens applies when the client sends no max_tokens.
	DefaultMaxTokens int
	// MaxTokensLimit is the largest max_tokens a client may ask for.
	MaxTokensLimit int
	// RetryAfter is advertised on 503 responses when the scheduler does not
	// provide its own value (see inference.RetryAfterError).
	RetryAfter time.Duration
	// StreamBuffer is the channel buffer between a worker and the client.
	StreamBuffer int
	// MaxBodyBytes caps the request body size.
	MaxBodyBytes int64
	// Observer receives one record per request (Phase 5). Default: none.
	Observer Observer
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		DefaultMaxTokens: 256,
		MaxTokensLimit:   4096,
		RetryAfter:       time.Second,
		StreamBuffer:     16,
		MaxBodyBytes:     4 << 20,
	}
}

// Server serves the HTTP API.
type Server struct {
	sched Scheduler
	cfg   Config
	log   *slog.Logger
	now   func() time.Time
}

// New returns a Server.
func New(sched Scheduler, cfg Config, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Observer == nil {
		cfg.Observer = nopObserver{}
	}
	return &Server{sched: sched, cfg: cfg, log: log, now: time.Now}
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.handleChatCompletions)
	mux.HandleFunc("GET /v1/models", s.handleModels)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /debug/workers", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, s.sched.Snapshots())
	})
	if ds, ok := s.sched.(debugStatser); ok {
		mux.HandleFunc("GET /debug/scheduler", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, ds.DebugStats())
		})
	}
	return mux
}

// ---- OpenAI-compatible wire types ----

// chatMessage is a request message; its UnmarshalJSON (params.go) only
// accepts string content.
type chatMessage struct {
	Role    string
	Content string
	Name    string
}

// responseMessage is the assistant message of a non-streaming response.
type responseMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type chatRequest struct {
	Model               string         `json:"model"`
	Messages            []chatMessage  `json:"messages"`
	MaxTokens           *int           `json:"max_tokens"`
	MaxCompletionTokens *int           `json:"max_completion_tokens"`
	Stream              bool           `json:"stream"`
	StreamOptions       *streamOptions `json:"stream_options"`

	// raw is the whole request body, field by field: the parameters that
	// are not controlled here are forwarded to the engine (params.go).
	raw map[string]json.RawMessage
}

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type delta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

type chunkChoice struct {
	Index        int     `json:"index"`
	Delta        delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

type completionChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []chunkChoice `json:"choices"`
	Usage   *usage        `json:"usage,omitempty"`
}

type messageChoice struct {
	Index        int             `json:"index"`
	Message      responseMessage `json:"message"`
	FinishReason *string         `json:"finish_reason"`
}

type completion struct {
	ID      string          `json:"id"`
	Object  string          `json:"object"`
	Created int64           `json:"created"`
	Model   string          `json:"model"`
	Choices []messageChoice `json:"choices"`
	Usage   usage           `json:"usage"`
}

type apiError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code,omitempty"`
}

// ---- handlers ----

func (s *Server) handleModels(w http.ResponseWriter, _ *http.Request) {
	type model struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		OwnedBy string `json:"owned_by"`
	}
	data := []model{}
	for _, m := range s.sched.Models() {
		data = append(data, model{ID: m, Object: "model", OwnedBy: "llm-inference-platform"})
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

func (s *Server) handleChatCompletions(rw http.ResponseWriter, r *http.Request) {
	start := s.now()
	w := &statusWriter{ResponseWriter: rw}
	requestID := clientRequestID(r.Header.Get("X-Request-Id"))
	if requestID == "" {
		requestID = newRequestID()
	}
	w.Header().Set("X-Request-Id", requestID)
	rec := RequestRecord{Model: UnknownModel}
	log := s.log.With("request_id", requestID)

	var body chatRequest
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	if err == nil {
		err = json.Unmarshal(raw, &body)
	}
	if err == nil {
		err = json.Unmarshal(raw, &body.raw)
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "", "invalid JSON body: "+err.Error())
		s.finish(log, w, start, rec, OutcomeRejectedInvalid, nil, streamResult{err: err})
		return
	}
	rec.Model = s.modelLabel(body.Model)
	req, err := s.toInferenceRequest(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "", err.Error())
		s.finish(log, w, start, rec, OutcomeRejectedInvalid, nil, streamResult{err: err})
		return
	}
	req.ID = requestID
	log = log.With("prompt_tokens", req.PromptTokens, "max_new_tokens", req.MaxNewTokens, "stream", body.Stream)

	// Admission (including any wait in the scheduler's queue) happens before
	// any byte of the response is written, so that a rejection can still use
	// a proper HTTP status code.
	lease, err := s.sched.Acquire(r.Context(), req)
	if err != nil {
		s.writeAdmissionError(w, r.Context(), err)
		s.finish(log, w, start, rec, failureOutcome(r.Context(), err), nil, streamResult{err: err})
		return
	}
	defer lease.Release()
	rec.Admitted = true
	rec.PromptTokens, rec.ReservedTokens = req.PromptTokens, req.TokenCost()
	s.cfg.Observer.RequestStarted(rec.Model)

	// ctx is cancelled when the client disconnects or when we fail to write
	// to it; the worker sees it and frees its capacity.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	var res streamResult
	if body.Stream {
		res = s.stream(ctx, cancel, w, req, lease, body.StreamOptions != nil && body.StreamOptions.IncludeUsage)
	} else {
		res = s.collect(ctx, w, req, lease)
	}

	outcome := OutcomeCompleted
	switch {
	case res.err == nil:
	case errors.Is(res.err, context.Canceled) || ctx.Err() != nil:
		outcome = OutcomeCancelled
	case body.Stream && res.completionTokens > 0:
		outcome = OutcomeFailedMidstream // the 200 was sent: reported in-band
	default:
		outcome = failureOutcome(ctx, res.err)
	}
	s.finish(log, w, start, rec, outcome, lease, res)
}

// finish logs the request and reports it to the observer: one log line and
// one record per request, whatever happened.
func (s *Server) finish(log *slog.Logger, w *statusWriter, start time.Time, rec RequestRecord,
	outcome string, lease inference.Lease, res streamResult) {
	rec.Outcome = outcome
	rec.Duration = s.now().Sub(start)
	rec.CompletionTokens = res.completionTokens
	if res.usage != nil {
		rec.UsageReported = true
		rec.EstimatedPromptTokens = rec.PromptTokens
		rec.PromptTokens = res.usage.PromptTokens
		rec.CompletionTokens = res.usage.CompletionTokens
	}
	if !res.firstToken.IsZero() {
		rec.FirstToken = true
		rec.TTFT = res.firstToken.Sub(start)
	}
	s.cfg.Observer.RequestFinished(rec)

	attrs := []any{"model", rec.Model, "outcome", outcome, "http_status", w.status,
		"completion_tokens", res.completionTokens, "duration_ms", rec.Duration.Milliseconds()}
	if rec.FirstToken {
		attrs = append(attrs, "ttft_ms", rec.TTFT.Milliseconds())
	}
	if lease != nil {
		// After the stream: a retry may have moved the request to another
		// worker.
		attrs = append(attrs, "worker_id", lease.WorkerID())
		if d, ok := lease.(leaseDetails); ok {
			attrs = append(attrs, "queue_wait_ms", d.QueueWait().Milliseconds(), "retries", d.Retries())
			if rec.FirstToken {
				attrs = append(attrs, "worker_ttft_ms", d.WorkerTTFT().Milliseconds())
			}
		}
	}
	if res.err != nil && outcome != OutcomeCancelled {
		attrs = append(attrs, "error", res.err.Error())
	}
	switch outcome {
	case OutcomeCompleted:
		log.Info("request completed", attrs...)
	case OutcomeCancelled:
		log.Info("request cancelled by client", attrs...)
	case OutcomeRejectedInvalid:
		log.Info("request rejected", attrs...)
	case OutcomeQueueFull, OutcomeQueueTimeout:
		log.Warn("request shed", attrs...)
	default:
		log.Error("request failed", attrs...)
	}
}

// modelLabel returns model if a worker serves it, else UnknownModel.
func (s *Server) modelLabel(model string) string {
	for _, m := range s.sched.Models() {
		if m == model {
			return model
		}
	}
	return UnknownModel
}

type streamResult struct {
	completionTokens int // chunks received (tokens when the worker gives no usage)
	usage            *inference.Usage
	firstToken       time.Time
	err              error
}

// tokenCounts returns the prompt and completion tokens of a finished
// stream: the engine's own count when it sent one (FI-8), else the estimate
// and the number of chunks.
func (r streamResult) tokenCounts(req inference.InferenceRequest) (prompt, completion int) {
	if r.usage != nil {
		return r.usage.PromptTokens, r.usage.CompletionTokens
	}
	return req.PromptTokens, r.completionTokens
}

func (s *Server) stream(ctx context.Context, cancel context.CancelFunc, w http.ResponseWriter,
	req inference.InferenceRequest, lease inference.Lease, includeUsage bool) streamResult {
	rc := http.NewResponseController(w)
	// The 200 and the SSE headers wait for the first chunk (Phase 4,
	// decision 4): until then the scheduler may still retry the request on
	// another worker, and a failure can still get a proper status code
	// (README §9, FI-9).
	started := false
	startResponse := func() {
		if started {
			return
		}
		started = true
		h := w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("Connection", "keep-alive")
		h.Set("X-Accel-Buffering", "no")
		h.Set("X-Request-Id", req.ID)
		h.Set("X-Worker-Id", lease.WorkerID())
		w.WriteHeader(http.StatusOK)
	}

	var res streamResult
	writeFailed := false
	send := func(v any) {
		if writeFailed {
			return
		}
		startResponse()
		if err := writeSSE(w, rc, v); err != nil {
			// The client is gone: stop the worker and keep draining.
			writeFailed = true
			cancel()
		}
	}

	created := s.now().Unix()
	chunks, wait := inference.StartStream(ctx, lease, s.cfg.StreamBuffer)
	for c := range chunks {
		if res.completionTokens == 0 {
			res.firstToken = s.now()
		}
		res.completionTokens++
		if c.Usage != nil {
			res.usage = c.Usage
		}
		d := delta{Content: c.Text}
		if c.Index == 0 {
			d.Role = "assistant"
		}
		send(completionChunk{
			ID: req.ID, Object: "chat.completion.chunk", Created: created, Model: req.Model,
			Choices: []chunkChoice{{Index: 0, Delta: d, FinishReason: finishReason(c.FinishReason)}},
		})
	}
	res.err = wait()
	if res.err == nil && writeFailed {
		res.err = context.Canceled
	}
	if res.err != nil && !started {
		// Failed before the first token, retries included: nothing was
		// sent, so answer with a real status code.
		s.writeAdmissionError(w, ctx, res.err)
		return res
	}

	switch {
	case res.err != nil && ctx.Err() == nil:
		// Mid-stream failure: the 200 status is already sent, so report it
		// in-band, as OpenAI-compatible servers do.
		send(map[string]apiError{"error": {Message: res.err.Error(), Type: "server_error"}})
	case res.err == nil && includeUsage:
		u := usageOf(res.tokenCounts(req))
		send(completionChunk{
			ID: req.ID, Object: "chat.completion.chunk", Created: created, Model: req.Model,
			Choices: []chunkChoice{}, Usage: &u,
		})
	}
	if ctx.Err() == nil {
		send(rawSSE("[DONE]"))
	}
	return res
}

func (s *Server) collect(ctx context.Context, w http.ResponseWriter,
	req inference.InferenceRequest, lease inference.Lease) streamResult {
	var (
		res    streamResult
		text   strings.Builder
		finish inference.FinishReason
	)
	chunks, wait := inference.StartStream(ctx, lease, s.cfg.StreamBuffer)
	for c := range chunks {
		if res.completionTokens == 0 {
			res.firstToken = s.now()
		}
		res.completionTokens++
		if c.Usage != nil {
			res.usage = c.Usage
		}
		text.WriteString(c.Text)
		if c.FinishReason != inference.FinishNone {
			finish = c.FinishReason
		}
	}
	res.err = wait()
	if res.err != nil {
		s.writeAdmissionError(w, ctx, res.err)
		return res
	}
	w.Header().Set("X-Worker-Id", lease.WorkerID())
	writeJSON(w, http.StatusOK, completion{
		ID: req.ID, Object: "chat.completion", Created: s.now().Unix(), Model: req.Model,
		Choices: []messageChoice{{
			Index:        0,
			Message:      responseMessage{Role: "assistant", Content: text.String()},
			FinishReason: finishReason(finish),
		}},
		Usage: usageOf(res.tokenCounts(req)),
	})
	return res
}

func (s *Server) toInferenceRequest(body chatRequest) (inference.InferenceRequest, error) {
	if body.Model == "" {
		return inference.InferenceRequest{}, errors.New("model is required")
	}
	if len(body.Messages) == 0 {
		return inference.InferenceRequest{}, errors.New("messages must not be empty")
	}
	maxTokens := s.cfg.DefaultMaxTokens
	switch {
	case body.MaxCompletionTokens != nil:
		maxTokens = *body.MaxCompletionTokens
	case body.MaxTokens != nil:
		maxTokens = *body.MaxTokens
	}
	if maxTokens < 1 || maxTokens > s.cfg.MaxTokensLimit {
		return inference.InferenceRequest{}, fmt.Errorf("max_tokens must be between 1 and %d", s.cfg.MaxTokensLimit)
	}
	params, err := forwardedParams(body.raw)
	if err != nil {
		return inference.InferenceRequest{}, err
	}
	messages := make([]inference.Message, len(body.Messages))
	for i, m := range body.Messages {
		if m.Role == "" {
			return inference.InferenceRequest{}, fmt.Errorf("messages[%d]: role is required", i)
		}
		messages[i] = inference.Message{Role: m.Role, Content: m.Content, Name: m.Name}
	}
	prompt := renderPrompt(body.Messages)
	return inference.InferenceRequest{
		ID:           newRequestID(),
		Model:        body.Model,
		Prompt:       prompt,
		PromptTokens: tokens.Estimate(prompt),
		MaxNewTokens: maxTokens,
		// Hashed once here; the scheduler may try to place the request
		// many times while it waits in the queue.
		PrefixHashes: prefix.Hashes(body.Model, prompt),
		Messages:     messages,
		Params:       params,
	}, nil
}

// renderPrompt flattens chat messages. Real chat templates are model specific
// and are applied by the inference engine (vLLM) in later phases.
func renderPrompt(msgs []chatMessage) string {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(m.Role)
		b.WriteString(": ")
		b.WriteString(m.Content)
		b.WriteString("\n")
	}
	return b.String()
}

// writeAdmissionError answers a request that failed before any byte of
// the response was written: at admission, or in the stream before its
// first token.
func (s *Server) writeAdmissionError(w http.ResponseWriter, ctx context.Context, err error) {
	switch {
	case ctx.Err() != nil:
		// Client already gone; nothing useful to write.
	case errors.Is(err, inference.ErrModelNotFound):
		writeError(w, http.StatusNotFound, "invalid_request_error", "model_not_found", err.Error())
	case errors.Is(err, inference.ErrRequestTooLarge):
		writeError(w, http.StatusBadRequest, "invalid_request_error", "context_length_exceeded", err.Error())
	case errors.Is(err, inference.ErrInvalidRequest):
		writeError(w, http.StatusBadRequest, "invalid_request_error", "", err.Error())
	case errors.Is(err, inference.ErrQueueFull):
		s.setRetryAfter(w, err)
		writeError(w, http.StatusServiceUnavailable, "server_overloaded", "queue_full", "the wait queue is full, retry later")
	case errors.Is(err, inference.ErrQueueTimeout):
		s.setRetryAfter(w, err)
		writeError(w, http.StatusServiceUnavailable, "server_overloaded", "queue_timeout", "no capacity was freed in time, retry later")
	case errors.Is(err, inference.ErrAtCapacity):
		s.setRetryAfter(w, err)
		writeError(w, http.StatusServiceUnavailable, "server_overloaded", "at_capacity", "no worker has free capacity, retry later")
	case errors.Is(err, inference.ErrNoHealthyWorkers):
		s.setRetryAfter(w, err)
		writeError(w, http.StatusServiceUnavailable, "server_error", "no_healthy_workers", "no healthy worker serves this model right now, retry later")
	case errors.Is(err, inference.ErrWorkerFailure):
		s.setRetryAfter(w, err)
		writeError(w, http.StatusServiceUnavailable, "server_error", "worker_unavailable", err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "server_error", "", err.Error())
	}
}

// setRetryAfter writes the Retry-After header (whole seconds, at least 1),
// preferring the scheduler's load-based value over the configured default.
func (s *Server) setRetryAfter(w http.ResponseWriter, err error) {
	d := s.cfg.RetryAfter
	var ra *inference.RetryAfterError
	if errors.As(err, &ra) && ra.RetryAfter > 0 {
		d = ra.RetryAfter
	}
	w.Header().Set("Retry-After", strconv.Itoa(max(1, int(math.Ceil(d.Seconds())))))
}

// ---- helpers ----

type rawSSE string

func writeSSE(w http.ResponseWriter, rc *http.ResponseController, v any) error {
	var payload []byte
	if raw, ok := v.(rawSSE); ok {
		payload = []byte(raw)
	} else {
		var err error
		if payload, err = json.Marshal(v); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
		return err
	}
	return rc.Flush()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, typ, code, msg string) {
	writeJSON(w, status, map[string]apiError{"error": {Message: msg, Type: typ, Code: code}})
}

func finishReason(f inference.FinishReason) *string {
	if f == inference.FinishNone {
		return nil
	}
	s := string(f)
	return &s
}

func usageOf(prompt, completion int) usage {
	return usage{
		PromptTokens:     prompt,
		CompletionTokens: completion,
		TotalTokens:      prompt + completion,
	}
}

func newRequestID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "chatcmpl-" + hex.EncodeToString(b[:])
}
