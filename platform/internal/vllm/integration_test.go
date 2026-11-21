package vllm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"llm-inference-platform/internal/httpapi"
	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/scheduler"
	"llm-inference-platform/internal/vllm/fakevllm"
	"llm-inference-platform/internal/workertest"
)

// engine starts a fake vLLM and returns it with its URL.
func engine(t *testing.T, cfg fakevllm.Config) (*fakevllm.Server, string) {
	t.Helper()
	f := fakevllm.New(cfg)
	ts := httptest.NewServer(f.Handler())
	t.Cleanup(ts.Close)
	return f, ts.URL
}

// adapter discovers a fake vLLM and returns its worker.
func adapter(t *testing.T, cfg Config) *Worker {
	t.Helper()
	info, err := Discover(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	w, err := New(cfg, info)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// TestContract: the adapter passes the same battery as the simulator.
func TestContract(t *testing.T) {
	workertest.Run(t, func(t *testing.T, o workertest.Options) inference.Worker {
		_, url := engine(t, fakevllm.Config{Model: o.Model, TokenDelay: o.TokenDelay})
		return adapter(t, Config{BaseURL: url, TokenBudget: o.TokenBudget, LeaseTTL: o.LeaseTTL})
	})
}

func TestDiscover(t *testing.T) {
	_, url := engine(t, fakevllm.Config{Model: "llama", MaxModelLen: 2048, NumGPUBlocks: 500, BlockSize: 16})
	info, err := Discover(context.Background(), Config{BaseURL: url})
	if err != nil {
		t.Fatal(err)
	}
	if info.TokenBudget != 8000 || info.MaxModelLen != 2048 || len(info.Models) != 1 || info.Models[0] != "llama" {
		t.Fatalf("info = %+v", info)
	}
	w, err := New(Config{BaseURL: url}, info)
	if err != nil {
		t.Fatal(err)
	}
	if s := w.Snapshot(); s.TokenBudget != 8000 || s.MaxRequestTokens != 2048 || !strings.HasPrefix(w.ID(), "vllm-127.0.0.1:") {
		t.Fatalf("snapshot = %+v, id %s", s, w.ID())
	}

	f, url2 := engine(t, fakevllm.Config{})
	f.SetFaults(fakevllm.Faults{Loading: true})
	if _, err := Discover(context.Background(), Config{BaseURL: url2}); !errors.Is(err, inference.ErrWorkerFailure) {
		t.Fatalf("loading server: %v", err)
	}
	if _, err := Discover(context.Background(), Config{BaseURL: "http://127.0.0.1:1"}); err == nil {
		t.Fatal("unreachable server discovered")
	}
}

func TestForwardedRequestAndUsage(t *testing.T) {
	f, url := engine(t, fakevllm.Config{Model: "m", APIKey: "secret"})
	w := adapter(t, Config{BaseURL: url, APIKey: "secret"})
	req := inference.InferenceRequest{ID: "req-1", Model: "m", PromptTokens: 5, MaxNewTokens: 4,
		Messages: []inference.Message{{Role: "user", Content: "hola"}},
		Params:   map[string]json.RawMessage{"temperature": json.RawMessage(`0.1`), "seed": json.RawMessage(`3`)}}
	l, err := w.AcquireLease(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	chunks, wait := inference.StartStream(context.Background(), l, 8)
	var got []inference.TokenChunk
	for c := range chunks {
		got = append(got, c)
	}
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	last := got[len(got)-1]
	if len(got) != 5 || last.Usage == nil || last.Usage.CompletionTokens != 4 || last.FinishReason != inference.FinishLength {
		t.Fatalf("chunks = %+v", got)
	}
	body := f.LastRequest()
	for k, want := range map[string]string{"temperature": "0.1", "seed": "3", "max_tokens": "4", "stream": "true",
		"stream_options": `{"include_usage":true}`, "model": `"m"`} {
		if string(body[k]) != want {
			t.Errorf("vLLM received %s = %s, want %s", k, body[k], want)
		}
	}
}

func TestErrorsReachTheContract(t *testing.T) {
	f, url := engine(t, fakevllm.Config{Model: "m", MaxModelLen: 50})
	w := adapter(t, Config{BaseURL: url, TokenBudget: 1000})
	run := func(req inference.InferenceRequest) error {
		l, err := w.AcquireLease(context.Background(), req)
		if err != nil {
			return err
		}
		defer l.Release()
		chunks, wait := inference.StartStream(context.Background(), l, 8)
		for range chunks {
		}
		return wait()
	}
	// Fits our estimate (and the ledger's max_model_len) but not the engine's tokenizer.
	long := inference.InferenceRequest{ID: "a", Model: "m", PromptTokens: 5, MaxNewTokens: 44,
		Messages: []inference.Message{{Role: "user", Content: strings.Repeat("x", 40)}}}
	if err := run(long); !errors.Is(err, inference.ErrRequestTooLarge) {
		t.Fatalf("context length: %v", err)
	}
	f.SetFaults(fakevllm.Faults{Status: 500})
	short := inference.InferenceRequest{ID: "b", Model: "m", PromptTokens: 2, MaxNewTokens: 3, Messages: []inference.Message{{Role: "user", Content: "hi"}}}
	if err := run(short); !errors.Is(err, inference.ErrWorkerFailure) {
		t.Fatalf("500: %v", err)
	}
	f.SetFaults(fakevllm.Faults{CutAfter: 1})
	if err := run(short); !errors.Is(err, inference.ErrWorkerFailure) || !strings.Contains(err.Error(), "[DONE]") {
		t.Fatalf("cut stream: %v", err)
	}
	f.SetFaults(fakevllm.Faults{ErrorEventAt: 1})
	if err := run(short); !errors.Is(err, inference.ErrWorkerFailure) || !strings.Contains(err.Error(), "injected engine error") {
		t.Fatalf("error event: %v", err)
	}
}

func TestCancellationAbortsTheEngineRequest(t *testing.T) {
	f, url := engine(t, fakevllm.Config{Model: "m", TokenDelay: time.Hour})
	w := adapter(t, Config{BaseURL: url})
	l, err := w.AcquireLease(context.Background(), inference.InferenceRequest{ID: "a", Model: "m", Prompt: "hola", PromptTokens: 2, MaxNewTokens: 5})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	chunks, wait := inference.StartStream(ctx, l, 0)
	<-chunks
	cancel()
	for range chunks {
	}
	if err := wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for f.Aborted() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("vLLM never saw the connection close: it would keep generating and holding VRAM")
		}
		time.Sleep(time.Millisecond)
	}
	if s := w.Snapshot(); s.TokensReserved != 0 {
		t.Fatalf("capacity leaked: %+v", s)
	}
}

func TestHealthEngineStatsAndRestarts(t *testing.T) {
	for _, noProcess := range []bool{false, true} {
		f, url := engine(t, fakevllm.Config{Model: "m", NumGPUBlocks: 100, NoProcessMetrics: noProcess})
		w := adapter(t, Config{BaseURL: url})
		ctx := context.Background()
		st1, err := w.Health(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// Two requests with the same first message: the second hits the cache.
		for i := range 2 {
			l, _ := w.AcquireLease(ctx, inference.InferenceRequest{ID: "r", Model: "m", PromptTokens: 30, MaxNewTokens: 1,
				Messages: []inference.Message{{Role: "system", Content: strings.Repeat("s", 120)}, {Role: "user", Content: "q" + strings.Repeat("x", i)}}})
			chunks, wait := inference.StartStream(ctx, l, 4)
			for range chunks {
			}
			if err := wait(); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := w.Health(ctx); err != nil {
			t.Fatal(err)
		}
		s := w.Snapshot()
		if s.Engine == nil || s.PrefixCacheBlocks != 100 || s.PrefixBlocksQueried == 0 || s.PrefixBlocksHit == 0 {
			t.Fatalf("noProcess=%v: engine stats = %+v / %+v", noProcess, s, s.Engine)
		}

		time.Sleep(20 * time.Millisecond) // a different process start time
		f.Restart()
		st2, err := w.Health(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if st2.InstanceID == st1.InstanceID {
			t.Fatalf("noProcess=%v: restart not detected (%s)", noProcess, st2.InstanceID)
		}
		f.SetFaults(fakevllm.Faults{Unhealthy: true})
		if _, err := w.Health(ctx); !errors.Is(err, inference.ErrWorkerFailure) {
			t.Fatalf("unhealthy: %v", err)
		}
	}
}

// registry records the connector's calls.
type registry struct {
	mu      sync.Mutex
	added   []inference.Worker
	removed []string
}

func (r *registry) AddWorker(w inference.Worker) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.added = append(r.added, w)
	return nil
}
func (r *registry) RemoveWorker(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed = append(r.removed, id)
	return nil
}
func (r *registry) counts() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.added), len(r.removed)
}

func TestConnectorWaitsForTheModelThenRegisters(t *testing.T) {
	f, url := engine(t, fakevllm.Config{Model: "m"})
	f.SetFaults(fakevllm.Faults{Loading: true})
	reg := &registry{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &Connector{Config: Config{BaseURL: url}, Registry: reg, MinBackoff: 5 * time.Millisecond,
		MaxBackoff: 10 * time.Millisecond, Recheck: 10 * time.Millisecond,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	go c.Run(ctx)
	time.Sleep(50 * time.Millisecond)
	if a, _ := reg.counts(); a != 0 {
		t.Fatal("registered while the model was loading")
	}
	f.SetFaults(fakevllm.Faults{})
	waitUntil(t, "registration", func() bool { a, _ := reg.counts(); return a == 1 })
	time.Sleep(50 * time.Millisecond)
	if a, r := reg.counts(); a != 1 || r != 0 {
		t.Fatalf("an unchanged server was re-registered: added %d removed %d", a, r)
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestEndToEnd: API + scheduler + adapter + fake vLLM, with a retry when
// one engine fails and the engine's usage in the response.
func TestEndToEnd(t *testing.T) {
	broken, url1 := engine(t, fakevllm.Config{Model: "m"})
	_, url2 := engine(t, fakevllm.Config{Model: "m"})
	broken.SetFaults(fakevllm.Faults{Status: 503})
	s := scheduler.New(scheduler.Config{HealthInterval: -1, BreakerFailures: 100, Placement: &scheduler.RoundRobin{}},
		adapter(t, Config{BaseURL: url1}), adapter(t, Config{BaseURL: url2}))
	t.Cleanup(s.Close)
	ts := httptest.NewServer(httpapi.New(s, httpapi.DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	t.Cleanup(ts.Close)

	for range 4 {
		resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(
			`{"model":"m","max_tokens":3,"temperature":0.5,"messages":[{"role":"user","content":"hola"}]}`))
		if err != nil {
			t.Fatal(err)
		}
		var c struct {
			Choices []struct {
				Message struct{ Content string } `json:"message"`
			} `json:"choices"`
			Usage struct {
				PromptTokens     int `json:"prompt_tokens"`
				CompletionTokens int `json:"completion_tokens"`
			} `json:"usage"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&c)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || c.Choices[0].Message.Content != "Hola desde un" || c.Usage.CompletionTokens != 3 {
			t.Fatalf("%d %+v", resp.StatusCode, c)
		}
		// The engine's tokenizer counted 3 + len/4 = 4 prompt tokens, not our estimate.
		if c.Usage.PromptTokens != 4 {
			t.Fatalf("usage.prompt_tokens = %d, want the engine's 4", c.Usage.PromptTokens)
		}
	}
	if st := s.Stats(); st.Retries == 0 || st.Retries != st.RetrySuccesses {
		t.Fatalf("retries %d, successes %d", st.Retries, st.RetrySuccesses)
	}
}
