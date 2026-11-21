package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/ledger"
	"llm-inference-platform/internal/scheduler"
	"llm-inference-platform/internal/workertest"
)

func parse(t *testing.T, body string) (inference.InferenceRequest, error) {
	t.Helper()
	var req chatRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return inference.InferenceRequest{}, err
	}
	if err := json.Unmarshal([]byte(body), &req.raw); err != nil {
		t.Fatal(err)
	}
	return New(nil, DefaultConfig(), nil).toInferenceRequest(req)
}

func TestParamsAreForwarded(t *testing.T) {
	r, err := parse(t, `{"model":"m","max_tokens":5,"stream":true,"stream_options":{"include_usage":true},
		"messages":[{"role":"system","content":"sé breve"},{"role":"user","content":"hola","name":"ana"}],
		"temperature":0.2,"top_p":0.9,"stop":["\n"],"seed":7,"n":1,"logprobs":false,"top_k":40,
		"response_format":{"type":"json_schema","json_schema":{"name":"x","schema":{"type":"object"}}}}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"temperature", "top_p", "stop", "seed", "n", "logprobs", "top_k", "response_format"} {
		if _, ok := r.Params[name]; !ok {
			t.Errorf("%s not forwarded", name)
		}
	}
	for _, name := range []string{"model", "messages", "max_tokens", "stream", "stream_options"} {
		if _, ok := r.Params[name]; ok {
			t.Errorf("controlled field %s forwarded", name)
		}
	}
	if string(r.Params["stop"]) != `["\n"]` {
		t.Errorf("stop forwarded as %s, want the raw JSON", r.Params["stop"])
	}
	want := []inference.Message{{Role: "system", Content: "sé breve"}, {Role: "user", Content: "hola", Name: "ana"}}
	if len(r.Messages) != 2 || r.Messages[0] != want[0] || r.Messages[1] != want[1] {
		t.Fatalf("messages = %+v", r.Messages)
	}
	if r.Prompt != "system: sé breve\nuser: hola\n" {
		t.Fatalf("prompt = %q", r.Prompt)
	}
}

func TestUnsupportedParamsAreRejected(t *testing.T) {
	for _, c := range []struct{ extra, want string }{
		{`"n":2`, "n (only one choice"},
		{`"best_of":3`, "best_of"},
		{`"logprobs":true`, "logprobs (logprobs are not supported)"},
		{`"top_logprobs":5`, "top_logprobs"},
		{`"tools":[{"type":"function","function":{"name":"f"}}]`, "tools (tool and function calls"},
		{`"tool_choice":"auto"`, "tool_choice"},
	} {
		_, err := parse(t, `{"model":"m","max_tokens":5,"messages":[{"role":"user","content":"hi"}],`+c.extra+`}`)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want it to mention %q", c.extra, err, c.want)
		}
	}
	// null is the same as absent.
	if _, err := parse(t, `{"model":"m","max_tokens":5,"messages":[{"role":"user","content":"hi"}],"tools":null,"n":null}`); err != nil {
		t.Errorf("null fields rejected: %v", err)
	}
}

func TestMessageContentMustBeAString(t *testing.T) {
	for body, want := range map[string]string{
		`{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`: "content parts",
		`{"model":"m","messages":[{"role":"user","content":null}]}`:                          "content is required",
		`{"model":"m","messages":[{"role":"user"}]}`:                                         "content is required",
		`{"model":"m","messages":[{"content":"hi"}]}`:                                        "role is required",
	} {
		if _, err := parse(t, body); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", body, err, want)
		}
	}
}

// engineServer serves model with a worker that reports the engine's usage.
func engineServer(t *testing.T, usage *inference.Usage, maxReq int, obs Observer) *httptest.Server {
	t.Helper()
	w := workertest.NewStub(ledger.Config{ID: "engine", Models: []string{model}, TokenBudget: 1000, MaxRequestTokens: maxReq},
		workertest.Words(usage, "hola", " mundo"))
	s := scheduler.New(scheduler.Config{HealthInterval: -1}, w)
	t.Cleanup(s.Close)
	cfg := DefaultConfig()
	cfg.Observer = obs
	ts := httptest.NewServer(New(s, cfg, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	t.Cleanup(ts.Close)
	return ts
}

func TestEngineUsageIsReported(t *testing.T) {
	obs := &records{}
	ts := engineServer(t, &inference.Usage{PromptTokens: 11, CompletionTokens: 2}, 0, obs)

	resp := post(t, context.Background(), ts.URL, chatBody(10, false, ""))
	var c completion
	if err := json.NewDecoder(resp.Body).Decode(&c); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if c.Usage.PromptTokens != 11 || c.Usage.CompletionTokens != 2 || c.Choices[0].Message.Content != "hola mundo" {
		t.Fatalf("non-streaming: %+v", c)
	}

	resp = post(t, context.Background(), ts.URL, chatBody(10, true, `"stream_options":{"include_usage":true}`))
	events := readEvents(t, resp.Body)
	resp.Body.Close()
	if !strings.Contains(events[len(events)-2], `"prompt_tokens":11,"completion_tokens":2`) {
		t.Fatalf("streaming usage chunk = %s", events[len(events)-2])
	}

	rec := obs.wait(t, 2)[1]
	if !rec.UsageReported || rec.PromptTokens != 11 || rec.CompletionTokens != 2 || rec.EstimatedPromptTokens == 0 {
		t.Fatalf("record = %+v", rec)
	}
}

// TestMaxModelLenRejectsAtOnce: a request that fits the budget but not the
// engine's max_model_len gets a 400 at once, without queueing (C3).
func TestMaxModelLenRejectsAtOnce(t *testing.T) {
	ts := engineServer(t, nil, 50, &records{})
	resp := post(t, context.Background(), ts.URL, chatBody(100, true, ""))
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "context_length_exceeded") {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
}
