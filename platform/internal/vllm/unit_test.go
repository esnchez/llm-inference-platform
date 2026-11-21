package vllm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"llm-inference-platform/internal/inference"
)

func TestParseMetrics(t *testing.T) {
	text := `# HELP vllm:cache_config_info Information of the LLMEngine CacheConfig
# TYPE vllm:cache_config_info gauge
vllm:cache_config_info{block_size="16",cache_dtype="auto",num_cpu_blocks="None",num_gpu_blocks="2048"} 1.0
vllm:kv_cache_usage_perc{engine="0",model_name="m"} 0.25
vllm:num_requests_running{model_name="m"} 3.0 1700000000000
vllm:request_success_total{finished_reason="stop",model_name="m"} 10.0
vllm:request_success_total{finished_reason="length",model_name="m"} 5.0
weird{a="x\"y,z",b="2"} 7
process_start_time_seconds 1.7e+09
not a metric line
`
	ms := indexMetrics(parseMetrics(strings.NewReader(text)))
	if s, ok := ms.first(metricCacheConfig...); !ok || s.labels["block_size"] != "16" || s.labels["num_gpu_blocks"] != "2048" {
		t.Fatalf("cache_config_info = %+v", s)
	}
	if s, _ := ms.first(metricKVUsage...); s.value != 0.25 {
		t.Fatalf("kv usage = %v", s.value)
	}
	if v, _ := ms.sum(metricSuccesses...); v != 15 {
		t.Fatalf("successes summed over labels = %v", v)
	}
	if s, ok := ms.first("weird"); !ok || s.labels["a"] != `x"y,z` || s.labels["b"] != "2" || s.value != 7 {
		t.Fatalf("escaped labels = %+v", s)
	}
	if s, _ := ms.first(metricProcessStart...); s.value != 1.7e9 {
		t.Fatalf("process start = %v", s.value)
	}
	if _, ok := ms.first("vllm:gpu_cache_usage_perc"); ok {
		t.Fatal("absent metric found")
	}
}

func TestErrorTranslation(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   error
	}{
		{400, `{"error":{"message":"This model's maximum context length is 4096 tokens. However, you requested 5000 tokens","type":"BadRequestError","code":400}}`, inference.ErrRequestTooLarge},
		{400, `{"object":"error","message":"This model's maximum context length is 2048 tokens.","type":"BadRequestError","code":400}`, inference.ErrRequestTooLarge},
		{400, `{"error":{"message":"temperature must be non-negative"}}`, inference.ErrInvalidRequest},
		{422, `{"detail":"bad"}`, inference.ErrInvalidRequest},
		{500, `{"error":{"message":"boom"}}`, inference.ErrWorkerFailure},
		{503, ``, inference.ErrWorkerFailure},
		{404, `{"error":{"message":"The model x does not exist."}}`, inference.ErrWorkerFailure},
		{401, `{"error":{"message":"Unauthorized"}}`, inference.ErrWorkerFailure},
	}
	for _, c := range cases {
		err := statusError("w", c.status, []byte(c.body))
		if !errors.Is(err, c.want) {
			t.Errorf("HTTP %d %s: %v, want %v", c.status, c.body, err, c.want)
		}
		if errors.Is(err, inference.ErrWorkerFailure) && c.want != inference.ErrWorkerFailure {
			t.Errorf("HTTP %d: a request error must never be a worker failure", c.status)
		}
	}
	if err := statusError("w", 400, []byte(`{"error":{"message":"maximum context length"}}`)); !strings.Contains(err.Error(), "maximum context length") {
		t.Errorf("the engine's message must reach the client: %v", err)
	}
}

func TestRequestBodyControlledFieldsWin(t *testing.T) {
	req := inference.InferenceRequest{
		Model: "m", MaxNewTokens: 7,
		Messages: []inference.Message{{Role: "system", Content: "s"}, {Role: "user", Content: "u", Name: "ana"}},
		Params: map[string]json.RawMessage{
			"temperature": json.RawMessage(`0.3`), "stop": json.RawMessage(`["\n"]`),
			// A forwarded param must never override what the platform controls.
			"max_tokens": json.RawMessage(`9999`), "stream": json.RawMessage(`false`),
		},
	}
	b, err := requestBody(req)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got["max_tokens"] != float64(7) || got["stream"] != true || got["temperature"] != 0.3 || got["model"] != "m" {
		t.Fatalf("body = %s", b)
	}
	if so := got["stream_options"].(map[string]any); so["include_usage"] != true {
		t.Fatalf("stream_options = %v", so)
	}
	msgs := got["messages"].([]any)
	if len(msgs) != 2 || msgs[1].(map[string]any)["name"] != "ana" {
		t.Fatalf("messages = %v", msgs)
	}
}

func relayString(t *testing.T, stream string) ([]inference.TokenChunk, error) {
	t.Helper()
	w := &Worker{cfg: Config{ID: "w"}}
	out := make(chan inference.TokenChunk, 100)
	err := w.relay(context.Background(), strings.NewReader(stream), out)
	close(out)
	var got []inference.TokenChunk
	for c := range out {
		got = append(got, c)
	}
	return got, err
}

func TestRelay(t *testing.T) {
	ok := `: keep-alive

data: {"choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}

data: {"choices":[{"index":0,"delta":{"content":"Hola"},"finish_reason":null}],"usage":null}
data: {"choices":[{"index":0,"delta":{"content":" mundo"},"finish_reason":null}]}

data: {"choices":[{"index":0,"delta":{"content":""},"finish_reason":"length"}]}

data: {"choices":[],"usage":{"prompt_tokens":12,"completion_tokens":2,"total_tokens":14}}

data: [DONE]

`
	got, err := relayString(t, ok)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Text != "Hola" || got[1].Text != " mundo" || got[0].Index != 0 || got[1].Index != 1 {
		t.Fatalf("chunks = %+v", got)
	}
	last := got[2]
	if last.Text != "" || last.FinishReason != inference.FinishLength || last.Usage == nil ||
		last.Usage.PromptTokens != 12 || last.Usage.CompletionTokens != 2 {
		t.Fatalf("last chunk = %+v", last)
	}

	for name, c := range map[string]struct {
		stream string
		want   string
	}{
		"no [DONE]":        {`data: {"choices":[{"delta":{"content":"a"}}]}` + "\n\n", "without [DONE]"},
		"error event":      {`data: {"choices":[{"delta":{"content":"a"}}]}` + "\n\n" + `data: {"error":{"message":"CUDA error"}}` + "\n\n", "CUDA error"},
		"legacy error":     {`data: {"object":"error","message":"engine dead"}` + "\n\n", "engine dead"},
		"unreadable event": {"data: {not json}\n\n", "unreadable"},
	} {
		_, err := relayString(t, c.stream)
		if !errors.Is(err, inference.ErrWorkerFailure) || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v", name, err)
		}
	}

	// No content at all (e.g. an immediate stop): one empty final chunk.
	got, err = relayString(t, `data: {"choices":[{"delta":{"content":""},"finish_reason":"stop"}]}`+"\n\ndata: [DONE]\n\n")
	if err != nil || len(got) != 1 || got[0].FinishReason != inference.FinishStop {
		t.Fatalf("empty output: %+v, %v", got, err)
	}
}
