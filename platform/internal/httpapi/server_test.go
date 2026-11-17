package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/scheduler"
	"llm-inference-platform/internal/simworker"
)

const model = "sim-model"

func newTestServer(t *testing.T, cfg simworker.Config) (*httptest.Server, *simworker.Worker) {
	t.Helper()
	return newTestServerWithQueue(t, cfg, scheduler.Config{QueueTimeout: 50 * time.Millisecond})
}

func newTestServerWithQueue(t *testing.T, cfg simworker.Config, scfg scheduler.Config) (*httptest.Server, *simworker.Worker) {
	t.Helper()
	cfg.ID = "w1"
	cfg.Models = []string{model}
	if cfg.TokenBudget == 0 {
		cfg.TokenBudget = 1000
	}
	w, err := simworker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sched := scheduler.New(scfg, w)
	t.Cleanup(sched.Close)
	api := New(sched, DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(api.Handler())
	t.Cleanup(ts.Close)
	return ts, w
}

func post(t *testing.T, ctx context.Context, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func chatBody(maxTokens int, stream bool, extra string) string {
	b, _ := json.Marshal(map[string]any{
		"model":      model,
		"messages":   []map[string]string{{"role": "user", "content": "Hello there, how are you?"}},
		"max_tokens": maxTokens,
		"stream":     stream,
	})
	if extra != "" {
		return strings.TrimSuffix(string(b), "}") + "," + extra + "}"
	}
	return string(b)
}

// readEvents returns the data payloads of an SSE stream.
func readEvents(t *testing.T, r io.Reader) []string {
	t.Helper()
	var events []string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		if data, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
			events = append(events, data)
		}
	}
	return events
}

func waitReserved(t *testing.T, w *simworker.Worker, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for w.Snapshot().TokensReserved != want {
		if time.Now().After(deadline) {
			t.Fatalf("reserved = %d, want %d", w.Snapshot().TokensReserved, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStreamingChatCompletion(t *testing.T) {
	ts, w := newTestServer(t, simworker.Config{})
	resp := post(t, context.Background(), ts.URL, chatBody(4, true, `"stream_options":{"include_usage":true}`))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type = %q", ct)
	}
	events := readEvents(t, resp.Body)
	// 4 token chunks + usage chunk + [DONE]
	if len(events) != 6 || events[5] != "[DONE]" {
		t.Fatalf("events = %v", events)
	}
	var text strings.Builder
	for i, e := range events[:4] {
		var c completionChunk
		if err := json.Unmarshal([]byte(e), &c); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
		if c.Object != "chat.completion.chunk" || c.Model != model || len(c.Choices) != 1 {
			t.Fatalf("event %d = %+v", i, c)
		}
		if i == 0 && c.Choices[0].Delta.Role != "assistant" {
			t.Errorf("first chunk has no assistant role")
		}
		fr := c.Choices[0].FinishReason
		if (i == 3) != (fr != nil) || (fr != nil && *fr != "length") {
			t.Errorf("event %d finish_reason = %v", i, fr)
		}
		text.WriteString(c.Choices[0].Delta.Content)
	}
	if text.Len() == 0 {
		t.Error("empty generated text")
	}
	var u completionChunk
	if err := json.Unmarshal([]byte(events[4]), &u); err != nil || u.Usage == nil || u.Usage.CompletionTokens != 4 {
		t.Fatalf("usage chunk = %s (err %v)", events[4], err)
	}
	waitReserved(t, w, 0)
}

func TestNonStreamingChatCompletion(t *testing.T) {
	ts, _ := newTestServer(t, simworker.Config{})
	resp := post(t, context.Background(), ts.URL, chatBody(3, false, ""))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var c completion
	if err := json.NewDecoder(resp.Body).Decode(&c); err != nil {
		t.Fatal(err)
	}
	if c.Object != "chat.completion" || len(c.Choices) != 1 || c.Choices[0].Message.Content == "" {
		t.Fatalf("completion = %+v", c)
	}
	if c.Usage.CompletionTokens != 3 || c.Usage.PromptTokens == 0 {
		t.Fatalf("usage = %+v", c.Usage)
	}
}

func TestAdmissionErrors(t *testing.T) {
	ts, w := newTestServer(t, simworker.Config{TokenBudget: 100, InterTokenLatency: time.Hour})

	cases := []struct {
		name   string
		body   string
		status int
	}{
		{"bad json", "{", http.StatusBadRequest},
		{"no messages", `{"model":"sim-model","messages":[]}`, http.StatusBadRequest},
		{"max_tokens over limit", chatBody(100000, true, ""), http.StatusBadRequest},
		{"unknown model", `{"model":"nope","messages":[{"role":"user","content":"hi"}]}`, http.StatusNotFound},
		{"never fits", chatBody(200, true, ""), http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := post(t, context.Background(), ts.URL, tc.body)
			defer resp.Body.Close()
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.status)
			}
			var e map[string]apiError
			if err := json.NewDecoder(resp.Body).Decode(&e); err != nil || e["error"].Message == "" {
				t.Fatalf("error body: %v %v", e, err)
			}
		})
	}

	// Saturate the worker with a long stream: the next request waits in the
	// queue (50ms timeout) and then gets 503 + Retry-After, before any header.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hog := post(t, ctx, ts.URL, chatBody(80, true, ""))
	defer hog.Body.Close()
	if hog.StatusCode != http.StatusOK {
		t.Fatalf("hog status = %d", hog.StatusCode)
	}
	resp := post(t, context.Background(), ts.URL, chatBody(50, true, ""))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("status = %d, Retry-After = %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	var e map[string]apiError
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil || e["error"].Code != "queue_timeout" {
		t.Fatalf("error body: %v %v", e, err)
	}
	if r := w.Snapshot().TokensReserved; r == 0 {
		t.Fatal("hog lost its reservation")
	}
}

func TestQueuedRequestCompletesAfterCapacityIsFreed(t *testing.T) {
	// Budget 100: the first request (about 87 tokens) leaves no room for the
	// second (about 57), which waits in the queue and streams once the first
	// one ends.
	ts, w := newTestServerWithQueue(t,
		simworker.Config{TokenBudget: 100, InterTokenLatency: 2 * time.Millisecond, LeaseTTL: time.Second},
		scheduler.Config{QueueTimeout: 5 * time.Second})
	first := post(t, context.Background(), ts.URL, chatBody(80, true, ""))
	defer first.Body.Close()

	done := make(chan *http.Response, 1)
	go func() { done <- post(t, context.Background(), ts.URL, chatBody(50, true, "")) }()

	readEvents(t, first.Body) // the first stream runs to completion
	var second *http.Response
	select {
	case second = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("queued request never admitted")
	}
	defer second.Body.Close()
	if second.StatusCode != http.StatusOK {
		t.Fatalf("queued request status = %d", second.StatusCode)
	}
	if events := readEvents(t, second.Body); len(events) != 51 || events[50] != "[DONE]" {
		t.Fatalf("queued request: %d events", len(events))
	}
	waitReserved(t, w, 0)

	resp, err := http.Get(ts.URL + "/debug/scheduler")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var st scheduler.Stats
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil || st.AdmittedFromQueue != 1 || st.Waiting != 0 {
		t.Fatalf("/debug/scheduler = %+v, err = %v", st, err)
	}
}

func TestClientDisconnectReleasesCapacity(t *testing.T) {
	ts, w := newTestServer(t, simworker.Config{InterTokenLatency: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	resp := post(t, ctx, ts.URL, chatBody(100, true, ""))
	defer resp.Body.Close()

	// Wait for the first token so the stream is really in progress.
	if _, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	if w.Snapshot().TokensReserved == 0 {
		t.Fatal("expected reserved capacity during the stream")
	}
	cancel()
	waitReserved(t, w, 0)
}

func TestModelsAndDebugEndpoints(t *testing.T) {
	ts, _ := newTestServer(t, simworker.Config{})
	resp, err := http.Get(ts.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var models struct {
		Data []struct{ ID string } `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&models); err != nil || len(models.Data) != 1 || models.Data[0].ID != model {
		t.Fatalf("models = %+v, err = %v", models, err)
	}

	resp2, err := http.Get(ts.URL + "/debug/workers")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	var snaps []inference.WorkerSnapshot
	if err := json.NewDecoder(resp2.Body).Decode(&snaps); err != nil || len(snaps) != 1 || snaps[0].WorkerID != "w1" {
		t.Fatalf("snapshots = %+v, err = %v", snaps, err)
	}
}
