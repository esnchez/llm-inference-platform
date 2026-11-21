package vllm

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"llm-inference-platform/internal/inference"
)

// The tests in this file run against real vLLM 0.34.0 responses captured
// in testdata (Phase 6b-CPU, decision 1), so the parsers and the error
// table are checked against what vLLM really sends, not against what the
// fake assumes.

const captures = "testdata/vllm-0.34.0"

func capture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(captures, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCapturedStream(t *testing.T) {
	got, err := relayString(t, string(capture(t, "stream.sse")))
	if err != nil {
		t.Fatal(err)
	}
	last := got[len(got)-1]
	if last.Usage == nil || last.Usage.PromptTokens == 0 || last.Usage.CompletionTokens == 0 || last.FinishReason == inference.FinishNone {
		t.Fatalf("last chunk = %+v", last)
	}
	text := ""
	for _, c := range got[:len(got)-1] {
		if c.Text == "" {
			t.Fatalf("empty text chunk before the end: %+v", got)
		}
		text += c.Text
	}
	if text == "" {
		t.Fatal("no text")
	}
}

func TestCapturedErrors(t *testing.T) {
	for file, c := range map[string]struct {
		status int
		want   error
	}{
		"error-context-length.json": {400, inference.ErrRequestTooLarge},
		"error-bad-param.json":      {400, inference.ErrInvalidRequest},
		"error-unknown-model.json":  {404, inference.ErrWorkerFailure},
	} {
		if err := statusError("w", c.status, capture(t, file)); !errors.Is(err, c.want) {
			t.Errorf("%s: %v, want %v", file, err, c.want)
		}
	}
}

// TestCapturedDiscovery serves the captured /v1/models and /metrics and
// checks what the adapter learns: on CPU vLLM uses 128-token blocks.
func TestCapturedDiscovery(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(capture(t, "models.json")) })
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(capture(t, "metrics.txt")) })
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	info, err := Discover(context.Background(), Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatal(err)
	}
	if info.BlockSize != 128 || info.NumGPUBlocks != 682 || info.TokenBudget != 87296 || info.MaxModelLen != 4096 ||
		len(info.Models) != 1 || info.Models[0] != "Qwen/Qwen2.5-0.5B-Instruct" {
		t.Fatalf("info = %+v", info)
	}
	w, err := New(Config{BaseURL: ts.URL}, info)
	if err != nil {
		t.Fatal(err)
	}
	st, err := w.Health(context.Background())
	if err != nil || st.InstanceID == "" {
		t.Fatalf("health: %v %+v", err, st)
	}
	if s := w.Snapshot(); s.Engine == nil || s.PrefixCacheBlocks != 682 {
		t.Fatalf("engine stats from the real /metrics: %+v", s)
	}
}
