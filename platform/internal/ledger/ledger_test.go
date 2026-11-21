package ledger

import (
	"context"
	"errors"
	"testing"

	"llm-inference-platform/internal/inference"
)

func noop(context.Context, inference.InferenceRequest, chan<- inference.TokenChunk) error { return nil }

func TestNewValidates(t *testing.T) {
	for i, c := range []Config{
		{Models: []string{"m"}, TokenBudget: 1},
		{ID: "w", TokenBudget: 1},
		{ID: "w", Models: []string{"m"}},
		{ID: "w", Models: []string{"m"}, TokenBudget: 1, MaxRequestTokens: -1},
	} {
		if _, err := New(c); err == nil {
			t.Errorf("config %d accepted", i)
		}
	}
}

// TestMaxRequestTokens: a request may fit in the budget but exceed the
// engine's max_model_len (conflict C3 of Phase 6a).
func TestMaxRequestTokens(t *testing.T) {
	l, err := New(Config{ID: "w", Models: []string{"m"}, TokenBudget: 1000, MaxRequestTokens: 100})
	if err != nil {
		t.Fatal(err)
	}
	_, err = l.Acquire(context.Background(), inference.InferenceRequest{Model: "m", PromptTokens: 90, MaxNewTokens: 20}, noop)
	if !errors.Is(err, inference.ErrRequestTooLarge) {
		t.Fatalf("over max_model_len: %v", err)
	}
	if s := l.Snapshot(); s.MaxRequestTokens != 100 || s.TokenBudget != 1000 {
		t.Fatalf("snapshot = %+v", s)
	}
	big, _ := New(Config{ID: "w", Models: []string{"m"}, TokenBudget: 100, MaxRequestTokens: 5000})
	if s := big.Snapshot(); s.MaxRequestTokens != 0 {
		t.Fatalf("a max above the budget is not a limit: %+v", s)
	}
}
