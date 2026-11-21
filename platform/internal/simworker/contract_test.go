package simworker

import (
	"testing"

	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/workertest"
)

// TestContract runs the battery every Worker implementation must pass
// (shared with the vLLM adapter).
func TestContract(t *testing.T) {
	workertest.Run(t, func(t *testing.T, o workertest.Options) inference.Worker {
		w, err := New(Config{ID: "sim", Models: []string{o.Model}, TokenBudget: o.TokenBudget,
			InterTokenLatency: o.TokenDelay, LeaseTTL: o.LeaseTTL})
		if err != nil {
			t.Fatal(err)
		}
		return w
	})
}
