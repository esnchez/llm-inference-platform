package simworker

import (
	"context"
	"testing"
	"time"

	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/prefix"
)

func TestPrefixCacheMatchesLeadingBlocksOnly(t *testing.T) {
	c := newPrefixCache(100)
	if got := c.use([]uint64{1, 2, 3}); got != 0 {
		t.Fatalf("cold cache matched %d", got)
	}
	cases := []struct {
		q    []uint64
		want int
	}{
		{[]uint64{1, 2, 3, 4}, 3},
		{[]uint64{1, 9, 3}, 1},  // block 3 is cached but useless after a miss
		{[]uint64{99, 1, 2}, 0}, // a different first block: nothing reusable
		{nil, 0},
	}
	for _, tc := range cases {
		if got := c.use(tc.q); got != tc.want {
			t.Errorf("use(%v) = %d, want %d", tc.q, got, tc.want)
		}
	}
	if _, _, queried, hit := c.stats(); queried != 13 || hit != 4 {
		t.Fatalf("queried %d hit %d, want 13 and 4", queried, hit)
	}
}

func TestPrefixCacheEvictsTailsBeforeSharedHeads(t *testing.T) {
	c := newPrefixCache(4)
	c.use([]uint64{1, 2, 3}) // a shared head {1} with a tail
	c.use([]uint64{1, 4})    // refreshes {1}; 4 blocks cached
	c.use([]uint64{5})       // over capacity: evict the LRU block
	// Blocks are touched last to first, so the oldest is the tail 3, and the
	// shared head 1 (touched last by every request) survives.
	if got := c.use([]uint64{1, 2, 3}); got != 2 {
		t.Fatalf("matched %d, want 2 (only the tail 3 evicted)", got)
	}
	if _, used, _, _ := c.stats(); used != 4 {
		t.Fatalf("used = %d, want capacity 4", used)
	}
}

func TestPrefixCacheShortensTTFT(t *testing.T) {
	const prefill = 200 * time.Microsecond
	w := newWorker(t, func(c *Config) {
		c.TokenBudget = 10_000
		c.PrefillPerToken = prefill
		c.PrefixCacheBlocks = 100
	})
	hashes := make([]uint64, 50) // 50 blocks = 800 tokens
	for i := range hashes {
		hashes[i] = uint64(i + 1)
	}
	r := inference.InferenceRequest{ID: "r", Model: model, PromptTokens: 50 * prefix.BlockTokens, MaxNewTokens: 1, PrefixHashes: hashes}

	ttft := func() time.Duration {
		t.Helper()
		l, err := w.AcquireLease(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		drain(t, l)
		return time.Since(start)
	}
	cold := ttft() // 800 tokens × 200µs = 160ms
	warm := ttft() // fully cached: no prefill
	if cold < 150*time.Millisecond || warm > cold/2 {
		t.Fatalf("cold %v, warm %v: the cached prefix must skip its prefill", cold, warm)
	}
	snap := w.Snapshot()
	if snap.PrefixCacheBlocks != 100 || snap.PrefixCacheUsed != 50 || snap.PrefixBlocksQueried != 100 || snap.PrefixBlocksHit != 50 {
		t.Fatalf("snapshot = %+v", snap)
	}
}

func TestNoPrefixCacheByDefault(t *testing.T) {
	w := newWorker(t, nil)
	l, err := w.AcquireLease(context.Background(), inference.InferenceRequest{ID: "r", Model: model, PromptTokens: 32, MaxNewTokens: 1, PrefixHashes: []uint64{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, l)
	if snap := w.Snapshot(); snap.PrefixCacheBlocks != 0 || snap.PrefixBlocksQueried != 0 {
		t.Fatalf("snapshot = %+v", snap)
	}
}
