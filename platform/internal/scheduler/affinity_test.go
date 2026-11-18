package scheduler

import (
	"context"
	"errors"
	"testing"

	"llm-inference-platform/internal/inference"
)

var tailSeq uint64

// prefixed returns a request of the given cost whose prompt starts with
// sysBlocks blocks of "system prompt" sys, followed by a private tail block.
func prefixed(cost int, sys uint64, sysBlocks int) inference.InferenceRequest {
	r := req("m", cost)
	for k := range sysBlocks {
		r.PrefixHashes = append(r.PrefixHashes, sys<<32|uint64(k))
	}
	tailSeq++
	r.PrefixHashes = append(r.PrefixHashes, 1<<63|tailSeq)
	return r
}

func place(t *testing.T, p Placement, r inference.InferenceRequest, workers ...inference.Worker) inference.Lease {
	t.Helper()
	l, err := p.Place(context.Background(), r, workers)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	t.Cleanup(l.Release)
	return l
}

func TestAffinityKeepsAPrefixOnOneWorker(t *testing.T) {
	ws := []inference.Worker{worker(t, "a", 1000, "m"), worker(t, "b", 1000, "m"), worker(t, "c", 1000, "m")}
	p := NewPrefixAffinity(AffinityConfig{})
	first := place(t, p, prefixed(10, 1, 8), ws...).WorkerID()
	for range 5 {
		if got := place(t, p, prefixed(10, 1, 8), ws...).WorkerID(); got != first {
			t.Fatalf("same prefix went to %s, want %s", got, first)
		}
	}
	st := p.Stats()
	if st.Placements != 6 || st.AffinityHits != 5 || st.AffinityOverflows != 0 {
		t.Fatalf("stats = %+v", st)
	}
	// 5 requests × 8 of 9 blocks predicted cached.
	if st.QueriedBlocks != 54 || st.PredictedHitBlocks != 40 {
		t.Fatalf("blocks: queried %d predicted %d", st.QueriedBlocks, st.PredictedHitBlocks)
	}
}

func TestAffinitySpreadsDifferentPrefixesByLoad(t *testing.T) {
	ws := []inference.Worker{worker(t, "a", 1000, "m"), worker(t, "b", 1000, "m"), worker(t, "c", 1000, "m")}
	p := NewPrefixAffinity(AffinityConfig{})
	seen := map[string]bool{}
	for sys := range uint64(3) {
		seen[place(t, p, prefixed(300, sys+1, 8), ws...).WorkerID()] = true
	}
	if len(seen) != 3 {
		t.Fatalf("3 new prefixes on 3 idle workers used %d workers", len(seen))
	}
}

func TestAffinityPrefersTheLongestMatch(t *testing.T) {
	a, b := worker(t, "a", 1000, "m"), worker(t, "b", 1000, "m")
	p := NewPrefixAffinity(AffinityConfig{})
	full := prefixed(10, 7, 10)
	short := inference.InferenceRequest{ID: "short", Model: "m", PromptTokens: 9, MaxNewTokens: 1, PrefixHashes: full.PrefixHashes[:5]}
	place(t, p, short, a)                     // a holds 5 blocks
	place(t, p, prefixed(10, 7, 10), b)       // b holds the 10 shared blocks
	place(t, p, prefixed(500, 9, 4), b)       // and is busier than a
	got := place(t, p, full, a, b).WorkerID() // a: 5 blocks, b: 10
	if got != "b" {
		t.Fatalf("placed on %s, want b (longest match)", got)
	}
}

func TestAffinityIgnoresShortMatches(t *testing.T) {
	a, b := worker(t, "a", 1000, "m"), worker(t, "b", 1000, "m")
	p := NewPrefixAffinity(AffinityConfig{MinMatchBlocks: 4})
	place(t, p, prefixed(400, 3, 3), a) // a holds 3 blocks of sys 3, and is loaded
	if got := place(t, p, prefixed(10, 3, 3), a, b).WorkerID(); got != "b" {
		t.Fatalf("a 3-block match must not beat load; placed on %s", got)
	}
}

func TestAffinityOverflowsWhenTheWorkerIsLoaded(t *testing.T) {
	a, b := worker(t, "a", 1000, "m"), worker(t, "b", 1000, "m")
	p := NewPrefixAffinity(AffinityConfig{MaxLoad: 0.5})
	place(t, p, prefixed(450, 1, 8), a) // a: prefix cached, load 0.45
	// 0.45 + 0.1 = 0.55 > MaxLoad: affinity no longer applies.
	if got := place(t, p, prefixed(100, 1, 8), a, b).WorkerID(); got != "b" {
		t.Fatalf("overloaded affine worker still chosen: %s", got)
	}
	if st := p.Stats(); st.AffinityOverflows != 1 {
		t.Fatalf("overflows = %d, want 1", st.AffinityOverflows)
	}
	// The overflow replicated the prefix on b: the next one sticks to b.
	if got := place(t, p, prefixed(100, 1, 8), a, b).WorkerID(); got != "b" {
		t.Fatalf("placed on %s, want b (now holds the prefix and has room)", got)
	}
}

func TestAffinityFallsBackWhenTheAffineWorkerIsFull(t *testing.T) {
	a, b := worker(t, "a", 1000, "m"), worker(t, "b", 1000, "m")
	p := NewPrefixAffinity(AffinityConfig{MaxLoad: 1})
	place(t, p, prefixed(500, 1, 8), a)
	// a holds the prefix but the request no longer fits there (load 1.1):
	// it goes to b, and counts as an overflow.
	if got := place(t, p, prefixed(600, 1, 8), a, b).WorkerID(); got != "b" {
		t.Fatalf("placed on %s, want b", got)
	}
	if st := p.Stats(); st.AffinityOverflows != 1 {
		t.Fatalf("overflows = %d, want 1", st.AffinityOverflows)
	}
}

func TestAffinityWithoutPrefixUsesLoad(t *testing.T) {
	a, b := worker(t, "a", 1000, "m"), worker(t, "b", 1000, "m")
	p := NewPrefixAffinity(AffinityConfig{})
	place(t, p, req("m", 500), a)
	if got := place(t, p, req("m", 10), a, b).WorkerID(); got != "b" {
		t.Fatalf("placed on %s, want the least loaded (b)", got)
	}
	if st := p.Stats(); st.WithPrefix != 0 || st.Tree.Nodes != 0 {
		t.Fatalf("requests without hashes must not touch the tree: %+v", st)
	}
}

func TestAffinityErrors(t *testing.T) {
	p := NewPrefixAffinity(AffinityConfig{})
	if _, err := p.Place(context.Background(), req("m", 10), nil); !errors.Is(err, inference.ErrAtCapacity) {
		t.Fatalf("no workers: %v", err)
	}
	a := worker(t, "a", 10, "m")
	place(t, p, req("m", 10), a)
	if _, err := p.Place(context.Background(), prefixed(5, 1, 2), []inference.Worker{a}); !errors.Is(err, inference.ErrAtCapacity) {
		t.Fatalf("full pool: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Place(ctx, req("m", 1), []inference.Worker{worker(t, "b", 10, "m")}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
	if st := p.Stats(); st.Placements != 1 || st.Tree.Nodes != 0 {
		t.Fatalf("failed placements must not be recorded: %+v", st)
	}
}

// TestSchedulerWithAffinity checks the integration: a queued request is
// placed by affinity once capacity frees, and the counters reach
// /debug/scheduler.
func TestSchedulerWithAffinity(t *testing.T) {
	// Budget 1000: the tree's default capacity estimate (1000/16 = 62
	// blocks) holds these 9-block prompts.
	a, b := worker(t, "a", 1000, "m"), worker(t, "b", 1000, "m")
	p := NewPrefixAffinity(AffinityConfig{MaxLoad: 1})
	s := newScheduler(t, func(c *Config) { c.Placement = p }, a, b)

	la, err := s.Acquire(context.Background(), prefixed(1000, 1, 8))
	if err != nil {
		t.Fatal(err)
	}
	lb, err := s.Acquire(context.Background(), prefixed(1000, 2, 8))
	if err != nil {
		t.Fatal(err)
	}
	if la.WorkerID() == lb.WorkerID() {
		t.Fatal("both on the same worker")
	}
	queued := acquireAsync(t, context.Background(), s, prefixed(600, 2, 8), 1)
	la.Release() // frees la's worker, but the request's prefix is on lb's
	r := get(t, queued)
	if r.err != nil {
		t.Fatal(r.err)
	}
	r.lease.Release()
	if r.lease.WorkerID() != la.WorkerID() {
		t.Fatalf("queued request placed on %s, want the freed %s", r.lease.WorkerID(), la.WorkerID())
	}
	lb.Release()
	// That overflow replicated prefix 2 on la's worker, which still is the
	// only one holding prefix 1: with both idle, prefix 1 goes back there.
	l, err := s.Acquire(context.Background(), prefixed(600, 1, 8))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()
	if l.WorkerID() != la.WorkerID() {
		t.Fatalf("prefix 1 placed on %s, want %s", l.WorkerID(), la.WorkerID())
	}
	st, ok := s.Stats().Placement.(AffinityStats)
	if !ok {
		t.Fatalf("placement stats missing: %#v", s.Stats().Placement)
	}
	if st.Placements != 4 || st.AffinityHits != 1 || st.AffinityOverflows != 1 {
		t.Fatalf("stats = %+v", st)
	}
}
