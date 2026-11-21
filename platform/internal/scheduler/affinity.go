package scheduler

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/prefix"
)

// AffinityConfig tunes PrefixAffinity. Zero fields take the
// DefaultAffinityConfig value.
type AffinityConfig struct {
	// MaxLoad is the ledger load, (reserved + request cost) / budget, above
	// which a worker no longer gets requests for affinity alone. Past it the
	// request goes to the least loaded worker, which then caches the prefix
	// too: hot prefixes spread to more workers instead of saturating one
	// (README §9, FI-6).
	MaxLoad float64
	// MinMatchBlocks is the shortest cached prefix, in blocks of
	// prefix.BlockTokens tokens, that counts as affinity. Shorter matches
	// save too little prefill to be worth unbalancing the pool.
	MinMatchBlocks int
	// CacheBlocks is the router's estimate of a worker's prefix-cache size,
	// in blocks. It bounds the worker's entries in the tree (LRU). Default:
	// the worker's token budget / prefix.BlockTokens, i.e. the whole KV cache,
	// which is what vLLM can keep cached when idle.
	CacheBlocks func(inference.WorkerSnapshot) int
}

// DefaultAffinityConfig returns the Phase 3 defaults.
func DefaultAffinityConfig() AffinityConfig {
	return AffinityConfig{
		MaxLoad:        0.8,
		MinMatchBlocks: 4,
		CacheBlocks: func(s inference.WorkerSnapshot) int {
			return s.TokenBudget / prefix.BlockTokens
		},
	}
}

func (c AffinityConfig) withDefaults() AffinityConfig {
	d := DefaultAffinityConfig()
	if c.MaxLoad <= 0 {
		c.MaxLoad = d.MaxLoad
	}
	if c.MinMatchBlocks <= 0 {
		c.MinMatchBlocks = d.MinMatchBlocks
	}
	if c.CacheBlocks == nil {
		c.CacheBlocks = d.CacheBlocks
	}
	return c
}

// PrefixAffinity is the Phase 3 Placement: it sends a request to the worker
// that most likely holds the longest prefix of its prompt in the KV cache,
// as long as that worker is not overloaded
// (docs/servicio/10-fase-3-decisiones.md).
//
// Order in which workers are tried (first lease granted wins):
//
//  1. Affine workers: cached match >= MinMatchBlocks and load <= MaxLoad,
//     longest match first, then least loaded.
//  2. Everyone else, least loaded first (ties rotate, like RoundRobin).
//
// After a lease is granted, the request's prefix is recorded in the tree as
// cached on that worker. The worker will cache it during the prefill that
// starts right after, so recording it now is only slightly early.
//
// Requests without PrefixHashes (prompts shorter than one block) are placed
// by load alone. PrefixAffinity is safe for concurrent use.
type PrefixAffinity struct {
	cfg  AffinityConfig
	tree *prefix.Tree
	next atomic.Uint64

	mu    sync.Mutex
	stats AffinityStats
}

// NewPrefixAffinity returns a PrefixAffinity with an empty tree.
func NewPrefixAffinity(cfg AffinityConfig) *PrefixAffinity {
	return &PrefixAffinity{cfg: cfg.withDefaults(), tree: prefix.NewTree()}
}

type candidate struct {
	w      inference.Worker
	snap   inference.WorkerSnapshot
	match  int
	load   float64
	affine bool
}

// Place implements Placement.
func (p *PrefixAffinity) Place(ctx context.Context, req inference.InferenceRequest, workers []inference.Worker) (inference.Lease, error) {
	n := len(workers)
	if n == 0 {
		return nil, inference.ErrAtCapacity
	}
	var matches map[string]int
	if len(req.PrefixHashes) > 0 {
		matches = p.tree.Match(req.PrefixHashes)
	}
	cost := req.TokenCost()
	start := int(p.next.Add(1)-1) % n
	cands := make([]candidate, n)
	best := 0
	for k := range n {
		w := workers[(start+k)%n]
		snap := w.Snapshot()
		c := candidate{w: w, snap: snap, match: matches[w.ID()], load: 1}
		if snap.TokenBudget > 0 {
			c.load = float64(snap.TokensReserved+cost) / float64(snap.TokenBudget)
		}
		c.affine = c.match >= p.cfg.MinMatchBlocks && c.load <= p.cfg.MaxLoad
		best = max(best, c.match)
		cands[k] = c
	}
	sort.SliceStable(cands, func(i, j int) bool {
		a, b := cands[i], cands[j]
		if a.affine != b.affine {
			return a.affine
		}
		if a.affine && a.match != b.match {
			return a.match > b.match
		}
		return a.load < b.load
	})
	ordered := make([]inference.Worker, n)
	for i, c := range cands {
		ordered[i] = c.w
	}

	lease, err := acquireFirst(ctx, req, ordered)
	if err != nil {
		return nil, err
	}
	var chosen candidate
	for _, c := range cands {
		if c.w.ID() == lease.WorkerID() {
			chosen = c
			break
		}
	}
	if len(req.PrefixHashes) > 0 {
		p.tree.Insert(lease.WorkerID(), req.PrefixHashes, p.cfg.CacheBlocks(chosen.snap))
	}
	p.record(req, chosen.match, best)
	return lease, nil
}

func (p *PrefixAffinity) record(req inference.InferenceRequest, match, best int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := &p.stats
	st.Placements++
	if len(req.PrefixHashes) == 0 {
		return
	}
	st.WithPrefix++
	st.QueriedBlocks += uint64(len(req.PrefixHashes))
	st.PredictedHitBlocks += uint64(match)
	switch {
	case match >= p.cfg.MinMatchBlocks:
		st.AffinityHits++
		if match < best {
			st.ShorterMatch++
		}
	case best >= p.cfg.MinMatchBlocks:
		st.AffinityOverflows++
	}
}

// AffinityStats counts PrefixAffinity's decisions; they appear under
// "placement" in /debug/scheduler.
type AffinityStats struct {
	// Placements is every lease granted; WithPrefix those whose prompt had
	// at least one full block.
	Placements uint64 `json:"placements"`
	WithPrefix uint64 `json:"with_prefix"`
	// AffinityHits went to a worker holding >= MinMatchBlocks of their
	// prefix; ShorterMatch is the subset where another worker held a longer
	// one but was overloaded or full.
	AffinityHits uint64 `json:"affinity_hits"`
	ShorterMatch uint64 `json:"shorter_match"`
	// AffinityOverflows had an affine worker but went elsewhere without any
	// useful match, because every affine worker was overloaded or full.
	AffinityOverflows uint64 `json:"affinity_overflows"`
	// PredictedHitBlocks / QueriedBlocks is the cache hit rate the router
	// expects. Compare it with the workers' real one (prefix_blocks_hit /
	// prefix_blocks_queried in /debug/workers) to see how stale the tree is.
	QueriedBlocks      uint64           `json:"queried_blocks"`
	PredictedHitBlocks uint64           `json:"predicted_hit_blocks"`
	PredictedHitRate   float64          `json:"predicted_hit_rate"`
	Tree               prefix.TreeStats `json:"tree"`
}

// Stats returns a snapshot of the counters and the tree size.
func (p *PrefixAffinity) Stats() AffinityStats {
	p.mu.Lock()
	st := p.stats
	p.mu.Unlock()
	if st.QueriedBlocks > 0 {
		st.PredictedHitRate = float64(st.PredictedHitBlocks) / float64(st.QueriedBlocks)
	}
	st.Tree = p.tree.Stats()
	return st
}

// DebugStats implements placementStatser.
func (p *PrefixAffinity) DebugStats() any { return p.Stats() }

// RemoveWorker forgets the prefixes recorded for a worker, for example when
// it leaves the pool or restarts with an empty cache.
func (p *PrefixAffinity) RemoveWorker(id string) { p.tree.RemoveWorker(id) }
