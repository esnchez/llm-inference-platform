package prefix

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// text returns n characters made of the letter c, so tests can build
// prompts block by block.
func text(c string, n int) string { return strings.Repeat(c, n) }

func TestHashesCutsFullBlocksOnly(t *testing.T) {
	cases := []struct {
		runes, want int
	}{
		{0, 0}, {blockRunes - 1, 0}, {blockRunes, 1}, {blockRunes + 1, 1}, {3*blockRunes - 1, 2}, {3 * blockRunes, 3},
	}
	for _, c := range cases {
		if got := len(Hashes("m", text("a", c.runes))); got != c.want {
			t.Errorf("%d runes: %d blocks, want %d", c.runes, got, c.want)
		}
	}
}

func TestHashesCountRunesNotBytes(t *testing.T) {
	// "ñ" is 2 bytes in UTF-8; blocks are cut by characters, like
	// tokens.Estimate counts them.
	if got := len(Hashes("m", text("ñ", 2*blockRunes))); got != 2 {
		t.Fatalf("got %d blocks, want 2", got)
	}
}

func TestHashesAreChained(t *testing.T) {
	system := text("s", 3*blockRunes)
	a := Hashes("m", system+text("a", 2*blockRunes))
	b := Hashes("m", system+text("b", 2*blockRunes))
	if !reflect.DeepEqual(a[:3], b[:3]) {
		t.Fatal("prompts sharing 3 blocks must share 3 hashes")
	}
	if a[3] == b[3] || a[4] == b[4] {
		t.Fatal("hashes after the divergence must differ")
	}
	// Same block text at a different position: different hash, because the
	// hash covers the whole prefix.
	c := Hashes("m", text("x", blockRunes)+text("s", blockRunes))
	if c[1] == a[0] {
		t.Fatal("a block's hash must depend on the blocks before it")
	}
	if !reflect.DeepEqual(a, Hashes("m", system+text("a", 2*blockRunes))) {
		t.Fatal("hashes must be deterministic")
	}
	if Hashes("other", system)[0] == a[0] {
		t.Fatal("hashes must depend on the model")
	}
}

func mustCheck(t *testing.T, tr *Tree) {
	t.Helper()
	if p := tr.check(); len(p) > 0 {
		t.Fatalf("tree invariants broken: %v", p)
	}
}

func TestTreeMatchAndSplit(t *testing.T) {
	tr := NewTree()
	tr.Insert("w1", []uint64{1, 2, 3, 4}, 0)
	tr.Insert("w2", []uint64{1, 2, 9}, 0) // splits the edge {1,2,3,4} after 2
	mustCheck(t, tr)

	cases := []struct {
		q    []uint64
		want map[string]int
	}{
		{[]uint64{1, 2, 3, 4, 5}, map[string]int{"w1": 4, "w2": 2}},
		{[]uint64{1, 2, 3}, map[string]int{"w1": 3, "w2": 2}}, // partial edge
		{[]uint64{1, 2, 9, 9}, map[string]int{"w1": 2, "w2": 3}},
		{[]uint64{1}, map[string]int{"w1": 1, "w2": 1}},
		{[]uint64{7}, map[string]int{}},
		{nil, map[string]int{}},
	}
	for _, c := range cases {
		if got := tr.Match(c.q); !reflect.DeepEqual(got, c.want) {
			t.Errorf("Match(%v) = %v, want %v", c.q, got, c.want)
		}
	}
	if st := tr.Stats(); st.Nodes != 3 || st.BlocksPerWorker["w1"] != 4 || st.BlocksPerWorker["w2"] != 3 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestTreeInsertIsIdempotent(t *testing.T) {
	tr := NewTree()
	for range 3 {
		tr.Insert("w1", []uint64{1, 2, 3}, 0)
	}
	tr.Insert("w1", []uint64{1, 2}, 0)
	mustCheck(t, tr)
	if st := tr.Stats(); st.BlocksPerWorker["w1"] != 3 {
		t.Fatalf("blocks = %d, want 3", st.BlocksPerWorker["w1"])
	}
}

func TestTreeEvictsLeastRecentlyUsedDeepestFirst(t *testing.T) {
	tr := NewTree()
	const capacity = 8                              // evicts down to 7
	tr.Insert("w1", []uint64{1, 2, 3}, capacity)    // oldest
	tr.Insert("w1", []uint64{10, 11, 12}, capacity) // middle
	tr.Insert("w1", []uint64{1, 2, 4}, capacity)    // refreshes {1,2}; 7 blocks
	tr.Insert("w2", []uint64{10, 11, 12}, capacity) // another worker: unaffected
	mustCheck(t, tr)
	if b := tr.Stats().BlocksPerWorker["w1"]; b != 7 {
		t.Fatalf("w1 holds %d blocks before eviction, want 7", b)
	}

	tr.Insert("w1", []uint64{20, 21}, capacity) // 9 > 8: evict down to <= 7
	mustCheck(t, tr)
	// The LRU leaves are {3} (tick 1) then {10,11,12} (tick 2). Eviction
	// works on whole nodes (compressed edges), so it removes 4 blocks and
	// ends at 5. The shared {1,2} was refreshed, so it stays.
	got := tr.Match([]uint64{1, 2, 3})
	if got["w1"] != 2 {
		t.Fatalf("w1 match on {1,2,3} = %d, want 2 (block 3 evicted)", got["w1"])
	}
	if got := tr.Match([]uint64{10, 11, 12}); !reflect.DeepEqual(got, map[string]int{"w2": 3}) {
		t.Fatalf("match on {10,11,12} = %v, want only w2:3", got)
	}
	if got := tr.Match([]uint64{20, 21}); got["w1"] != 2 {
		t.Fatalf("the newest prefix must survive, got %v", got)
	}
	if st := tr.Stats(); st.BlocksPerWorker["w1"] != 5 || st.EvictedBlocks != 4 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestTreeRemoveWorker(t *testing.T) {
	tr := NewTree()
	tr.Insert("w1", []uint64{1, 2, 3}, 0)
	tr.Insert("w2", []uint64{1, 2}, 0)
	tr.RemoveWorker("w1")
	tr.RemoveWorker("unknown")
	mustCheck(t, tr)
	if got := tr.Match([]uint64{1, 2, 3}); !reflect.DeepEqual(got, map[string]int{"w2": 2}) {
		t.Fatalf("match = %v", got)
	}
	if st := tr.Stats(); st.Nodes != 1 {
		t.Fatalf("nodes = %d, want 1 (the {3} node must be gone)", st.Nodes)
	}
}

// TestTreeRandomized compares the tree with a trivially correct model on a
// random workload with shared prefixes, and checks the invariants and the
// per-worker capacity bound after every operation.
func TestTreeRandomized(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	tr := NewTree()
	const capacity = 40
	workers := []string{"w1", "w2", "w3"}
	randomSeq := func() []uint64 {
		// A few shared "system prompts" plus a random tail.
		seq := []uint64{uint64(rng.IntN(3)) + 1}
		for range rng.IntN(4) {
			seq = append(seq, uint64(rng.IntN(3))+10)
		}
		for range rng.IntN(6) {
			seq = append(seq, uint64(rng.IntN(50))+100)
		}
		return seq
	}
	for i := range 3000 {
		w := workers[rng.IntN(len(workers))]
		seq := randomSeq()
		tr.Insert(w, seq, capacity)
		if p := tr.check(); len(p) > 0 {
			t.Fatalf("op %d: %v", i, p)
		}
		for w, b := range tr.Stats().BlocksPerWorker {
			if b > capacity {
				t.Fatalf("op %d: %s holds %d blocks > capacity %d", i, w, b, capacity)
			}
		}
		// Just inserted: the worker holds the whole sequence unless the
		// sequence alone exceeds what eviction leaves.
		if got := tr.Match(seq)[w]; got != len(seq) {
			t.Fatalf("op %d: %s match = %d right after inserting %d blocks", i, w, got, len(seq))
		}
	}
}

func TestTreeConcurrentUse(t *testing.T) {
	tr := NewTree()
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := fmt.Sprint("w", g%3)
			for i := range 500 {
				seq := []uint64{1, uint64(i % 7), uint64(i)}
				tr.Insert(w, seq, 30)
				tr.Match(seq)
				tr.Stats()
			}
		}()
	}
	wg.Wait()
	mustCheck(t, tr)
}
