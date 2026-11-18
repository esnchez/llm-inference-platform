package prefix

import (
	"container/heap"
	"maps"
	"slices"
	"sync"
)

// Tree is a radix tree over chained block hashes (see Hashes) that records
// which workers hold which prompt prefixes in their KV cache. It is the
// router's approximation of the workers' caches: the router cannot see what
// a worker evicts, so each worker gets a capacity in blocks and the tree
// evicts that worker's least recently used prefixes once it goes over
// (README §9, FI-6).
//
// Edges are compressed: a node holds a run of block hashes, so a long system
// prompt shared by many requests costs one node, not one per block.
//
// Invariant: a worker marked on a node is also marked on every ancestor (to
// hold a prefix is to hold every shorter prefix of it). Insert marks the
// whole path and eviction removes a worker from the deepest nodes first, so
// the invariant always holds and Match can stop at the deepest mark.
//
// Recency is a logical clock (one tick per Insert), so the tree does not
// depend on wall time. Tree is safe for concurrent use.
type Tree struct {
	mu      sync.Mutex
	root    *node
	clock   uint64
	nodes   int
	blocks  map[string]int // blocks held per worker
	evicted uint64         // blocks evicted, all workers
}

type node struct {
	parent   *node
	key      []uint64          // the run of block hashes on the edge into this node
	children map[uint64]*node  // by the first hash of each child's key
	workers  map[string]uint64 // worker ID → last tick it was inserted through here
}

// NewTree returns an empty tree.
func NewTree() *Tree {
	return &Tree{root: newNode(nil, nil), blocks: map[string]int{}}
}

func newNode(parent *node, key []uint64) *node {
	return &node{parent: parent, key: key, children: map[uint64]*node{}, workers: map[string]uint64{}}
}

// Match returns, for every worker holding at least the first block of
// hashes, how many leading blocks of hashes it holds. It does not change
// recency: only an actual placement (Insert) counts as a use.
func (t *Tree) Match(hashes []uint64) map[string]int {
	t.mu.Lock()
	defer t.mu.Unlock()
	matched := map[string]int{}
	n, i := t.root, 0
	for i < len(hashes) {
		child := n.children[hashes[i]]
		if child == nil {
			break
		}
		c := commonPrefix(child.key, hashes[i:])
		// Every worker on child holds child's whole key, so it holds the
		// c blocks matched inside it too. Deeper nodes overwrite.
		for w := range child.workers {
			matched[w] = i + c
		}
		if c < len(child.key) {
			break
		}
		i += c
		n = child
	}
	return matched
}

// Insert records that worker now holds every prefix of hashes and marks
// them as its most recently used. If the worker then holds more than
// capacity blocks, its least recently used prefixes are evicted, deepest
// first, down to 7/8 of capacity (the slack makes eviction amortized rather
// than per insert). A capacity <= 0 means unbounded.
func (t *Tree) Insert(worker string, hashes []uint64, capacity int) {
	if len(hashes) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.clock++
	n, i := t.root, 0
	for i < len(hashes) {
		child := n.children[hashes[i]]
		if child == nil {
			child = newNode(n, append([]uint64(nil), hashes[i:]...))
			n.children[hashes[i]] = child
			t.nodes++
		} else if c := commonPrefix(child.key, hashes[i:]); c < len(child.key) {
			child = t.split(child, c)
		}
		if _, ok := child.workers[worker]; !ok {
			t.blocks[worker] += len(child.key)
		}
		child.workers[worker] = t.clock
		i += len(child.key)
		n = child
	}
	if capacity > 0 && t.blocks[worker] > capacity {
		t.evict(worker, capacity-capacity/8)
	}
}

// split cuts child's edge after c blocks and returns the new upper node.
// Workers on child hold the whole edge, so they are copied to the upper node
// with the same recency; block counts do not change.
func (t *Tree) split(child *node, c int) *node {
	parent := child.parent
	upper := newNode(parent, child.key[:c:c])
	maps.Copy(upper.workers, child.workers)
	child.key = child.key[c:]
	child.parent = upper
	upper.children[child.key[0]] = child
	parent.children[upper.key[0]] = upper
	t.nodes++
	return upper
}

// evict removes worker from its least recently used deepest nodes until it
// holds at most target blocks.
func (t *Tree) evict(worker string, target int) {
	// The worker's "leaves": nodes where it is marked and no child is.
	// Removing one may turn its parent into a leaf.
	var h leafHeap
	var walk func(*node)
	walk = func(n *node) {
		leaf := true
		for _, c := range n.children {
			if _, ok := c.workers[worker]; ok {
				leaf = false
				walk(c)
			}
		}
		if leaf && n != t.root {
			h = append(h, leafItem{n, n.workers[worker]})
		}
	}
	walk(t.root)
	heap.Init(&h)
	for t.blocks[worker] > target && h.Len() > 0 {
		n := heap.Pop(&h).(leafItem).n
		delete(n.workers, worker)
		t.blocks[worker] -= len(n.key)
		t.evicted += uint64(len(n.key))
		parent := n.parent
		if len(n.workers) == 0 {
			// No worker left here, so (invariant) none below either.
			t.removeSubtree(n)
		}
		if parent != t.root && t.isLeafFor(parent, worker) {
			heap.Push(&h, leafItem{parent, parent.workers[worker]})
		}
	}
	if t.blocks[worker] == 0 {
		delete(t.blocks, worker)
	}
}

func (t *Tree) isLeafFor(n *node, worker string) bool {
	if _, ok := n.workers[worker]; !ok {
		return false
	}
	for _, c := range n.children {
		if _, ok := c.workers[worker]; ok {
			return false
		}
	}
	return true
}

func (t *Tree) removeSubtree(n *node) {
	delete(n.parent.children, n.key[0])
	var count func(*node) int
	count = func(n *node) int {
		k := 1
		for _, c := range n.children {
			k += count(c)
		}
		return k
	}
	t.nodes -= count(n)
}

// RemoveWorker forgets everything recorded for worker (for example when it
// leaves the pool).
func (t *Tree) RemoveWorker(worker string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.blocks[worker]; ok {
		t.evict(worker, 0)
	}
}

// TreeStats is a point-in-time view of the tree's size.
type TreeStats struct {
	Nodes           int            `json:"nodes"`
	BlocksPerWorker map[string]int `json:"blocks_per_worker"`
	EvictedBlocks   uint64         `json:"evicted_blocks"`
}

// Stats returns the tree's size.
func (t *Tree) Stats() TreeStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	per := make(map[string]int, len(t.blocks))
	maps.Copy(per, t.blocks)
	return TreeStats{Nodes: t.nodes, BlocksPerWorker: per, EvictedBlocks: t.evicted}
}

// check verifies the tree's invariants; tests call it.
func (t *Tree) check() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var problems []string
	blocks := map[string]int{}
	nodes := 0
	var walk func(*node)
	walk = func(n *node) {
		keys := make([]uint64, 0, len(n.children))
		for k := range n.children {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			c := n.children[k]
			nodes++
			switch {
			case len(c.key) == 0:
				problems = append(problems, "node with empty key")
			case c.key[0] != k:
				problems = append(problems, "child indexed under the wrong hash")
			case c.parent != n:
				problems = append(problems, "wrong parent pointer")
			case len(c.workers) == 0:
				problems = append(problems, "node without workers")
			}
			for w := range c.workers {
				blocks[w] += len(c.key)
				if _, ok := n.workers[w]; !ok && n != t.root {
					problems = append(problems, "worker on a node but not on its parent: "+w)
				}
			}
			walk(c)
		}
	}
	walk(t.root)
	if nodes != t.nodes {
		problems = append(problems, "node count out of sync")
	}
	if len(blocks) != len(t.blocks) {
		problems = append(problems, "per-worker block map out of sync")
	}
	for w, b := range blocks {
		if t.blocks[w] != b {
			problems = append(problems, "block count out of sync for "+w)
		}
	}
	return problems
}

func commonPrefix(a, b []uint64) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

type leafItem struct {
	n    *node
	tick uint64
}

type leafHeap []leafItem

func (h leafHeap) Len() int           { return len(h) }
func (h leafHeap) Less(i, j int) bool { return h[i].tick < h[j].tick }
func (h leafHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *leafHeap) Push(x any)        { *h = append(*h, x.(leafItem)) }
func (h *leafHeap) Pop() any {
	old := *h
	it := old[len(old)-1]
	*h = old[:len(old)-1]
	return it
}
