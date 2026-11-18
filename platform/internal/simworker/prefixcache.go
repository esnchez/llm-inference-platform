package simworker

import (
	"container/list"
	"sync"
)

// prefixCache simulates vLLM's automatic prefix caching: a bounded set of
// KV-cache blocks identified by their chained hash (inference.InferenceRequest
// .PrefixHashes). A request whose first k blocks are cached skips their
// prefill.
//
// Like vLLM, it only reuses a run of blocks from the start of the prompt
// (block i is useless without blocks 0..i-1) and evicts the least recently
// used block. A request touches its blocks from last to first, so the first
// blocks of a prompt, the ones other requests share, are evicted last. That
// mirrors vLLM, which frees a request's blocks in reverse order.
//
// Unlike vLLM, the capacity is separate from the token budget: blocks in use
// by running requests are not pinned, and cached blocks do not take capacity
// from new leases. See docs/servicio/04-worker-simulado.md.
type prefixCache struct {
	capacity int

	mu      sync.Mutex
	lru     *list.List // of uint64, front = most recently used
	byHash  map[uint64]*list.Element
	queried uint64
	hit     uint64
}

func newPrefixCache(capacity int) *prefixCache {
	return &prefixCache{capacity: capacity, lru: list.New(), byHash: map[uint64]*list.Element{}}
}

// use returns how many leading blocks of hashes were already cached, then
// caches all of them as most recently used.
func (c *prefixCache) use(hashes []uint64) int {
	if c == nil || len(hashes) == 0 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	matched := 0
	for matched < len(hashes) {
		if _, ok := c.byHash[hashes[matched]]; !ok {
			break
		}
		matched++
	}
	c.queried += uint64(len(hashes))
	c.hit += uint64(matched)
	for i := len(hashes) - 1; i >= 0; i-- {
		h := hashes[i]
		if e, ok := c.byHash[h]; ok {
			c.lru.MoveToFront(e)
			continue
		}
		c.byHash[h] = c.lru.PushFront(h)
		if c.lru.Len() > c.capacity {
			old := c.lru.Back()
			c.lru.Remove(old)
			delete(c.byHash, old.Value.(uint64))
		}
	}
	return matched
}

func (c *prefixCache) stats() (capacity, used int, queried, hit uint64) {
	if c == nil {
		return 0, 0, 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capacity, c.lru.Len(), c.queried, c.hit
}
