// Package prefix finds the prompt prefixes that requests share, so the
// control plane can send a request to the worker that already holds that
// prefix in its KV cache (Phase 3, docs/servicio/09-fase-3-decisiones.md).
//
// It has two parts:
//
//   - Hashes cuts a prompt into fixed-size blocks and gives each full block a
//     chained hash: the hash of block i identifies the whole prefix up to and
//     including block i, like vLLM's automatic prefix caching.
//   - Tree is a radix tree over those hash sequences that remembers which
//     workers hold which prefixes, bounded per worker with LRU eviction.
package prefix

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"unicode/utf8"

	"llm-inference-platform/internal/tokens"
)

// BlockTokens is the size of a prefix block, in (estimated) tokens. It
// matches vLLM's default KV-cache block size: vLLM only reuses full blocks,
// so a shared prefix shorter than a block brings no benefit.
const BlockTokens = 16

// blockRunes is the size of a block in characters. Without a tokenizer
// (README §9, FI-3) blocks are cut by characters with the same ratio that
// tokens.Estimate uses, so that "N blocks" means about N×BlockTokens tokens.
const blockRunes = BlockTokens * tokens.CharsPerToken

// Hashes returns one chained hash per full block of prompt. The chain is
// seeded with the model, because the KV cache of one model is useless to
// another. Two prompts share their first k hashes exactly when they share
// their first k blocks of text (barring 64-bit collisions). The trailing
// partial block is not hashed. It returns nil when the prompt is shorter
// than one block.
func Hashes(model, prompt string) []uint64 {
	n := utf8.RuneCountInString(prompt) / blockRunes
	if n == 0 {
		return nil
	}
	out := make([]uint64, 0, n)
	parent := seed(model)
	var buf [8]byte
	h := fnv.New64a()
	start, runes := 0, 0
	for i := range prompt { // i is the byte offset of each rune
		if runes == blockRunes {
			parent = chain(h, &buf, parent, prompt[start:i])
			out = append(out, parent)
			start, runes = i, 0
		}
		runes++
	}
	if runes == blockRunes { // the prompt ends exactly on a block boundary
		out = append(out, chain(h, &buf, parent, prompt[start:]))
	}
	return out
}

func seed(model string) uint64 {
	h := fnv.New64a()
	h.Write([]byte("prefix/v1\x00"))
	h.Write([]byte(model))
	return h.Sum64()
}

// chain returns hash(parent || block).
func chain(h hash.Hash64, buf *[8]byte, parent uint64, block string) uint64 {
	h.Reset()
	binary.LittleEndian.PutUint64(buf[:], parent)
	h.Write(buf[:])
	h.Write([]byte(block))
	return h.Sum64()
}
