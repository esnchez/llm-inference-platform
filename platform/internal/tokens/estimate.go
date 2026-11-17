// Package tokens estimates token counts without a model tokenizer.
package tokens

import "unicode/utf8"

// charsPerToken is the usual rule of thumb for BPE tokenizers on English text.
const charsPerToken = 4

// Estimate returns an approximate token count for text (about 4 characters
// per token, at least 1 for non-empty text). It is only an admission-control
// heuristic: real counts depend on each model's tokenizer (README §9, FI-3).
func Estimate(text string) int {
	n := utf8.RuneCountInString(text)
	if n == 0 {
		return 0
	}
	return (n + charsPerToken - 1) / charsPerToken
}
