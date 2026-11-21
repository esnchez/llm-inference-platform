package tokens

import (
	"strings"
	"testing"
)

func TestEstimate(t *testing.T) {
	cases := []struct {
		text string
		want int
	}{
		{"", 0},
		{"a", 1},     // non-empty text is at least one token
		{"abcd", 1},  // exactly one token's worth
		{"abcde", 2}, // rounds up
		{"hola mundo", 3},
		{strings.Repeat("x", 400), 100},
		{"ñandú", 2},   // counts characters, not UTF-8 bytes (5 runes, 7 bytes)
		{"日本語テキスト", 2}, // 7 runes
	}
	for _, c := range cases {
		if got := Estimate(c.text); got != c.want {
			t.Errorf("Estimate(%q) = %d, want %d", c.text, got, c.want)
		}
	}
}

// TestEstimateIsMonotonic: more text never estimates fewer tokens, which
// admission relies on (a longer prompt must never look cheaper).
func TestEstimateIsMonotonic(t *testing.T) {
	prev := 0
	for n := 0; n < 200; n++ {
		got := Estimate(strings.Repeat("é", n))
		if got < prev {
			t.Fatalf("%d runes: %d tokens < %d for %d runes", n, got, prev, n-1)
		}
		prev = got
	}
}
