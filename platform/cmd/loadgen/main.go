// Command loadgen sends a realistic mix of streaming chat requests to the
// platform and reports how admission behaved: status codes, rejection
// reasons, TTFT and total latency percentiles, and the scheduler's own view
// (/debug/scheduler).
//
// Traffic model: Poisson arrivals at -rate requests per second for
// -duration; a -rag-fraction of long-context requests (long prompt, large
// max_tokens) among short chat requests; and a -cancel-fraction of clients
// that give up after a random delay. It compares queueing policies with the
// simulator; llmperf takes over against real vLLM in Phase 6.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

type outcome struct {
	status    int
	code      string // OpenAI error code on failures (queue_full, queue_timeout, ...)
	cancelled bool
	ttft      time.Duration
	total     time.Duration
	rag       bool
}

func main() {
	var (
		url       = flag.String("url", "http://localhost:8080", "platform base URL")
		model     = flag.String("model", "sim-llama-3-8b", "model to request")
		rate      = flag.Float64("rate", 20, "mean arrival rate, requests per second (Poisson)")
		duration  = flag.Duration("duration", 10*time.Second, "how long to send requests")
		ragFrac   = flag.Float64("rag-fraction", 0.1, "fraction of long-context (RAG-like) requests")
		chatMax   = flag.Int("chat-max-tokens", 256, "max_tokens of chat requests")
		ragMax    = flag.Int("rag-max-tokens", 1024, "max_tokens of RAG requests")
		ragChars  = flag.Int("rag-prompt-chars", 16000, "prompt size of RAG requests, in characters (~4 per token)")
		cancelFr  = flag.Float64("cancel-fraction", 0.05, "fraction of clients that give up early")
		cancelMax = flag.Duration("cancel-max", 2*time.Second, "clients that give up do so after a uniform delay up to this")
		seed      = flag.Uint64("seed", 1, "random seed (same seed, same traffic)")
	)
	flag.Parse()
	if *rate <= 0 || *duration <= 0 {
		fmt.Fprintln(os.Stderr, "loadgen: -rate and -duration must be positive")
		os.Exit(2)
	}

	rng := rand.New(rand.NewPCG(*seed, *seed^0x9e3779b97f4a7c15))
	client := &http.Client{}
	chatPrompt := strings.Repeat("Hola, ¿qué tal? ", 12)
	ragPrompt := strings.Repeat("contexto ", *ragChars/9+1)[:*ragChars]

	var (
		mu       sync.Mutex
		outcomes []outcome
		wg       sync.WaitGroup
	)
	start := time.Now()
	sent := 0
	for next := time.Duration(0); next < *duration; next += time.Duration(rng.ExpFloat64() / *rate * float64(time.Second)) {
		time.Sleep(time.Until(start.Add(next)))
		rag := rng.Float64() < *ragFrac
		prompt, maxTokens := chatPrompt, *chatMax
		if rag {
			prompt, maxTokens = ragPrompt, *ragMax
		}
		var giveUp time.Duration
		if rng.Float64() < *cancelFr {
			giveUp = time.Duration(rng.Int64N(int64(*cancelMax)) + 1)
		}
		sent++
		wg.Add(1)
		go func() {
			defer wg.Done()
			o := send(client, *url, *model, prompt, maxTokens, giveUp)
			o.rag = rag
			mu.Lock()
			outcomes = append(outcomes, o)
			mu.Unlock()
		}()
	}
	sendWindow := time.Since(start)
	wg.Wait()
	report(os.Stdout, outcomes, sent, sendWindow, time.Since(start))
	printSchedulerStats(os.Stdout, client, *url)
}

func send(client *http.Client, url, model, prompt string, maxTokens int, giveUp time.Duration) outcome {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if giveUp > 0 {
		time.AfterFunc(giveUp, cancel)
	}
	body, _ := json.Marshal(map[string]any{
		"model":      model,
		"stream":     true,
		"max_tokens": maxTokens,
		"messages":   []map[string]string{{"role": "user", "content": prompt}},
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v1/chat/completions", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return outcome{cancelled: ctx.Err() != nil, code: "transport", total: time.Since(start)}
	}
	defer resp.Body.Close()
	o := outcome{status: resp.StatusCode}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		o.code, o.total = e.Error.Code, time.Since(start)
		return o
	}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "data: ") && o.ttft == 0 {
			o.ttft = time.Since(start)
		}
	}
	o.total = time.Since(start)
	o.cancelled = ctx.Err() != nil
	if err := sc.Err(); err != nil && !o.cancelled {
		o.code = "stream_error"
	}
	return o
}

func report(w io.Writer, outs []outcome, sent int, sendWindow, elapsed time.Duration) {
	fmt.Fprintf(w, "sent %d requests in %v (%.1f req/s); all finished after %v\n\n",
		sent, sendWindow.Round(time.Millisecond), float64(sent)/sendWindow.Seconds(), elapsed.Round(time.Millisecond))

	counts := map[string]int{}
	var ttfts, totals, ragTTFTs []time.Duration
	for _, o := range outs {
		key := fmt.Sprint(o.status)
		switch {
		case o.cancelled:
			key = "cancelled by client"
		case o.status == 0:
			key = "transport error"
		case o.code != "":
			key += " " + o.code
		}
		counts[key]++
		if o.status == http.StatusOK && !o.cancelled {
			ttfts = append(ttfts, o.ttft)
			totals = append(totals, o.total)
			if o.rag {
				ragTTFTs = append(ragTTFTs, o.ttft)
			}
		}
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Fprintln(w, "outcomes:")
	for _, k := range keys {
		fmt.Fprintf(w, "  %-22s %6d  (%.1f%%)\n", k, counts[k], 100*float64(counts[k])/float64(len(outs)))
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "completed streams: %d\n", len(ttfts))
	fmt.Fprintf(w, "  TTFT     p50 %8v  p99 %8v   (includes queue wait)\n", pct(ttfts, 0.5), pct(ttfts, 0.99))
	fmt.Fprintf(w, "  TTFT RAG p50 %8v  p99 %8v\n", pct(ragTTFTs, 0.5), pct(ragTTFTs, 0.99))
	fmt.Fprintf(w, "  total    p50 %8v  p99 %8v\n\n", pct(totals, 0.5), pct(totals, 0.99))
}

func pct(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	return s[int(p*float64(len(s)-1))].Round(time.Millisecond)
}

func printSchedulerStats(w io.Writer, client *http.Client, url string) {
	resp, err := client.Get(url + "/debug/scheduler")
	if err != nil {
		fmt.Fprintln(w, "/debug/scheduler:", err)
		return
	}
	defer resp.Body.Close()
	var v any
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		fmt.Fprintln(w, "/debug/scheduler:", err)
		return
	}
	out, _ := json.MarshalIndent(v, "", "  ")
	fmt.Fprintf(w, "/debug/scheduler:\n%s\n", out)
}
