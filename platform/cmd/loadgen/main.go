// Command loadgen sends a realistic mix of streaming chat requests to the
// platform and reports how admission behaved: status codes, rejection
// reasons, TTFT and total latency percentiles, and the scheduler's own view
// (/debug/scheduler).
//
// Traffic model: Poisson arrivals at -rate requests per second for
// -duration; a -rag-fraction of long-context requests (long prompt, large
// max_tokens) among short chat requests; and a -cancel-fraction of clients
// that give up after a random delay. With -prefixes N, chat requests start
// with one of N long system prompts (uniform, or Zipf with -prefix-skew) to
// exercise prefix-affinity routing (Phase 3); the report then adds the
// workers' real prefix-cache hit rate. With -chaos, one worker is broken
// (down, hung, failing, slow or restarted) through the platform's
// /debug/workers/{id}/faults endpoint for part of the run (Phase 4). It compares scheduling policies with
// the simulator; llmperf takes over against real vLLM in Phase 6.
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
		prefixes  = flag.Int("prefixes", 0, "number of distinct system prompts shared by chat requests; 0 = no system prompt")
		sysChars  = flag.Int("system-chars", 6000, "size of each system prompt, in characters (~4 per token)")
		skew      = flag.Float64("prefix-skew", 0, "Zipf exponent (> 1) for choosing the system prompt, so a few are hot; 0 = uniform")
		chaos     = flag.String("chaos", "", "break one worker during the run: down, hang, errors (50% of streams fail), stall, slow or restart; empty = none")
		chaosWkr  = flag.String("chaos-worker", "sim-worker-1", "worker that -chaos breaks: a simulated worker ID, or the URL of a fake vLLM (e.g. http://localhost:8001)")
		chaosAt   = flag.Duration("chaos-at", 0, "when the fault starts; 0 = a third into -duration")
		chaosFor  = flag.Duration("chaos-for", 0, "how long it lasts; 0 = a third of -duration")
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
	systems := make([]string, *prefixes)
	for i := range systems {
		// Distinct from the first character, so they share no block.
		head := fmt.Sprintf("Sistema %d. Eres el asistente número %d. ", i, i)
		systems[i] = (head + strings.Repeat("Sigue estas instrucciones con cuidado. ", *sysChars/39+1))[:*sysChars]
	}
	pickSystem := func() int { return rng.IntN(len(systems)) }
	if *skew > 1 && len(systems) > 1 {
		z := rand.NewZipf(rng, *skew, 1, uint64(len(systems)-1))
		pickSystem = func() int { return int(z.Uint64()) }
	}

	var (
		mu       sync.Mutex
		outcomes []outcome
		wg       sync.WaitGroup
	)
	start := time.Now()
	if *chaos != "" {
		at, span := *chaosAt, *chaosFor
		if at <= 0 {
			at = *duration / 3
		}
		if span <= 0 {
			span = *duration / 3
		}
		if err := scheduleChaos(client, *url, *chaos, *chaosWkr, at, span); err != nil {
			fmt.Fprintln(os.Stderr, "loadgen:", err)
			os.Exit(2)
		}
	}
	sent := 0
	for next := time.Duration(0); next < *duration; next += time.Duration(rng.ExpFloat64() / *rate * float64(time.Second)) {
		time.Sleep(time.Until(start.Add(next)))
		rag := rng.Float64() < *ragFrac
		system, prompt, maxTokens := "", fmt.Sprintf("Pregunta %d. %s", sent, chatPrompt), *chatMax
		if rag {
			prompt, maxTokens = ragPrompt, *ragMax
		} else if len(systems) > 0 {
			system = systems[pickSystem()]
		}
		var giveUp time.Duration
		if rng.Float64() < *cancelFr {
			giveUp = time.Duration(rng.Int64N(int64(*cancelMax)) + 1)
		}
		sent++
		wg.Add(1)
		go func() {
			defer wg.Done()
			o := send(client, *url, *model, system, prompt, maxTokens, giveUp)
			o.rag = rag
			mu.Lock()
			outcomes = append(outcomes, o)
			mu.Unlock()
		}()
	}
	sendWindow := time.Since(start)
	wg.Wait()
	report(os.Stdout, outcomes, sent, sendWindow, time.Since(start))
	printPrefixCache(os.Stdout, client, *url)
	printSchedulerStats(os.Stdout, client, *url)
}

// scheduleChaos breaks worker at the given offset and repairs it span
// later, through the platform's debug endpoints.
func scheduleChaos(client *http.Client, url, kind, worker string, at, span time.Duration) error {
	faults := map[string]string{
		"down":   `{"down": true}`,
		"hang":   `{"hang": true}`,
		"errors": `{"error_rate": 0.5}`,
		"stall":  `{"stall_after_tokens": 5}`,
		"slow":   `{"slowdown": 4}`,
	}
	target := url
	base := "/debug/workers/" + worker
	if strings.HasPrefix(worker, "http://") || strings.HasPrefix(worker, "https://") {
		// A fake vLLM (Phase 6a): its own fault endpoint and fault names.
		target, base = strings.TrimRight(worker, "/"), "/fake"
		faults = map[string]string{
			"down":   `{"unhealthy": true, "status": 503}`,
			"hang":   `{"hang": true}`,
			"errors": `{"status": 500}`,
			"stall":  `{"cut_after": 5}`,
			"slow":   `{"slow_factor": 4}`,
		}
	}
	post := func(path, body string) {
		resp, err := client.Post(target+path, "application/json", strings.NewReader(body))
		if err != nil {
			fmt.Fprintf(os.Stderr, "loadgen: chaos %s: %v\n", path, err)
			return
		}
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "loadgen: chaos %s: HTTP %d\n", path, resp.StatusCode)
		}
	}
	if kind == "restart" {
		time.AfterFunc(at, func() { post(base+"/restart", "") })
		return nil
	}
	body, ok := faults[kind]
	if !ok {
		return fmt.Errorf("unknown -chaos %q (want down, hang, errors, stall, slow or restart)", kind)
	}
	time.AfterFunc(at, func() {
		fmt.Fprintf(os.Stderr, "loadgen: %s: %s %s\n", time.Now().Format("15:04:05.000"), worker, kind)
		post(base+"/faults", body)
	})
	time.AfterFunc(at+span, func() {
		fmt.Fprintf(os.Stderr, "loadgen: %s: %s repaired\n", time.Now().Format("15:04:05.000"), worker)
		post(base+"/faults", "{}")
	})
	return nil
}

func send(client *http.Client, url, model, system, prompt string, maxTokens int, giveUp time.Duration) outcome {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if giveUp > 0 {
		time.AfterFunc(giveUp, cancel)
	}
	messages := []map[string]string{{"role": "user", "content": prompt}}
	if system != "" {
		messages = append([]map[string]string{{"role": "system", "content": system}}, messages...)
	}
	body, _ := json.Marshal(map[string]any{
		"model":      model,
		"stream":     true,
		"max_tokens": maxTokens,
		"messages":   messages,
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

// printPrefixCache reports the workers' real prefix-cache hit rate (blocks
// served from cache / blocks asked for), the ground truth that the router's
// predicted_hit_rate in /debug/scheduler tries to match. Counters are
// cumulative since the platform started.
func printPrefixCache(w io.Writer, client *http.Client, url string) {
	resp, err := client.Get(url + "/debug/workers")
	if err != nil {
		fmt.Fprintln(w, "/debug/workers:", err)
		return
	}
	defer resp.Body.Close()
	var workers []struct {
		ID       string `json:"worker_id"`
		Granted  uint64 `json:"leases_granted"`
		Capacity int    `json:"prefix_cache_blocks"`
		Used     int    `json:"prefix_cache_used"`
		Queried  uint64 `json:"prefix_blocks_queried"`
		Hit      uint64 `json:"prefix_blocks_hit"`
		Status   struct {
			Healthy  bool   `json:"healthy"`
			Breaker  string `json:"breaker"`
			Failures uint64 `json:"failures"`
		} `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&workers); err != nil {
		fmt.Fprintln(w, "/debug/workers:", err)
		return
	}
	rate := func(hit, queried uint64) float64 {
		if queried == 0 {
			return 0
		}
		return 100 * float64(hit) / float64(queried)
	}
	var hit, queried uint64
	fmt.Fprintln(w, "workers (since start): leases, real prefix-cache hit rate, health, breaker, failures:")
	for _, s := range workers {
		hit, queried = hit+s.Hit, queried+s.Queried
		fmt.Fprintf(w, "  %-14s leases %6d  cache %5d/%-5d blocks  hit rate %5.1f%%  healthy %-5v breaker %-9s failures %d\n",
			s.ID, s.Granted, s.Used, s.Capacity, rate(s.Hit, s.Queried), s.Status.Healthy, s.Status.Breaker, s.Status.Failures)
	}
	fmt.Fprintf(w, "  %-14s %42s hit rate %5.1f%%\n\n", "total", "", rate(hit, queried))
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
