package vllm

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"llm-inference-platform/internal/inference"
	"llm-inference-platform/internal/scheduler"
	"llm-inference-platform/internal/vllm/fakevllm"
)

// TestStressWithChaos is the Phase 4 chaos stress test over the network:
// three fake vLLM servers behind the adapter, failing at random, while
// hundreds of requests go through the scheduler. Invariants: no ledger over
// budget, every request has exactly one outcome, nothing reserved or in
// flight at the end.
func TestStressWithChaos(t *testing.T) {
	const budget, requests = 400, 400
	var fakes []*fakevllm.Server
	var workers []inference.Worker
	for range 3 {
		f, url := engine(t, fakevllm.Config{Model: "m", TokenDelay: 200 * time.Microsecond, OutputTokens: 6})
		fakes = append(fakes, f)
		workers = append(workers, adapter(t, Config{BaseURL: url, TokenBudget: budget}))
	}
	s := scheduler.New(scheduler.Config{
		QueueTimeout: 2 * time.Second, QueueTokenFactor: 20, SafetyPoll: 5 * time.Millisecond,
		HealthInterval: 5 * time.Millisecond, BreakerFailures: 3, BreakerCooldown: 20 * time.Millisecond,
		TTFTTimeout: 50 * time.Millisecond, TTFTTimeoutPerToken: -1, StallTimeout: 50 * time.Millisecond,
	}, workers...)

	stop := make(chan struct{})
	chaosDone := make(chan struct{})
	go func() {
		defer close(chaosDone)
		faults := []fakevllm.Faults{{}, {}, {Status: 500}, {Hang: true}, {CutAfter: 2}, {ErrorEventAt: 3}, {Unhealthy: true}, {SlowFactor: 5}}
		for {
			select {
			case <-stop:
				for _, f := range fakes {
					f.SetFaults(fakevllm.Faults{})
				}
				return
			case <-time.After(5 * time.Millisecond):
			}
			f := fakes[rand.IntN(len(fakes))]
			if rand.IntN(10) == 0 {
				f.Restart()
			} else {
				f.SetFaults(faults[rand.IntN(len(faults))])
			}
		}
	}()

	var ok, failed, shed, over atomic.Int64
	var wg sync.WaitGroup
	for i := range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := inference.InferenceRequest{ID: fmt.Sprint(i), Model: "m", Prompt: "hola", PromptTokens: 10 + rand.IntN(30), MaxNewTokens: 6,
				Messages: []inference.Message{{Role: "user", Content: "hola"}}}
			lease, err := s.Acquire(context.Background(), req)
			if err != nil {
				if errors.Is(err, inference.ErrQueueFull) || errors.Is(err, inference.ErrQueueTimeout) || errors.Is(err, inference.ErrNoHealthyWorkers) {
					shed.Add(1)
					return
				}
				t.Errorf("acquire: %v", err)
				return
			}
			defer lease.Release()
			for _, w := range workers {
				if w.Snapshot().TokensReserved > budget {
					over.Add(1)
				}
			}
			chunks, wait := inference.StartStream(context.Background(), lease, 4)
			for range chunks {
			}
			switch err := wait(); {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, inference.ErrWorkerFailure):
				failed.Add(1)
			default:
				t.Errorf("stream: %v", err)
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-chaosDone
	s.Close()

	if over.Load() > 0 {
		t.Fatalf("a ledger went over budget %d times", over.Load())
	}
	if total := ok.Load() + failed.Load() + shed.Load(); total != requests {
		t.Fatalf("outcomes = %d, want %d", total, requests)
	}
	for _, snap := range s.Snapshots() {
		if snap.TokensReserved != 0 || snap.ActiveLeases != 0 || snap.Status.InFlight != 0 {
			t.Fatalf("leak: %+v %+v", snap, snap.Status)
		}
	}
	st := s.Stats()
	if ok.Load() == 0 || st.WorkerFailures == 0 {
		t.Fatalf("the chaos did not exercise both paths: ok=%d stats=%+v", ok.Load(), st)
	}
	t.Logf("ok=%d failed=%d shed=%d worker_failures=%d retries=%d breaker_opens=%d restarts=%d",
		ok.Load(), failed.Load(), shed.Load(), st.WorkerFailures, st.Retries, st.BreakerOpens, st.RestartsDetected)
}
