package vllm

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"llm-inference-platform/internal/vllm/fakevllm"
)

// resolver is a DNS answer the test changes at will.
type resolver struct {
	mu  sync.Mutex
	ips []string
	err error
}

func (r *resolver) set(err error, ips ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ips, r.err = ips, err
}

func (r *resolver) lookup(context.Context, string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.ips), r.err
}

// engineAt serves a fake vLLM on ip:port (the headless Service gives every
// pod its own IP and the same port).
func engineAt(t *testing.T, ip, port string) {
	t.Helper()
	ln, err := net.Listen("tcp", net.JoinHostPort(ip, port))
	if err != nil {
		t.Skipf("cannot listen on %s (loopback alias missing?): %v", ip, err)
	}
	ts := httptest.NewUnstartedServer(fakevllm.New(fakevllm.Config{Model: "m"}).Handler())
	ts.Listener = ln
	ts.Start()
	t.Cleanup(ts.Close)
}

func TestDNSWatcherFollowsTheAnswer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	ts := httptest.NewUnstartedServer(fakevllm.New(fakevllm.Config{Model: "m"}).Handler())
	ts.Listener = ln
	ts.Start()
	t.Cleanup(ts.Close)
	engineAt(t, "127.0.0.2", port)

	res := &resolver{}
	res.set(nil, "127.0.0.1")
	reg := &registry{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	w := &DNSWatcher{
		Host: "fakevllm.test", Port: port, Registry: reg, Lookup: res.lookup,
		Interval: 5 * time.Millisecond, MinBackoff: 5 * time.Millisecond, MaxBackoff: 10 * time.Millisecond,
		Recheck: 10 * time.Millisecond, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	go func() { defer close(done); w.Run(ctx) }()

	waitUntil(t, "first server", func() bool { a, _ := reg.counts(); return a == 1 })

	// Scale-up: a second pod appears in the answer.
	res.set(nil, "127.0.0.1", "127.0.0.2")
	waitUntil(t, "second server", func() bool { a, _ := reg.counts(); return a == 2 })

	// A resolver failure keeps the current set (no mass drain).
	res.set(errors.New("server misbehaving"))
	time.Sleep(30 * time.Millisecond)
	if _, r := reg.counts(); r != 0 {
		t.Fatalf("a lookup error removed %d workers", r)
	}

	// Scale-down: the first pod leaves the answer and is drained.
	res.set(nil, "127.0.0.2")
	waitUntil(t, "removal", func() bool { _, r := reg.counts(); return r == 1 })
	reg.mu.Lock()
	gone := reg.removed[0]
	reg.mu.Unlock()
	if want := "vllm-127.0.0.1:" + port; gone != want {
		t.Fatalf("removed %q, want %q", gone, want)
	}

	// No pod ready: NXDOMAIN is an answer, every worker is drained.
	res.set(&net.DNSError{Err: "no such host", Name: "fakevllm.test", IsNotFound: true})
	waitUntil(t, "removal of the last one", func() bool { _, r := reg.counts(); return r == 2 })

	cancel()
	<-done
	if a, r := reg.counts(); a != 2 || r != 2 {
		t.Fatalf("added %d removed %d, want 2 and 2", a, r)
	}
}

func TestIsNotFound(t *testing.T) {
	if !isNotFound(&net.DNSError{IsNotFound: true}) || isNotFound(errors.New("x")) || isNotFound(&net.DNSError{IsTimeout: true}) {
		t.Fatal("isNotFound")
	}
}
