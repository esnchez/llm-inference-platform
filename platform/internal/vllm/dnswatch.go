package vllm

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"slices"
	"time"
)

// DNSWatcher discovers vLLM servers behind a DNS name (Phase 7, decision 3).
// A Kubernetes headless Service answers with one A record per ready pod, so
// re-resolving the name every Interval is enough to follow scale-ups and
// scale-downs without the Kubernetes API (no client-go, no RBAC).
//
// Each new IP gets its own Connector (Phase 6a): it waits for the server to
// be ready and registers it. When an IP disappears its Connector is stopped
// and the worker is removed, which drains it (Phase 4): no new requests, the
// streams in flight finish.
type DNSWatcher struct {
	// Host and Port of the servers; Host is resolved, Port is shared.
	Host, Port string
	// Template is copied for every server; its BaseURL is replaced.
	Template Config
	Registry Registry
	Log      *slog.Logger
	// Interval between resolutions (default 10s).
	Interval time.Duration
	// Lookup resolves Host (default net.DefaultResolver.LookupHost); tests
	// replace it.
	Lookup func(ctx context.Context, host string) ([]string, error)
	// Connector settings (zero = Connector defaults).
	MinBackoff, MaxBackoff, Recheck time.Duration
}

// Run blocks until ctx is cancelled.
func (d *DNSWatcher) Run(ctx context.Context) {
	if d.Interval <= 0 {
		d.Interval = 10 * time.Second
	}
	if d.Lookup == nil {
		d.Lookup = net.DefaultResolver.LookupHost
	}
	if d.Log == nil {
		d.Log = slog.Default()
	}
	type running struct {
		cancel context.CancelFunc
		done   chan struct{}
	}
	conns := map[string]running{} // by IP
	defer func() {
		for _, r := range conns {
			r.cancel()
			<-r.done
		}
	}()
	for {
		ips, err := d.Lookup(ctx, d.Host)
		switch {
		case ctx.Err() != nil:
			return
		case err != nil && !isNotFound(err):
			// A transient resolver error must not drain every worker: keep
			// the current set until the next successful resolution.
			d.Log.Warn("vllm dns lookup failed", "host", d.Host, "error", err)
		default:
			// NXDOMAIN is a valid answer: a headless Service with no ready
			// pod has no records.
			for _, ip := range ips {
				if _, ok := conns[ip]; ok {
					continue
				}
				cfg := d.Template
				cfg.BaseURL = "http://" + net.JoinHostPort(ip, d.Port)
				cfg.ID = ""
				cctx, cancel := context.WithCancel(ctx)
				done := make(chan struct{})
				c := &Connector{
					Config: cfg, Registry: d.Registry, Log: d.Log, RemoveOnStop: true,
					MinBackoff: d.MinBackoff, MaxBackoff: d.MaxBackoff, Recheck: d.Recheck,
				}
				go func() { defer close(done); c.Run(cctx) }()
				conns[ip] = running{cancel, done}
				d.Log.Info("vllm dns: new server", "host", d.Host, "url", cfg.BaseURL)
			}
			for ip, r := range conns {
				if slices.Contains(ips, ip) {
					continue
				}
				d.Log.Info("vllm dns: server gone, draining", "host", d.Host, "ip", ip)
				r.cancel()
				<-r.done // the connector removes (drains) its worker on exit
				delete(conns, ip)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(d.Interval):
		}
	}
}

func isNotFound(err error) bool {
	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr) && dnsErr.IsNotFound
}
