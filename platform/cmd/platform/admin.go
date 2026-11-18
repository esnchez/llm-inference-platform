package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"llm-inference-platform/internal/scheduler"
	"llm-inference-platform/internal/simworker"
)

// simPool owns the simulated workers and serves the debug endpoints that
// add, remove, break and restart them at run time (Phase 4, decisions 1 and
// 6). They exist to exercise the scheduler's lifecycle by hand and from
// cmd/loadgen; with vLLM (Phase 6) workers will come from the deployment
// (Phase 7), not from an HTTP call.
type simPool struct {
	template simworker.Config // ID left empty
	sched    *scheduler.Scheduler

	mu      sync.Mutex
	next    int
	workers map[string]*simworker.Worker
}

func newSimPool(template simworker.Config) *simPool {
	return &simPool{template: template, workers: map[string]*simworker.Worker{}}
}

// newWorker creates the next simulated worker (sim-worker-N).
func (p *simPool) newWorker() (*simworker.Worker, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next++
	cfg := p.template
	cfg.ID = fmt.Sprintf("sim-worker-%d", p.next)
	w, err := simworker.New(cfg)
	if err != nil {
		return nil, err
	}
	p.workers[cfg.ID] = w
	return w, nil
}

func (p *simPool) get(id string) *simworker.Worker {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.workers[id]
}

// register adds the admin routes to mux.
func (p *simPool) register(mux *http.ServeMux) {
	mux.HandleFunc("POST /debug/workers", p.handleAdd)
	mux.HandleFunc("DELETE /debug/workers/{id}", p.handleRemove)
	mux.HandleFunc("POST /debug/workers/{id}/faults", p.handleFaults)
	mux.HandleFunc("POST /debug/workers/{id}/restart", p.handleRestart)
}

func (p *simPool) handleAdd(w http.ResponseWriter, _ *http.Request) {
	sw, err := p.newWorker()
	if err == nil {
		err = p.sched.AddWorker(sw)
	}
	if err != nil {
		reply(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	reply(w, http.StatusCreated, map[string]string{"worker_id": sw.ID()})
}

// handleRemove drains the worker: it answers at once, the worker leaves
// /debug/workers when its last stream ends.
func (p *simPool) handleRemove(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := p.sched.RemoveWorker(id); err != nil {
		reply(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	p.mu.Lock()
	delete(p.workers, id)
	p.mu.Unlock()
	reply(w, http.StatusAccepted, map[string]string{"worker_id": id, "state": "draining"})
}

// handleFaults replaces the worker's injected faults with the JSON body,
// e.g. {"down": true} or {"error_rate": 0.3}; {} clears them.
func (p *simPool) handleFaults(w http.ResponseWriter, r *http.Request) {
	sw := p.get(r.PathValue("id"))
	if sw == nil {
		reply(w, http.StatusNotFound, map[string]string{"error": "unknown worker"})
		return
	}
	var f simworker.Faults
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		reply(w, http.StatusBadRequest, map[string]string{"error": "invalid faults: " + err.Error()})
		return
	}
	if err := sw.SetFaults(f); err != nil {
		reply(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	reply(w, http.StatusOK, sw.Faults())
}

func (p *simPool) handleRestart(w http.ResponseWriter, r *http.Request) {
	sw := p.get(r.PathValue("id"))
	if sw == nil {
		reply(w, http.StatusNotFound, map[string]string{"error": "unknown worker"})
		return
	}
	sw.Restart()
	reply(w, http.StatusOK, map[string]string{"worker_id": sw.ID(), "state": "restarted"})
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // the client may be gone; nothing to do
}
