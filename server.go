package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func LocalFactory(python, script string) Factory {
	return func(ctx context.Context, model string) (Worker, error) {
		command := exec.Command(python, script, "--model", model)
		stdout, err := command.StdoutPipe()
		if err != nil {
			return Worker{}, err
		}
		command.Stderr = io.Discard
		if err = command.Start(); err != nil {
			return Worker{}, err
		}
		done := make(chan error, 1)
		go func() { done <- command.Wait() }()
		var once sync.Once
		stop := func() { once.Do(func() { _ = command.Process.Kill(); <-done }) }
		type ready struct {
			URL string `json:"url"`
		}
		result := make(chan ready, 1)
		go func() {
			var value ready
			scanner := bufio.NewScanner(stdout)
			if scanner.Scan() {
				_ = json.Unmarshal(scanner.Bytes(), &value)
			}
			result <- value
		}()
		select {
		case <-ctx.Done():
			stop()
			return Worker{}, ctx.Err()
		case value := <-result:
			parsed, err := url.Parse(value.URL)
			if err != nil || parsed.Scheme != "http" || parsed.Hostname() != "127.0.0.1" || parsed.Port() == "" {
				stop()
				return Worker{}, fmt.Errorf("worker failed readiness handshake")
			}
			return Worker{URL: value.URL, Stop: stop}, nil
		}
	}
}
func RemoteFactory(base string) (Factory, error) {
	parsed, err := url.Parse(base)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("invalid upstream base URL")
	}
	// Explicit operator configuration, never a user-provided request destination.
	return func(ctx context.Context, model string) (Worker, error) {
		return Worker{URL: strings.TrimRight(base, "/"), Stop: func() {}}, nil
	}, nil
}

type Gateway struct {
	pool      *Pool
	admission chan struct{}
	client    *http.Client
	timeout   time.Duration
	rejected  atomic.Uint64
	failures  atomic.Uint64
	completed atomic.Uint64
}

func NewGateway(pool *Pool, capacity int) *Gateway {
	if capacity < 1 {
		panic("invalid request capacity")
	}
	return &Gateway{pool: pool, admission: make(chan struct{}, capacity), client: &http.Client{Timeout: 30 * time.Second}, timeout: 45 * time.Second}
}
func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		respond(w, 200, map[string]any{"workers": g.pool.Size(), "worker_limit": g.pool.maximum, "admitted_requests": len(g.admission), "request_capacity": cap(g.admission), "cold_starts": g.pool.cold.Load(), "warm_hits": g.pool.warm.Load(), "evictions": g.pool.evicted.Load(), "idle_expirations": g.pool.expired.Load(), "rejected": g.rejected.Load(), "failures": g.failures.Load(), "completed": g.completed.Load()})
	})
	mux.HandleFunc("POST /v1/chat/completions", g.generate)
	return mux
}
func (g *Gateway) generate(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	r.Body = http.MaxBytesReader(w, r.Body, 16384)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		respond(w, 413, map[string]string{"error": "request_too_large"})
		return
	}
	var payload struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if json.Unmarshal(body, &payload) != nil || (payload.Model != "demo-a" && payload.Model != "demo-b" && payload.Model != "demo-c") || payload.Stream {
		respond(w, 400, map[string]string{"error": "unsupported_model_or_payload"})
		return
	}
	select {
	case g.admission <- struct{}{}:
		defer func() { <-g.admission }()
	default:
		g.rejected.Add(1)
		w.Header().Set("Retry-After", "1")
		respond(w, 429, map[string]string{"error": "capacity_exceeded"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), g.timeout)
	defer cancel()
	item, cold, err := g.pool.Acquire(ctx, payload.Model)
	if err != nil {
		g.failures.Add(1)
		respond(w, 503, map[string]string{"error": "worker_unavailable"})
		return
	}
	healthy := false
	defer func() { g.pool.Release(item, healthy) }()
	request, err := http.NewRequestWithContext(ctx, "POST", item.worker.URL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		g.failures.Add(1)
		respond(w, 502, map[string]string{"error": "invalid_worker"})
		return
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := g.client.Do(request)
	if err != nil {
		g.failures.Add(1)
		respond(w, 502, map[string]string{"error": "upstream_failed"})
		return
	}
	defer response.Body.Close()
	result, err := io.ReadAll(io.LimitReader(response.Body, 1048577))
	if err != nil || len(result) > 1048576 || !json.Valid(result) {
		g.failures.Add(1)
		respond(w, 502, map[string]string{"error": "invalid_upstream_response"})
		return
	}
	healthy = response.StatusCode < 500
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-WarmServe-Cold", fmt.Sprint(cold))
	w.WriteHeader(response.StatusCode)
	_, _ = w.Write(result)
	g.completed.Add(1)
	slog.Info("inference request", "model", payload.Model, "cold", cold, "duration_ms", time.Since(start).Milliseconds(), "status", response.StatusCode)
}
