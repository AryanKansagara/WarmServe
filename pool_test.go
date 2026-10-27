package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fakeFactory(starts, stops *atomic.Int32) Factory {
	return func(ctx context.Context, model string) (Worker, error) {
		starts.Add(1)
		return Worker{URL: "http://127.0.0.1:1", Stop: func() { stops.Add(1) }}, nil
	}
}
func TestWarmReuse(t *testing.T) {
	var starts, stops atomic.Int32
	p := NewPool(2, time.Minute, fakeFactory(&starts, &stops))
	defer p.Close()
	first, cold, err := p.Acquire(context.Background(), "demo-a")
	if err != nil || !cold {
		t.Fatal("cold acquisition failed")
	}
	p.Release(first, true)
	next, cold, err := p.Acquire(context.Background(), "demo-a")
	if err != nil || cold || first != next || starts.Load() != 1 {
		t.Fatal("worker not reused")
	}
	p.Release(next, true)
}
func TestCapacityWaitIsCancellable(t *testing.T) {
	var starts, stops atomic.Int32
	p := NewPool(1, time.Minute, fakeFactory(&starts, &stops))
	defer p.Close()
	item, _, _ := p.Acquire(context.Background(), "demo-a")
	defer p.Release(item, true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := p.Acquire(ctx, "demo-b"); err == nil {
		t.Fatal("capacity exceeded")
	}
	if starts.Load() != 1 {
		t.Fatal("extra process started")
	}
}
func TestIdleLRUEviction(t *testing.T) {
	var starts, stops atomic.Int32
	p := NewPool(2, time.Minute, fakeFactory(&starts, &stops))
	defer p.Close()
	a, _, _ := p.Acquire(context.Background(), "demo-a")
	p.Release(a, true)
	b, _, _ := p.Acquire(context.Background(), "demo-b")
	p.Release(b, true)
	c, _, err := p.Acquire(context.Background(), "demo-c")
	if err != nil {
		t.Fatal(err)
	}
	p.Release(c, true)
	if p.evicted.Load() != 1 || stops.Load() != 1 || p.Size() != 2 {
		t.Fatal("eviction contract failed")
	}
}
func TestScaleToZero(t *testing.T) {
	var starts, stops atomic.Int32
	p := NewPool(2, time.Second, fakeFactory(&starts, &stops))
	defer p.Close()
	a, _, _ := p.Acquire(context.Background(), "demo-a")
	p.Release(a, true)
	p.now = func() time.Time { return time.Now().Add(2 * time.Second) }
	if p.Expire() != 1 || p.Size() != 0 || stops.Load() != 1 {
		t.Fatal("idle worker retained")
	}
}
func TestBusyWorkerIsNeverExpired(t *testing.T) {
	var starts, stops atomic.Int32
	p := NewPool(1, time.Second, fakeFactory(&starts, &stops))
	defer p.Close()
	a, _, _ := p.Acquire(context.Background(), "demo-a")
	p.now = func() time.Time { return time.Now().Add(2 * time.Second) }
	if p.Expire() != 0 || stops.Load() != 0 {
		t.Fatal("busy worker expired")
	}
	p.Release(a, true)
}
func TestUnhealthyWorkerIsRemoved(t *testing.T) {
	var starts, stops atomic.Int32
	p := NewPool(1, time.Minute, fakeFactory(&starts, &stops))
	defer p.Close()
	a, _, _ := p.Acquire(context.Background(), "demo-a")
	p.Release(a, false)
	if p.Size() != 0 || stops.Load() != 1 {
		t.Fatal("unhealthy worker retained")
	}
}
func TestFailedStartupReleasesCapacity(t *testing.T) {
	p := NewPool(1, time.Minute, func(context.Context, string) (Worker, error) { return Worker{}, errors.New("startup failed") })
	defer p.Close()
	if _, _, err := p.Acquire(context.Background(), "demo-a"); err == nil || p.Size() != 0 {
		t.Fatal("capacity leaked")
	}
}
func TestClosedPoolRejectsRequests(t *testing.T) {
	var starts, stops atomic.Int32
	p := NewPool(1, time.Minute, fakeFactory(&starts, &stops))
	p.Close()
	if _, _, err := p.Acquire(context.Background(), "demo-a"); err == nil {
		t.Fatal("closed pool accepted work")
	}
}
func TestCloseDuringStartup(t *testing.T) {
	begin := make(chan struct{})
	finish := make(chan struct{})
	var stopped atomic.Int32
	p := NewPool(1, time.Minute, func(context.Context, string) (Worker, error) {
		close(begin)
		<-finish
		return Worker{Stop: func() { stopped.Add(1) }}, nil
	})
	done := make(chan error, 1)
	go func() { _, _, err := p.Acquire(context.Background(), "demo-a"); done <- err }()
	<-begin
	p.Close()
	close(finish)
	if err := <-done; err == nil || stopped.Load() != 1 {
		t.Fatal("startup worker leaked during close")
	}
}
func setupGateway(t *testing.T, backend http.HandlerFunc, capacity int) (*Gateway, *httptest.Server) {
	t.Helper()
	upstream := httptest.NewServer(backend)
	p := NewPool(2, time.Minute, func(context.Context, string) (Worker, error) { return Worker{URL: upstream.URL, Stop: func() {}}, nil })
	g := NewGateway(p, capacity)
	server := httptest.NewServer(g.Handler())
	t.Cleanup(func() { server.Close(); p.Close(); upstream.Close() })
	return g, server
}
func goodBackend(w http.ResponseWriter, r *http.Request) {
	respond(w, 200, map[string]any{"choices": []any{map[string]string{"text": "demo"}}})
}

const requestBody = `{"model":"demo-a","messages":[{"role":"user","content":"test"}],"max_tokens":4}`

func post(server *httptest.Server, body string) (*http.Response, error) {
	return server.Client().Post(server.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
}
func TestGatewayColdAndWarmHeaders(t *testing.T) {
	_, server := setupGateway(t, goodBackend, 4)
	for index, expected := range []string{"true", "false"} {
		response, err := post(server, requestBody)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != 200 || response.Header.Get("X-WarmServe-Cold") != expected {
			t.Fatalf("request %d failed", index)
		}
	}
}
func TestBackpressureReturns429(t *testing.T) {
	begin := make(chan struct{})
	finish := make(chan struct{})
	_, server := setupGateway(t, func(w http.ResponseWriter, r *http.Request) { close(begin); <-finish; goodBackend(w, r) }, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		response, err := post(server, requestBody)
		if err == nil {
			response.Body.Close()
		}
	}()
	<-begin
	response, err := post(server, requestBody)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	close(finish)
	<-done
	if response.StatusCode != 429 || response.Header.Get("Retry-After") == "" {
		t.Fatal("missing backpressure")
	}
}
func TestInvalidPayloads(t *testing.T) {
	_, server := setupGateway(t, goodBackend, 4)
	for _, body := range []string{"not JSON", `{"model":"other"}`, `{"model":"demo-a","stream":true}`} {
		response, err := post(server, body)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != 400 {
			t.Fatal("invalid payload accepted")
		}
	}
}
func TestOversizedBody(t *testing.T) {
	_, server := setupGateway(t, goodBackend, 4)
	response, err := post(server, strings.Repeat("x", 17000))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 413 {
		t.Fatal("oversized body accepted")
	}
}
func TestInvalidUpstreamJSON(t *testing.T) {
	g, server := setupGateway(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("not JSON")) }, 4)
	response, err := post(server, requestBody)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 502 || g.pool.Size() != 0 {
		t.Fatal("invalid worker response accepted")
	}
}
func TestUpstreamFailureRemovesWorker(t *testing.T) {
	g, server := setupGateway(t, func(w http.ResponseWriter, r *http.Request) { respond(w, 500, map[string]string{"error": "failed"}) }, 4)
	response, err := post(server, requestBody)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if g.pool.Size() != 0 {
		t.Fatal("failed worker reused")
	}
}
func TestHealthAndMetrics(t *testing.T) {
	_, server := setupGateway(t, goodBackend, 4)
	for _, path := range []string{"/healthz", "/metrics"} {
		response, err := server.Client().Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]any
		err = json.NewDecoder(response.Body).Decode(&result)
		response.Body.Close()
		if err != nil || response.StatusCode != 200 {
			t.Fatal("invalid status endpoint")
		}
	}
}
func TestConcurrentProxyRequests(t *testing.T) {
	g, server := setupGateway(t, goodBackend, 32)
	var wg sync.WaitGroup
	for index := 0; index < 24; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := post(server, requestBody)
			if err != nil {
				t.Error(err)
				return
			}
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode != 200 {
				t.Errorf("status %d", response.StatusCode)
			}
		}()
	}
	wg.Wait()
	if g.pool.Size() > 2 || g.completed.Load() != 24 {
		t.Fatal("concurrency invariant failed")
	}
}
func TestRemoteAdapter(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(goodBackend))
	defer upstream.Close()
	factory, err := RemoteFactory(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := factory(context.Background(), "demo-a")
	if err != nil || worker.URL != upstream.URL {
		t.Fatal("adapter failed")
	}
	worker.Stop()
}
func TestInvalidRemoteConfiguration(t *testing.T) {
	for _, base := range []string{"file:///tmp/test", "http://user:secret@host", "not a url", "http://host/?key=x"} {
		if _, err := RemoteFactory(base); err == nil {
			t.Fatal("unsafe configuration accepted")
		}
	}
}
