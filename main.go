package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
func main() {
	maximum, err := strconv.Atoi(env("WARMSERVE_MAX_WORKERS", "2"))
	if err != nil || maximum < 1 || maximum > 16 {
		log.Fatal("invalid worker limit")
	}
	ttl, err := time.ParseDuration(env("WARMSERVE_IDLE_TTL", "30s"))
	if err != nil || ttl <= 0 {
		log.Fatal("invalid idle TTL")
	}
	factory := LocalFactory(env("WARMSERVE_PYTHON", "python3"), env("WARMSERVE_WORKER", "worker.py"))
	if upstream := os.Getenv("WARMSERVE_UPSTREAM"); upstream != "" {
		factory, err = RemoteFactory(upstream)
		if err != nil {
			log.Fatal(err)
		}
	}
	pool := NewPool(maximum, ttl, factory)
	defer pool.Close()
	gateway := NewGateway(pool, 16)
	server := &http.Server{Addr: env("WARMSERVE_ADDR", "127.0.0.1:8088"), Handler: gateway.Handler(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 30 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = server.Shutdown(shutdown)
				return
			case <-ticker.C:
				pool.Expire()
			}
		}
	}()
	log.Printf("WarmServe on %s (local CPU demo; no authentication)", server.Addr)
	if err = server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
