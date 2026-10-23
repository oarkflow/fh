package main

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// BenchmarkResilientServerThroughput demonstrates high-concurrency request handling
// through the entire defense-in-depth middleware pipeline (recovery, security headers,
// request-id, rate limiter, metrics, and JSON response serialization).
//
// Run with:
//   go test -bench=BenchmarkResilientServerThroughput -benchtime=3s ./examples/production-resilient
func BenchmarkResilientServerThroughput(b *testing.B) {
	cfg := DefaultServerConfig()
	cfg.RateLimitMax = 1_000_000 // allow high benchmark throughput
	cfg.RateLimitWindow = time.Minute

	app, _ := BuildApp(cfg)

	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen on loopback: %v", err)
	}
	defer app.ShutdownWithTimeout(time.Second)
	defer ln.Close()

	go func() {
		_ = app.Serve(ln)
	}()

	addr := "http://" + ln.Addr().String()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 5 * time.Second,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(addr + "/health/live")
			if err != nil {
				b.Errorf("get failed: %v", err)
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	})
}

// BenchmarkResilientServerPostOrder benchmarks transactional order creation
// under concurrent load including body parsing, validation, and JSON echo.
func BenchmarkResilientServerPostOrder(b *testing.B) {
	cfg := DefaultServerConfig()
	cfg.RateLimitMax = 1_000_000
	cfg.RateLimitWindow = time.Minute

	app, _ := BuildApp(cfg)

	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen on loopback: %v", err)
	}
	defer app.ShutdownWithTimeout(time.Second)
	defer ln.Close()

	go func() {
		_ = app.Serve(ln)
	}()

	addr := "http://" + ln.Addr().String()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 5 * time.Second,
	}

	payload := []byte(`{"item":"enterprise-sku","quantity":5,"amount":1250.00}`)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Post(addr+"/api/v1/orders", "application/json", bytes.NewReader(payload))
			if err != nil {
				b.Errorf("post failed: %v", err)
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	})
}
