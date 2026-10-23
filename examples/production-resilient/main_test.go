package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/circuitbreaker"
)

func startTestServer(t *testing.T, cfg ServerConfig) (string, *ApplicationState, func()) {
	t.Helper()
	app, state := BuildApp(cfg)

	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on loopback: %v", err)
	}

	go func() {
		_ = app.Serve(ln)
	}()

	addr := "http://" + ln.Addr().String()
	cleanup := func() {
		_ = app.ShutdownWithTimeout(time.Second)
		_ = ln.Close()
	}

	return addr, state, cleanup
}

func TestLivenessAndReadinessProbe(t *testing.T) {
	cfg := DefaultServerConfig()
	addr, _, cleanup := startTestServer(t, cfg)
	defer cleanup()

	client := &http.Client{Timeout: 2 * time.Second}

	// 1. Liveness
	resp, err := client.Get(addr + "/health/live")
	if err != nil {
		t.Fatalf("liveness request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for live probe, got %d", resp.StatusCode)
	}

	// 2. Readiness
	respReady, err := client.Get(addr + "/health/ready")
	if err != nil {
		t.Fatalf("readiness request failed: %v", err)
	}
	defer respReady.Body.Close()
	if respReady.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for ready probe, got %d", respReady.StatusCode)
	}
}

func TestSecurityHeadersAndRequestID(t *testing.T) {
	cfg := DefaultServerConfig()
	addr, _, cleanup := startTestServer(t, cfg)
	defer cleanup()

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(addr + "/health/live")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	// Verify X-Request-ID
	if reqID := resp.Header.Get("X-Request-ID"); reqID == "" {
		t.Fatal("expected non-empty X-Request-ID header")
	}

	// Verify Security Headers
	if xfo := resp.Header.Get("X-Frame-Options"); xfo != "DENY" {
		t.Fatalf("expected X-Frame-Options: DENY, got %q", xfo)
	}
	if ct := resp.Header.Get("X-Content-Type-Options"); ct != "nosniff" {
		t.Fatalf("expected X-Content-Type-Options: nosniff, got %q", ct)
	}
}

func TestRateLimiting(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.RateLimitMax = 5 // low limit for quick test
	cfg.RateLimitWindow = time.Second

	addr, _, cleanup := startTestServer(t, cfg)
	defer cleanup()

	client := &http.Client{Timeout: 2 * time.Second}

	// First 5 requests should pass
	for i := 1; i <= 5; i++ {
		resp, err := client.Get(addr + "/api/v1/catalog")
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK on request %d, got %d", i, resp.StatusCode)
		}
	}

	// 6th request must be rejected with 429
	respExceeded, err := client.Get(addr + "/api/v1/catalog")
	if err != nil {
		t.Fatalf("rate limited request failed: %v", err)
	}
	defer respExceeded.Body.Close()

	if respExceeded.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", respExceeded.StatusCode)
	}
	if retryAfter := respExceeded.Header.Get("Retry-After"); retryAfter == "" {
		t.Fatal("expected Retry-After header on 429 response")
	}
}

func TestBodyLimitProtection(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.BodyLimitBytes = 256 // limit to 256 bytes

	addr, _, cleanup := startTestServer(t, cfg)
	defer cleanup()

	client := &http.Client{Timeout: 2 * time.Second}

	// Send an oversized payload (1 KiB > 256 bytes)
	hugePayload := bytes.Repeat([]byte("A"), 1024)
	resp, err := client.Post(addr+"/api/v1/orders", "application/json", bytes.NewReader(hugePayload))
	if err != nil {
		t.Fatalf("post failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 Payload Too Large, got %d", resp.StatusCode)
	}
}

func TestPanicRecoveryWithoutCrashing(t *testing.T) {
	cfg := DefaultServerConfig()
	addr, _, cleanup := startTestServer(t, cfg)
	defer cleanup()

	client := &http.Client{Timeout: 2 * time.Second}

	// 1. Hit panic endpoint
	resp, err := client.Get(addr + "/api/v1/panic")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 Internal Server Error, got %d", resp.StatusCode)
	}

	// 2. Ensure server is still healthy and operational immediately afterwards
	respAlive, err := client.Get(addr + "/health/live")
	if err != nil {
		t.Fatalf("subsequent alive check failed: %v", err)
	}
	defer respAlive.Body.Close()

	if respAlive.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK after recovered panic, got %d", respAlive.StatusCode)
	}
}

func TestCircuitBreakerTrippingAndRecovery(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.BreakerFailures = 2
	cfg.BreakerReset = 100 * time.Millisecond

	addr, state, cleanup := startTestServer(t, cfg)
	defer cleanup()

	client := &http.Client{Timeout: 2 * time.Second}

	// 1. Initial request succeeds
	resp, err := client.Get(addr + "/api/v1/external-service")
	if err != nil {
		t.Fatalf("initial request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	// 2. Inject fault into upstream
	state.SimulateFault.Store(true)

	// Cause 2 consecutive failures
	for i := 0; i < 2; i++ {
		respFail, err := client.Get(addr + "/api/v1/external-service")
		if err != nil {
			t.Fatalf("failure request %d failed: %v", i, err)
		}
		respFail.Body.Close()
		if respFail.StatusCode != http.StatusBadGateway {
			t.Fatalf("expected 502 Bad Gateway on fault, got %d", respFail.StatusCode)
		}
	}

	// 3. Circuit breaker must now be OPEN
	if state.Breaker.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected StateOpen, got %v", state.Breaker.State())
	}

	// 4. Next request must be rejected with 503 without hitting downstream
	respTripped, err := client.Get(addr + "/api/v1/external-service")
	if err != nil {
		t.Fatalf("breaker trip check failed: %v", err)
	}
	defer respTripped.Body.Close()
	if respTripped.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d", respTripped.StatusCode)
	}

	// 5. Readiness probe should reflect degraded state
	respReady, err := client.Get(addr + "/health/ready")
	if err != nil {
		t.Fatalf("readiness probe check failed: %v", err)
	}
	respReady.Body.Close()
	if respReady.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on degraded readiness, got %d", respReady.StatusCode)
	}

	// 6. Heal downstream and wait for reset window
	state.SimulateFault.Store(false)
	time.Sleep(150 * time.Millisecond)

	// 7. Canary probe succeeds, resetting circuit to CLOSED
	respRecovered, err := client.Get(addr + "/api/v1/external-service")
	if err != nil {
		t.Fatalf("recovery request failed: %v", err)
	}
	respRecovered.Body.Close()
	if respRecovered.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on recovery, got %d", respRecovered.StatusCode)
	}

	if state.Breaker.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected StateClosed after recovery, got %v", state.Breaker.State())
	}
}

func TestCoalescingSingleFlight(t *testing.T) {
	cfg := DefaultServerConfig()
	addr, state, cleanup := startTestServer(t, cfg)
	defer cleanup()

	client := &http.Client{Timeout: 2 * time.Second}

	// Spawn 10 concurrent requests simultaneously to /api/v1/catalog
	const concurrency = 10
	var wg sync.WaitGroup
	wg.Add(concurrency)

	errorsChan := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			resp, err := client.Get(addr + "/api/v1/catalog")
			if err != nil {
				errorsChan <- err
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errorsChan <- io.EOF
			}
		}()
	}

	wg.Wait()
	close(errorsChan)

	for err := range errorsChan {
		if err != nil {
			t.Fatalf("concurrent request error: %v", err)
		}
	}

	// Because of coalesce middleware, only 1 actual execution hit the backend handler
	hits := state.BackendHits.Load()
	if hits != 1 {
		t.Fatalf("expected exactly 1 backend execution due to coalescing, got %d", hits)
	}
}

func TestPrometheusMetricsExport(t *testing.T) {
	cfg := DefaultServerConfig()
	addr, _, cleanup := startTestServer(t, cfg)
	defer cleanup()

	client := &http.Client{Timeout: 2 * time.Second}

	// Execute some traffic
	_, _ = client.Get(addr + "/health/live")

	// Fetch metrics in Prometheus format
	resp, err := client.Get(addr + "/metrics?format=prometheus")
	if err != nil {
		t.Fatalf("metrics request failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read metrics body: %v", err)
	}

	bodyStr := string(body)
	if !strings.Contains(bodyStr, "fh_http_requests_total") && !strings.Contains(bodyStr, "http_requests_total") {
		// Verify Prometheus metrics format
		if !strings.Contains(bodyStr, "requests_total") && !strings.Contains(bodyStr, "http_") {
			t.Fatalf("expected Prometheus metrics output, got:\n%s", bodyStr)
		}
	}
}

func TestOrderValidation(t *testing.T) {
	cfg := DefaultServerConfig()
	addr, _, cleanup := startTestServer(t, cfg)
	defer cleanup()

	client := &http.Client{Timeout: 2 * time.Second}

	// Valid order
	validOrder := `{"item":"enterprise-license","quantity":2,"amount":499.00}`
	req, _ := http.NewRequest("POST", addr+"/api/v1/orders", strings.NewReader(validOrder))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(fh.HeaderIdempotencyKey, "idem-test-999")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("order request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", resp.StatusCode)
	}

	var res map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if res["idempotency_key"] != "idem-test-999" {
		t.Fatalf("expected idempotency_key echo, got %v", res["idempotency_key"])
	}
}
