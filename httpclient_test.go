package fh

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPClientJSONRetryAndMiddleware(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test") != "ok" {
			t.Fatalf("missing middleware header")
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":42,"name":"fh"}`))
	}))
	defer ts.Close()

	c := NewClient(ClientConfig{BaseURL: ts.URL, Retry: RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond, RetryStatuses: map[int]bool{503: true}, RetryMethods: map[string]bool{http.MethodGet: true}}})
	c.Use(ClientHeader("X-Test", "ok"))
	defer c.Close()

	var out struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	res, err := c.R().Decode(&out).Get(context.Background(), "/user")
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsSuccess() || out.ID != 42 || out.Name != "fh" || calls.Load() != 2 {
		t.Fatalf("unexpected response: status=%d out=%+v calls=%d", res.StatusCode(), out, calls.Load())
	}
}

func TestHTTPClientCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(CircuitConfig{FailureThreshold: 1, RecoveryTimeout: time.Hour})
	c := NewClient(ClientConfig{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("down")
	})})
	c.Use(cb.Middleware())
	defer c.Close()

	_, _ = c.Get(context.Background(), "http://example.invalid")
	_, err := c.Get(context.Background(), "http://example.invalid")
	if !errors.Is(err, ErrClientCircuitOpen) {
		t.Fatalf("expected open circuit, got %v", err)
	}
}

func TestHTTPClientSecurityBlocksLocalhost(t *testing.T) {
	c := NewClient(ClientConfig{Security: ClientSecurity{Strict: true, RequireHTTPS: false}})
	defer c.Close()
	_, err := c.Get(context.Background(), "http://127.0.0.1/")
	if !errors.Is(err, ErrClientHostBlocked) {
		t.Fatalf("expected localhost to be blocked, got %v", err)
	}
}
