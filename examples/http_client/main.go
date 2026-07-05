package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/oarkflow/fh"
)

type echoResponse struct {
	OK      bool              `json:"ok"`
	Query   string            `json:"query"`
	Headers map[string]string `json:"headers"`
}

func main() {
	ctx := context.Background()

	// Keep this example self-contained and deterministic. Using a public service
	// such as httpbin can return HTML from a proxy/WAF/outage page, which makes a
	// JSON example look broken even when the client is working correctly.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/get":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true,"query":"` + r.URL.Query().Get("hello") + `","headers":{"x-request-id":"` + r.Header.Get("X-Request-ID") + `"}}`))
		case "/html":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("<html><body>upstream error</body></html>"))
		case "/methods":
			w.Header().Set("X-Seen-Method", r.Method)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := fh.NewClient(fh.ClientConfig{
		BaseURL:             srv.URL,
		Timeout:             3 * time.Second,
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 128,
		Retry: fh.RetryPolicy{
			MaxAttempts:   3,
			BaseDelay:     50 * time.Millisecond,
			MaxDelay:      time.Second,
			Jitter:        true,
			RetryStatuses: map[int]bool{408: true, 429: true, 500: true, 502: true, 503: true, 504: true},
			RetryMethods:  map[string]bool{http.MethodGet: true, http.MethodPost: true},
		},
	})
	defer client.Close()

	client.Use(
		fh.ClientRecover(),
		fh.ClientRequestID("X-Request-ID"),
		fh.ClientCircuitBreaker(fh.CircuitConfig{FailureThreshold: 5, RecoveryTimeout: 30 * time.Second}),
		fh.ClientBulkhead(128, 100*time.Millisecond),
	)

	var out echoResponse
	res, err := client.R().
		Query("hello", "fh").
		Decode(&out).
		Get(ctx, "/get")
	if err != nil {
		fmt.Println("request failed:", err)
		return
	}
	defer res.DrainAndClose()

	fmt.Println("status:", res.StatusCode())
	fmt.Println("ok:", out.OK)
	fmt.Println("query:", out.Query)
	fmt.Println("request_id_present:", out.Headers["x-request-id"] != "")

	// Non-JSON / non-2xx responses should be handled as HTTP errors, not decoded
	// blindly as JSON and not panicked.
	bad, err := client.R().Get(ctx, "/html")
	if err != nil {
		fmt.Println("request failed:", err)
		return
	}
	defer bad.DrainAndClose()
	if !bad.IsSuccess() {
		fmt.Println("handled_status:", bad.StatusCode())
	}

	methodRes, err := client.Options(ctx, "/methods")
	if err != nil {
		panic(err)
	}
	fmt.Println("options_status:", methodRes.StatusCode())
	_ = methodRes.Close()
}
