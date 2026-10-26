package fh

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHealthAwareSelectorEjectsAndRecoversWithOneProbe(t *testing.T) {
	selector, err := NewHealthAwareSelector(HealthSelectorConfig{FailureThreshold: 2, EjectionDuration: time.Minute}, "https://api.example.test")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	selector.now = func() time.Time { return now }
	for i := 0; i < 2; i++ {
		endpoint, err := selector.Next(nil)
		if err != nil {
			t.Fatal(err)
		}
		selector.Report(endpoint, &http.Response{StatusCode: http.StatusServiceUnavailable}, nil)
	}
	if _, err := selector.Next(nil); !errors.Is(err, ErrClientNoHealthyEndpoints) {
		t.Fatalf("expected ejection, got %v", err)
	}
	state := selector.Snapshot()[0]
	if state.Failures != 2 || !state.EjectedUntil.Equal(now.Add(time.Minute)) {
		t.Fatalf("unexpected ejected state: %+v", state)
	}
	now = now.Add(time.Minute)
	probe, err := selector.Next(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := selector.Next(nil); !errors.Is(err, ErrClientNoHealthyEndpoints) {
		t.Fatalf("second probe allowed: %v", err)
	}
	selector.Report(probe, &http.Response{StatusCode: http.StatusOK}, nil)
	healthy, err := selector.Next(nil)
	if err != nil {
		t.Fatalf("healthy endpoint did not recover: %v", err)
	}
	selector.Report(healthy, &http.Response{StatusCode: http.StatusOK}, nil)
	state = selector.Snapshot()[0]
	if state.Failures != 0 || !state.EjectedUntil.IsZero() || state.ProbeInFlight {
		t.Fatalf("unexpected recovered state: %+v", state)
	}
}

func TestHealthAwareSelectorIgnoresStaleAndCanceledReports(t *testing.T) {
	selector, err := NewHealthAwareSelector(HealthSelectorConfig{FailureThreshold: 1, EjectionDuration: time.Minute}, "https://api.example.test")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	selector.now = func() time.Time { return now }
	stale, _ := selector.Next(nil)
	failing, _ := selector.Next(nil)
	selector.Report(failing, nil, errors.New("dial failed"))
	selector.Report(stale, &http.Response{StatusCode: 200}, nil)
	if _, err := selector.Next(nil); !errors.Is(err, ErrClientNoHealthyEndpoints) {
		t.Fatalf("stale success cleared ejection: %v", err)
	}
	now = now.Add(time.Minute)
	probe, _ := selector.Next(nil)
	selector.Report(probe, nil, context.Canceled)
	if _, err := selector.Next(nil); !errors.Is(err, ErrClientNoHealthyEndpoints) {
		t.Fatalf("canceled probe reopened endpoint: %v", err)
	}
	now = now.Add(time.Minute)
	probe, _ = selector.Next(nil)
	selector.Report(probe, &http.Response{StatusCode: 503}, nil)
	if _, err := selector.Next(nil); !errors.Is(err, ErrClientNoHealthyEndpoints) {
		t.Fatalf("failed probe reopened endpoint: %v", err)
	}
}

func TestHealthAwareSelectorClassifiesStatusErrors(t *testing.T) {
	selector, err := NewHealthAwareSelector(HealthSelectorConfig{FailureThreshold: 1}, "https://api.example.test")
	if err != nil {
		t.Fatal(err)
	}
	endpoint, _ := selector.Next(nil)
	selector.Report(endpoint, &http.Response{StatusCode: 404}, ErrClientStatus)
	endpoint, err = selector.Next(nil)
	if err != nil {
		t.Fatalf("HTTP 404 incorrectly ejected endpoint: %v", err)
	}
	selector.Report(endpoint, nil, context.Canceled)
	if selector.Snapshot()[0].Failures != 0 {
		t.Fatal("canceled request counted as endpoint failure")
	}
}

func TestHealthAwareSelectorCustomFailureStatuses(t *testing.T) {
	statuses := map[int]bool{429: true}
	selector, err := NewHealthAwareSelector(HealthSelectorConfig{FailureThreshold: 1, FailureStatuses: statuses}, "https://api.example.test")
	if err != nil {
		t.Fatal(err)
	}
	delete(statuses, 429)
	endpoint, _ := selector.Next(nil)
	selector.Report(endpoint, &http.Response{StatusCode: 503}, nil)
	endpoint, err = selector.Next(nil)
	if err != nil {
		t.Fatalf("unconfigured status ejected endpoint: %v", err)
	}
	selector.Report(endpoint, &http.Response{StatusCode: 200}, nil)
	endpoint, _ = selector.Next(nil)
	selector.Report(endpoint, &http.Response{StatusCode: 429}, nil)
	if _, err := selector.Next(nil); !errors.Is(err, ErrClientNoHealthyEndpoints) {
		t.Fatalf("configured status did not eject endpoint: %v", err)
	}
}

func TestHealthAwareSelectorFailoverOnRetry(t *testing.T) {
	selector, err := NewHealthAwareSelector(HealthSelectorConfig{FailureThreshold: 1, EjectionDuration: time.Minute}, "https://a.example.test", "https://b.example.test")
	if err != nil {
		t.Fatal(err)
	}
	var hosts []string
	client := NewClient(ClientConfig{
		Retry: RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond, RetryMethods: map[string]bool{http.MethodGet: true}, RetryStatuses: map[int]bool{503: true}},
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			hosts = append(hosts, req.URL.Host)
			status := 200
			if req.URL.Host == "a.example.test" {
				status = 503
			}
			return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok"))}, nil
		}),
	})
	client.Use(ClientLoadBalance(selector))
	defer client.Close()
	response, err := client.Get(context.Background(), "https://service.example.test/items")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Close()
	if response.StatusCode() != 200 || len(hosts) != 2 || hosts[0] != "a.example.test" || hosts[1] != "b.example.test" {
		t.Fatalf("failover failed: status=%d hosts=%v", response.StatusCode(), hosts)
	}
}

func TestHealthAwareSelectorNoHealthyEndpointSkipsTransport(t *testing.T) {
	selector, err := NewHealthAwareSelector(HealthSelectorConfig{FailureThreshold: 1}, "https://api.example.test")
	if err != nil {
		t.Fatal(err)
	}
	endpoint, _ := selector.Next(nil)
	selector.Report(endpoint, nil, errors.New("connection failed"))
	client := NewClient(ClientConfig{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("ejected endpoint reached transport")
		return nil, nil
	})})
	client.Use(ClientLoadBalance(selector))
	defer client.Close()
	_, err = client.Get(context.Background(), "https://service.example.test/items")
	if !errors.Is(err, ErrClientNoHealthyEndpoints) {
		t.Fatalf("expected no healthy endpoint error, got %v", err)
	}
}

func TestHealthAwareSelectorReportsAbortedRoundTrip(t *testing.T) {
	selector, err := NewHealthAwareSelector(HealthSelectorConfig{FailureThreshold: 1}, "https://api.example.test")
	if err != nil {
		t.Fatal(err)
	}
	transport := ClientLoadBalance(selector)(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		panic("transport failed")
	}))
	request, err := http.NewRequest(http.MethodGet, "https://service.example.test/items", nil)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() { _ = recover() }()
		_, _ = transport.RoundTrip(request)
	}()
	if len(selector.inflight) != 0 || selector.Snapshot()[0].Failures != 1 {
		t.Fatalf("aborted round trip left selector state: inflight=%d state=%+v", len(selector.inflight), selector.Snapshot())
	}
}

func TestHealthAwareSelectorConcurrentReports(t *testing.T) {
	selector, err := NewHealthAwareSelector(HealthSelectorConfig{}, "https://a.example.test", "https://b.example.test")
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for i := 0; i < 100; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			endpoint, err := selector.Next(nil)
			if err == nil {
				selector.Report(endpoint, &http.Response{StatusCode: 200}, nil)
			}
		}()
	}
	workers.Wait()
	if len(selector.inflight) != 0 {
		t.Fatalf("unreported selections: %d", len(selector.inflight))
	}
}

func TestHealthAwareSelectorRejectsInvalidEndpoints(t *testing.T) {
	for _, endpoints := range [][]string{
		nil,
		{"/relative"},
		{"ftp://example.test"},
		{"https://same.example.test", "https://same.example.test"},
	} {
		if _, err := NewHealthAwareSelector(HealthSelectorConfig{}, endpoints...); err == nil {
			t.Fatalf("accepted invalid endpoints: %v", endpoints)
		}
	}
}
