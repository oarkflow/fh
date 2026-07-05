package fh

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPClientReplayablePostRetry(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var in struct {
			Name string `json:"name"`
		}
		if err := CurrentJSONEngine().NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatal(err)
		}
		if in.Name != "fh" {
			t.Fatalf("unexpected body: %+v", in)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer ts.Close()
	c := NewClient(ClientConfig{Retry: RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond, RetryStatuses: map[int]bool{503: true}, RetryMethods: map[string]bool{http.MethodPost: true}}})
	defer c.Close()
	var out struct {
		OK bool `json:"ok"`
	}
	_, err := c.R().JSON(map[string]string{"name": "fh"}).Decode(&out).Post(context.Background(), ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	if !out.OK || calls.Load() != 2 {
		t.Fatalf("out=%+v calls=%d", out, calls.Load())
	}
}

func TestHTTPClientMemoryCache(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte(`cached`))
	}))
	defer ts.Close()
	c := NewClient(ClientConfig{Retry: NoRetry()})
	c.Use(ClientCache(NewMemoryHTTPCache(time.Minute, 1024)))
	defer c.Close()
	r1, err := c.Get(context.Background(), ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	s1, _ := r1.String()
	r2, err := c.Get(context.Background(), ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	s2, _ := r2.String()
	if s1 != "cached" || s2 != "cached" || calls.Load() != 1 {
		t.Fatalf("cache bad: %q %q calls=%d", s1, s2, calls.Load())
	}
}

func TestHTTPServiceClient(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/users" || r.Header.Get("X-Service") != "yes" {
			t.Fatalf("bad request path=%s header=%s", r.URL.Path, r.Header.Get("X-Service"))
		}
		_, _ = w.Write([]byte(`ok`))
	}))
	defer ts.Close()
	c := NewClient(ClientConfig{Retry: NoRetry()})
	svc := c.Service(ts.URL+"/api").Header("X-Service", "yes")
	res, err := svc.Get(context.Background(), "/users")
	if err != nil {
		t.Fatal(err)
	}
	txt, _ := res.String()
	if txt != "ok" {
		t.Fatalf("got %q", txt)
	}
}
