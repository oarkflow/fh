package fh

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPClientAllCommonMethodsAndQueryHelpers(t *testing.T) {
	t.Parallel()
	seen := make(chan string, 16)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Method + " " + r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"method": r.Method, "query": r.URL.RawQuery})
	}))
	defer ts.Close()

	c := NewClient(ClientConfig{BaseURL: ts.URL})
	defer c.Close()
	ctx := context.Background()

	calls := []struct {
		name string
		fn   func() (*Response, error)
	}{
		{"GET", func() (*Response, error) { return c.Get(ctx, "/resource") }},
		{"HEAD", func() (*Response, error) { return c.Head(ctx, "/resource") }},
		{"OPTIONS", func() (*Response, error) { return c.Options(ctx, "/resource") }},
		{"TRACE", func() (*Response, error) { return c.Trace(ctx, "/resource") }},
		{"DELETE", func() (*Response, error) { return c.Delete(ctx, "/resource") }},
		{"POST", func() (*Response, error) { return c.Post(ctx, "/resource", map[string]string{"a": "b"}) }},
		{"PUT", func() (*Response, error) { return c.Put(ctx, "/resource", map[string]string{"a": "b"}) }},
		{"PATCH", func() (*Response, error) { return c.Patch(ctx, "/resource", map[string]string{"a": "b"}) }},
		{"QUERY", func() (*Response, error) { return c.Query(ctx, "/resource", map[string]string{"filter": "active"}) }},
		{"SEARCH", func() (*Response, error) { return c.Search(ctx, "/resource", map[string]string{"filter": "active"}) }},
		{"PROPFIND", func() (*Response, error) { return c.Do(ctx, MethodPropFind, "/resource") }},
	}
	for _, call := range calls {
		res, err := call.fn()
		if err != nil {
			t.Fatalf("%s failed: %v", call.name, err)
		}
		res.Close()
		got := <-seen
		if got[:len(call.name)] != call.name {
			t.Fatalf("expected method %s, got %s", call.name, got)
		}
	}

	res, err := c.R().Query("a", "1").QueryMap(map[string]string{"b": "2"}).QueryRaw("c=3&c=4").Get(ctx, "/query")
	if err != nil {
		t.Fatal(err)
	}
	res.Close()
	got := <-seen
	if got != "GET a=1&b=2&c=3&c=4" {
		t.Fatalf("unexpected query: %s", got)
	}
}

func TestServiceClientAllMethods(t *testing.T) {
	t.Parallel()
	seen := make(chan string, 4)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Method + " " + r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	svc := NewClient(ClientConfig{}).Service(ts.URL + "/api")
	ctx := context.Background()
	checks := []struct {
		want string
		fn   func() (*Response, error)
	}{
		{"OPTIONS /api/users", func() (*Response, error) { return svc.Options(ctx, "/users") }},
		{"PATCH /api/users", func() (*Response, error) { return svc.Patch(ctx, "/users", map[string]string{"x": "y"}) }},
		{"QUERY /api/users", func() (*Response, error) { return svc.Query(ctx, "/users", map[string]string{"x": "y"}) }},
	}
	for _, check := range checks {
		res, err := check.fn()
		if err != nil {
			t.Fatal(err)
		}
		res.Close()
		if got := <-seen; got != check.want {
			t.Fatalf("want %q, got %q", check.want, got)
		}
	}
}
