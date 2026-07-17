package fh

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestRouterHEADFallbackIsPerPath(t *testing.T) {
	r := NewRouter()
	getCalled := false
	r.Add("GET", "/articles/:id", func(Ctx) error {
		getCalled = true
		return nil
	})
	// This unrelated route used to disable HEAD-to-GET fallback in Find.
	r.Add("HEAD", "/health", func(Ctx) error { return nil })
	r.Freeze()

	var params []Param
	h := r.Find("HEAD", "/articles/42", &params)
	if h == nil {
		t.Fatal("HEAD did not fall back to matching GET route")
	}
	if err := h(nil); err != nil || !getCalled {
		t.Fatalf("GET fallback handler was not called: err=%v", err)
	}
	if len(params) != 1 || params[0] != (Param{Key: "id", Value: "42"}) {
		t.Fatalf("fallback params = %#v", params)
	}
}

func TestRouterLookupIgnoresQueryString(t *testing.T) {
	r := NewRouter()
	r.Add("GET", "/", func(Ctx) error { return nil })
	r.Add("GET", "/static", func(Ctx) error { return nil })
	r.Add("GET", "/users/:id", func(Ctx) error { return nil })
	r.Add("GET", "/files/*path", func(Ctx) error { return nil })
	r.Freeze()

	for _, tc := range []struct {
		path      string
		paramKey  string
		paramWant string
	}{
		{path: "/?format=json"},
		{path: "/static?format=json"},
		{path: "/users/42?expand=team", paramKey: "id", paramWant: "42"},
		{path: "/files/css/app.css?v=1", paramKey: "path", paramWant: "css/app.css"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			for _, find := range []func(*[]Param) HandlerFunc{
				func(params *[]Param) HandlerFunc { return r.Find("GET", tc.path, params) },
				func(params *[]Param) HandlerFunc {
					return r.FindBytes([]byte("GET"), []byte(tc.path), params)
				},
			} {
				var params []Param
				if find(&params) == nil {
					t.Fatal("route not found")
				}
				if tc.paramKey == "" {
					if len(params) != 0 {
						t.Fatalf("static route params = %#v", params)
					}
				} else if len(params) != 1 || params[0] != (Param{Key: tc.paramKey, Value: tc.paramWant}) {
					t.Fatalf("params = %#v", params)
				}
			}
		})
	}
}

func TestRouterStaticPrecedenceWithLargeRouteTable(t *testing.T) {
	r := NewRouter()
	called := ""
	r.Add("GET", "/users/:id", func(Ctx) error { called = "param"; return nil })
	for i := 0; i < maxLinearRouteShortcuts; i++ {
		r.Add("GET", fmt.Sprintf("/static-%d", i), func(Ctx) error { return nil })
	}
	r.Add("GET", "/users/new", func(Ctx) error { called = "static"; return nil })
	r.Freeze()

	params := []Param{{Key: "stale", Value: "stale"}}
	h := r.FindBytes([]byte("GET"), []byte("/users/new"), &params)
	if h == nil {
		t.Fatal("static route not found")
	}
	if err := h(nil); err != nil || called != "static" {
		t.Fatalf("matched %q route, err=%v", called, err)
	}
	if len(params) != 0 {
		t.Fatalf("static match retained params: %#v", params)
	}
}

func TestFrozenRouterConcurrentHighCardinalityLookup(t *testing.T) {
	const routeCount = 512
	r := NewRouter()
	for i := 0; i < routeCount; i++ {
		r.Add("GET", fmt.Sprintf("/resources/%d/:id", i), func(Ctx) error { return nil })
	}
	r.Freeze()

	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			params := make([]Param, 0, 1)
			for i := 0; i < routeCount; i++ {
				path := fmt.Sprintf("/resources/%d/%d", (i+offset)%routeCount, i)
				if r.FindBytes([]byte("GET"), []byte(path), &params) == nil {
					t.Errorf("route not found: %s", path)
					return
				}
				if len(params) != 1 || params[0].Key != "id" {
					t.Errorf("route params for %s = %#v", path, params)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}

func TestFrozenRouterMethodsSnapshotIsImmutable(t *testing.T) {
	r := NewRouter()
	r.Add("GET", "/", routerBenchmarkHandler)
	r.Add("PURGE", "/cache", routerBenchmarkHandler)
	r.Freeze()

	want := []string{"GET", "HEAD", "OPTIONS", "PURGE"}
	first := r.Methods()
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("Methods = %#v, want %#v", first, want)
	}
	first[0] = "BROKEN"
	if got := r.Methods(); !reflect.DeepEqual(got, want) {
		t.Fatalf("cached Methods was mutated: %#v", got)
	}
}

func TestRouterAllowedOrderingWithCustomMethod(t *testing.T) {
	r := NewRouter()
	for _, method := range []string{"PURGE", "POST", "GET"} {
		r.Add(method, "/resource/:id", routerBenchmarkHandler)
	}
	r.Freeze()
	if got, want := r.Allowed([]byte("/resource/42")), []string{"GET", "POST", "HEAD", "PURGE", "OPTIONS"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Allowed = %#v, want %#v", got, want)
	}
}

func TestNamedRouteURLStaticParamWildcardAndQuery(t *testing.T) {
	r := NewRouter()
	r.AddNamed("GET", "/", "home", routerBenchmarkHandler)
	r.AddNamed("GET", "/accounts/:account/events/:event", "event", routerBenchmarkHandler)
	r.AddNamed("GET", "/files/*path", "file", routerBenchmarkHandler)

	tests := []struct {
		name   string
		values map[string]string
		want   string
	}{
		{name: "home", values: map[string]string{"b": "two words", "a": "1"}, want: "/?a=1&b=two+words"},
		{name: "event", values: map[string]string{"account": "a/b", "event": "42", "view": "full"}, want: "/accounts/a%2Fb/events/42?view=full"},
		{name: "file", values: map[string]string{"path": "css/my file.css", "download": "1"}, want: "/files/css/my%20file.css?download=1"},
	}
	for _, tt := range tests {
		if got, err := r.URL(tt.name, tt.values); err != nil || got != tt.want {
			t.Errorf("URL(%q) = %q, %v; want %q", tt.name, got, err, tt.want)
		}
	}
}

func TestRouterBoundsDisabledShortcutTables(t *testing.T) {
	r := NewRouter()
	for i := 0; i < 256; i++ {
		r.Add("GET", fmt.Sprintf("/static/%d", i), routerBenchmarkHandler)
		r.Add("GET", fmt.Sprintf("/param-%d/:id", i), routerBenchmarkHandler)
	}
	want := maxLinearRouteShortcuts + 1
	if len(r.staticShortcutGET) != want || len(r.paramGET) != want {
		t.Fatalf("shortcut lengths = static:%d param:%d, want %d", len(r.staticShortcutGET), len(r.paramGET), want)
	}
}
