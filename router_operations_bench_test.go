package fh

import (
	"fmt"
	"testing"
)

var routerBenchmarkHandler = func(Ctx) error { return nil }

// BenchmarkRouterOperations exercises the router API independently of request
// parsing and handler execution. Keep setup outside the timed regions so the
// results describe the operation named by each sub-benchmark.
func BenchmarkRouterOperations(b *testing.B) {
	const routeCount = 256
	methods := []string{
		"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE",
		"CONNECT", "OPTIONS", "TRACE", "QUERY",
	}

	b.Run("Method", func(b *testing.B) {
		for _, method := range methods {
			b.Run(method, func(b *testing.B) {
				r := NewRouter()
				for i := 0; i < routeCount; i++ {
					r.Add(method, fmt.Sprintf("/method/%d", i), routerBenchmarkHandler)
				}
				r.Freeze()
				methodBytes := []byte(method)
				path := []byte("/method/128")
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if r.FindBytes(methodBytes, path, nil) == nil {
						b.Fatal("route not found")
					}
				}
			})
		}
	})

	r := NewRouter()
	for i := 0; i < routeCount; i++ {
		r.Add("GET", fmt.Sprintf("/static/%d", i), routerBenchmarkHandler)
		r.Add("GET", fmt.Sprintf("/param-%d/:id", i), routerBenchmarkHandler)
	}
	r.Add("GET", "/teams/:team/users/:id", routerBenchmarkHandler)
	r.Add("GET", "/assets/*path", routerBenchmarkHandler)
	r.Add("HEAD", "/health", routerBenchmarkHandler)
	r.Add("PURGE", "/cache/:key", routerBenchmarkHandler)
	r.AddNamed("POST", "/accounts/:account/events/:event", "events.show", routerBenchmarkHandler)
	r.UnsafeParams = true
	r.Freeze()

	lookup := func(name, method, path string, wantParams int) {
		b.Run(name, func(b *testing.B) {
			methodBytes := []byte(method)
			pathBytes := []byte(path)
			params := make([]Param, 0, 2)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h := r.FindBytes(methodBytes, pathBytes, &params)
				if h == nil || len(params) != wantParams {
					b.Fatalf("match = %v, params = %#v", h != nil, params)
				}
			}
		})
	}

	lookup("StaticHit", "GET", "/static/128", 0)
	lookup("ParamHit", "GET", "/param-128/42", 1)
	lookup("MultiParamHit", "GET", "/teams/core/users/42", 2)
	lookup("WildcardHit", "GET", "/assets/css/app.css", 1)
	lookup("HEADExplicit", "HEAD", "/health", 0)
	lookup("HEADFallback", "HEAD", "/param-128/42", 1)
	lookup("CustomMethod", "PURGE", "/cache/session-42", 1)

	b.Run("StaticMiss", func(b *testing.B) {
		method, path := []byte("GET"), []byte("/does-not-exist")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if r.FindBytes(method, path, nil) != nil {
				b.Fatal("unexpected match")
			}
		}
	})

	b.Run("ParamMiss", func(b *testing.B) {
		method, path := []byte("GET"), []byte("/param-128/42/extra")
		params := make([]Param, 0, 2)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if r.FindBytes(method, path, &params) != nil {
				b.Fatal("unexpected match")
			}
		}
	})

	b.Run("Allowed", func(b *testing.B) {
		path := []byte("/param-128/42")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if methods := r.Allowed(path); len(methods) == 0 {
				b.Fatal("no allowed methods")
			}
		}
	})

	b.Run("Methods", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if methods := r.Methods(); len(methods) == 0 {
				b.Fatal("no methods")
			}
		}
	})

	b.Run("NamedURL", func(b *testing.B) {
		values := map[string]string{"account": "acme", "event": "42", "view": "full"}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if path, err := r.URL("events.show", values); err != nil || path == "" {
				b.Fatalf("URL = %q, %v", path, err)
			}
		}
	})

	b.Run("CompiledPattern", func(b *testing.B) {
		pattern := CompileRoutePattern("/teams/:team/users/:id")
		params := make([]Param, 0, 2)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if !pattern.Match("/teams/core/users/42", &params) {
				b.Fatal("pattern did not match")
			}
		}
	})

	b.Run("Register256", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			router := NewRouter()
			for route := 0; route < routeCount; route++ {
				router.Add("GET", fmt.Sprintf("/register/%d/:id", route), routerBenchmarkHandler)
			}
			router.Freeze()
		}
	})
}
