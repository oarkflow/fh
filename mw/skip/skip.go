// Package skip conditionally bypasses middleware.
//
// It is useful when middleware should not run for health checks, public routes,
// CORS preflights, static assets, specific methods, or custom request rules.
//
// Example:
//
//	app.Use(skip.New(csrf.New(), skip.Any(
//		skip.Paths("/health", "/csrf-token"),
//		skip.Methods("GET", "HEAD", "OPTIONS"),
//	)))
package skip

import (
	"path"
	"strings"

	"github.com/oarkflow/fh"
)

// Predicate returns true when the wrapped middleware should be skipped.
type Predicate func(*fh.Ctx) bool

// Logic controls how multiple predicates are combined.
type Logic uint8

const (
	// LogicAny skips when any predicate returns true.
	LogicAny Logic = iota

	// LogicAll skips only when all predicates return true.
	LogicAll
)

// Config configures skip behavior.
type Config struct {
	// Predicate is the main predicate. If it returns true, middleware is skipped.
	Predicate Predicate

	// Predicates are additional predicates combined using Logic.
	Predicates []Predicate

	// Logic controls how Predicate + Predicates are combined.
	// Default is LogicAny.
	Logic Logic

	// RecoverPredicatePanic controls whether predicate panics are recovered.
	// Default true. A panicking predicate is treated as "do not skip".
	RecoverPredicatePanic bool

	// OnPredicatePanic is called after a predicate panic is recovered.
	OnPredicatePanic func(*fh.Ctx, any)
}

// New wraps middleware and skips it when predicate returns true.
func New(middleware fh.HandlerFunc, predicate Predicate) fh.HandlerFunc {
	return NewWithConfig(middleware, Config{
		Predicate:             predicate,
		Logic:                 LogicAny,
		RecoverPredicatePanic: true,
	})
}

// NewWithConfig wraps middleware using a full skip configuration.
func NewWithConfig(middleware fh.HandlerFunc, cfg Config) fh.HandlerFunc {
	if middleware == nil {
		panic("skip: nil middleware")
	}

	predicates := make([]Predicate, 0, 1+len(cfg.Predicates))
	if cfg.Predicate != nil {
		predicates = append(predicates, cfg.Predicate)
	}
	for _, p := range cfg.Predicates {
		if p != nil {
			predicates = append(predicates, p)
		}
	}

	if len(predicates) == 0 {
		return middleware
	}

	recoverPanics := cfg.RecoverPredicatePanic
	if !recoverPanics && cfg.OnPredicatePanic == nil {
		// Explicit false is respected only when there is no handler.
		recoverPanics = false
	} else if cfg.OnPredicatePanic != nil {
		recoverPanics = true
	} else {
		recoverPanics = true
	}

	shouldSkip := func(c *fh.Ctx) bool {
		switch cfg.Logic {
		case LogicAll:
			for _, p := range predicates {
				if !safeEval(c, p, recoverPanics, cfg.OnPredicatePanic) {
					return false
				}
			}
			return true
		default:
			for _, p := range predicates {
				if safeEval(c, p, recoverPanics, cfg.OnPredicatePanic) {
					return true
				}
			}
			return false
		}
	}

	return func(c *fh.Ctx) error {
		if shouldSkip(c) {
			return c.Next()
		}
		return middleware(c)
	}
}

func safeEval(c *fh.Ctx, p Predicate, recoverPanics bool, onPanic func(*fh.Ctx, any)) (ok bool) {
	if p == nil {
		return false
	}

	if !recoverPanics {
		return p(c)
	}

	defer func() {
		if r := recover(); r != nil {
			if onPanic != nil {
				onPanic(c, r)
			}
			ok = false
		}
	}()

	return p(c)
}

// Always always skips middleware.
func Always() Predicate {
	return func(*fh.Ctx) bool { return true }
}

// Never never skips middleware.
func Never() Predicate {
	return func(*fh.Ctx) bool { return false }
}

// Not negates a predicate.
func Not(p Predicate) Predicate {
	if p == nil {
		return Never()
	}
	return func(c *fh.Ctx) bool {
		return !p(c)
	}
}

// Any skips when any predicate returns true.
func Any(predicates ...Predicate) Predicate {
	ps := compact(predicates)
	if len(ps) == 0 {
		return Never()
	}
	return func(c *fh.Ctx) bool {
		for _, p := range ps {
			if p(c) {
				return true
			}
		}
		return false
	}
}

// All skips only when all predicates return true.
func All(predicates ...Predicate) Predicate {
	ps := compact(predicates)
	if len(ps) == 0 {
		return Never()
	}
	return func(c *fh.Ctx) bool {
		for _, p := range ps {
			if !p(c) {
				return false
			}
		}
		return true
	}
}

// Paths skips middleware for exact request paths.
func Paths(paths ...string) Predicate {
	set := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		if p == "" {
			continue
		}
		set[p] = struct{}{}
	}

	if len(set) == 0 {
		return Never()
	}

	return func(c *fh.Ctx) bool {
		_, ok := set[c.Path()]
		return ok
	}
}

// PathCI skips middleware for exact path, case-insensitive.
func PathCI(paths ...string) Predicate {
	set := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		if p == "" {
			continue
		}
		set[strings.ToLower(p)] = struct{}{}
	}

	if len(set) == 0 {
		return Never()
	}

	return func(c *fh.Ctx) bool {
		_, ok := set[strings.ToLower(c.Path())]
		return ok
	}
}

// Prefixes skips middleware when request path has one of the prefixes.
func Prefixes(prefixes ...string) Predicate {
	items := cleanStrings(prefixes)
	if len(items) == 0 {
		return Never()
	}

	return func(c *fh.Ctx) bool {
		p := c.Path()
		for _, prefix := range items {
			if strings.HasPrefix(p, prefix) {
				return true
			}
		}
		return false
	}
}

// Suffixes skips middleware when request path has one of the suffixes.
func Suffixes(suffixes ...string) Predicate {
	items := cleanStrings(suffixes)
	if len(items) == 0 {
		return Never()
	}

	return func(c *fh.Ctx) bool {
		p := c.Path()
		for _, suffix := range items {
			if strings.HasSuffix(p, suffix) {
				return true
			}
		}
		return false
	}
}

// Contains skips middleware when request path contains one of the fragments.
func Contains(parts ...string) Predicate {
	items := cleanStrings(parts)
	if len(items) == 0 {
		return Never()
	}

	return func(c *fh.Ctx) bool {
		p := c.Path()
		for _, part := range items {
			if strings.Contains(p, part) {
				return true
			}
		}
		return false
	}
}

// Globs skips middleware when request path matches one of the glob patterns.
//
// Uses path.Match syntax:
//
//	/static/*
//	/assets/*.css
//	/api/v?/health
func Globs(patterns ...string) Predicate {
	items := cleanStrings(patterns)
	if len(items) == 0 {
		return Never()
	}

	return func(c *fh.Ctx) bool {
		p := c.Path()
		for _, pattern := range items {
			if ok, _ := path.Match(pattern, p); ok {
				return true
			}
		}
		return false
	}
}

// Methods skips middleware for exact HTTP methods.
// Method matching is case-insensitive.
func Methods(methods ...string) Predicate {
	set := make(map[string]struct{}, len(methods))
	for _, m := range methods {
		m = strings.ToUpper(strings.TrimSpace(m))
		if m == "" {
			continue
		}
		set[m] = struct{}{}
	}

	if len(set) == 0 {
		return Never()
	}

	return func(c *fh.Ctx) bool {
		_, ok := set[strings.ToUpper(c.Method())]
		return ok
	}
}

// Preflight skips middleware for CORS preflight requests.
func Preflight() Predicate {
	return All(
		Methods("OPTIONS"),
		HeaderExists("Access-Control-Request-Method"),
	)
}

// HeaderExists skips middleware when a request header exists.
func HeaderExists(name string) Predicate {
	name = strings.TrimSpace(name)
	if name == "" {
		return Never()
	}

	return func(c *fh.Ctx) bool {
		return c.Get(name) != ""
	}
}

// HeaderEquals skips middleware when a request header exactly matches value.
func HeaderEquals(name, value string) Predicate {
	name = strings.TrimSpace(name)
	if name == "" {
		return Never()
	}

	return func(c *fh.Ctx) bool {
		return c.Get(name) == value
	}
}

// HeaderContains skips middleware when a request header contains value.
func HeaderContains(name, value string) Predicate {
	name = strings.TrimSpace(name)
	if name == "" || value == "" {
		return Never()
	}

	return func(c *fh.Ctx) bool {
		return strings.Contains(c.Get(name), value)
	}
}

// HeaderPrefix skips middleware when a request header has prefix.
func HeaderPrefix(name, prefix string) Predicate {
	name = strings.TrimSpace(name)
	if name == "" || prefix == "" {
		return Never()
	}

	return func(c *fh.Ctx) bool {
		return strings.HasPrefix(c.Get(name), prefix)
	}
}

// HeaderSuffix skips middleware when a request header has suffix.
func HeaderSuffix(name, suffix string) Predicate {
	name = strings.TrimSpace(name)
	if name == "" || suffix == "" {
		return Never()
	}

	return func(c *fh.Ctx) bool {
		return strings.HasSuffix(c.Get(name), suffix)
	}
}

// QueryExists skips middleware when a query parameter exists.
func QueryExists(name string) Predicate {
	name = strings.TrimSpace(name)
	if name == "" {
		return Never()
	}

	return func(c *fh.Ctx) bool {
		return c.Query(name) != ""
	}
}

// QueryEquals skips middleware when a query parameter exactly matches value.
func QueryEquals(name, value string) Predicate {
	name = strings.TrimSpace(name)
	if name == "" {
		return Never()
	}

	return func(c *fh.Ctx) bool {
		return c.Query(name) == value
	}
}

// QueryContains skips middleware when a query parameter contains value.
func QueryContains(name, value string) Predicate {
	name = strings.TrimSpace(name)
	if name == "" || value == "" {
		return Never()
	}

	return func(c *fh.Ctx) bool {
		return strings.Contains(c.Query(name), value)
	}
}

// Static skips common static asset paths/extensions.
func Static() Predicate {
	return Any(
		Prefixes("/assets/", "/static/", "/public/", "/favicon"),
		Suffixes(
			".css", ".js", ".mjs", ".map",
			".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico",
			".woff", ".woff2", ".ttf", ".otf", ".eot",
			".txt", ".xml",
		),
	)
}

// Health skips common health/readiness endpoints.
func Health() Predicate {
	return Paths("/health", "/healthz", "/ready", "/readyz", "/live", "/livez")
}

// SafeMethods skips safe/read-only methods.
func SafeMethods() Predicate {
	return Methods("GET", "HEAD", "OPTIONS")
}

// Custom wraps a custom predicate while nil-protecting it.
func Custom(p Predicate) Predicate {
	if p == nil {
		return Never()
	}
	return p
}

func compact(predicates []Predicate) []Predicate {
	if len(predicates) == 0 {
		return nil
	}

	out := make([]Predicate, 0, len(predicates))
	for _, p := range predicates {
		if p != nil {
			out = append(out, p)
		}
	}
	return out
}

func cleanStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))

	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}

	return out
}
