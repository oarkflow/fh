package skip_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/skip"
)

// marker is a stand-in "security" middleware whose execution we can observe.
// It is used throughout to assert whether skip actually bypassed the wrapped
// middleware or let it run.
func marker(ran *bool) fh.HandlerFunc {
	return func(c fh.Ctx) error {
		*ran = true
		return c.Next()
	}
}

func mustDo(t *testing.T, app *fh.App, req *http.Request) *http.Response {
	t.Helper()
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func mustGet(t *testing.T, app *fh.App, path string) int {
	t.Helper()
	return mustDo(t, app, httptest.NewRequest("GET", path, nil)).StatusCode
}

func TestNewWithConfigNilMiddlewarePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for nil middleware")
		}
	}()
	skip.NewWithConfig(nil, skip.Config{})
}

func TestNewWithConfigNoPredicatesAlwaysRunsMiddleware(t *testing.T) {
	var ran bool
	app := fh.New()
	app.Use(skip.NewWithConfig(marker(&ran), skip.Config{}))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

	if code := mustGet(t, app, "/x"); code != fh.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if !ran {
		t.Fatal("expected wrapped middleware to run when no predicates are configured")
	}
}

func TestSecurityMiddlewareIsBypassedWhenPredicateMatches(t *testing.T) {
	var ran bool
	app := fh.New()
	app.Use(skip.New(marker(&ran), skip.Health()))
	app.Get("/healthz", func(c fh.Ctx) error { return c.SendString("ok") })

	if code := mustGet(t, app, "/healthz"); code != fh.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if ran {
		t.Fatal("expected the wrapped middleware to be skipped for a health path")
	}
}

func TestSecurityMiddlewareRunsWhenPredicateDoesNotMatch(t *testing.T) {
	var ran bool
	app := fh.New()
	app.Use(skip.New(marker(&ran), skip.Health()))
	app.Get("/api/data", func(c fh.Ctx) error { return c.SendString("ok") })

	if code := mustGet(t, app, "/api/data"); code != fh.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if !ran {
		t.Fatal("expected the wrapped middleware to run for a non-matching path")
	}
}

func TestLogicAnySkipsWhenAnyPredicateMatches(t *testing.T) {
	var ran bool
	app := fh.New()
	app.Use(skip.NewWithConfig(marker(&ran), skip.Config{
		Predicates:            []skip.Predicate{skip.Never(), skip.Always()},
		Logic:                 skip.LogicAny,
		RecoverPredicatePanic: true,
	}))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app, "/x")
	if ran {
		t.Fatal("expected skip under LogicAny when one predicate matches")
	}
}

func TestLogicAllRequiresEveryPredicateToMatch(t *testing.T) {
	var ranPartial bool
	app := fh.New()
	app.Use(skip.NewWithConfig(marker(&ranPartial), skip.Config{
		Predicates:            []skip.Predicate{skip.Always(), skip.Never()},
		Logic:                 skip.LogicAll,
		RecoverPredicatePanic: true,
	}))
	app.Get("/partial", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app, "/partial")
	if !ranPartial {
		t.Fatal("expected middleware to run under LogicAll when not all predicates match")
	}

	var ranFull bool
	app2 := fh.New()
	app2.Use(skip.NewWithConfig(marker(&ranFull), skip.Config{
		Predicates:            []skip.Predicate{skip.Always(), skip.Always()},
		Logic:                 skip.LogicAll,
		RecoverPredicatePanic: true,
	}))
	app2.Get("/full", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app2, "/full")
	if ranFull {
		t.Fatal("expected middleware to be skipped under LogicAll when all predicates match")
	}
}

// TestPredicatePanicDefaultTreatsAsDoNotSkip guards the fail-closed contract
// documented on Config.RecoverPredicatePanic: a panicking predicate must
// never silently disable the wrapped (security) middleware. New() always
// requests recovery, so the wrapped middleware must still run.
func TestPredicatePanicDefaultTreatsAsDoNotSkip(t *testing.T) {
	var ran bool
	app := fh.New()
	panicky := func(fh.Ctx) bool { panic("boom") }
	app.Use(skip.New(marker(&ran), panicky))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

	if code := mustGet(t, app, "/x"); code != fh.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if !ran {
		t.Fatal("expected wrapped middleware to run when the predicate panics (fail closed)")
	}
}

func TestOnPredicatePanicInvokedAndTreatedAsDoNotSkip(t *testing.T) {
	var ran bool
	var captured any
	app := fh.New()
	app.Use(skip.NewWithConfig(marker(&ran), skip.Config{
		Predicate:        func(fh.Ctx) bool { panic("boom") },
		OnPredicatePanic: func(c fh.Ctx, r any) { captured = r },
	}))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app, "/x")

	if !ran {
		t.Fatal("expected wrapped middleware to run after a recovered predicate panic")
	}
	if captured != "boom" {
		t.Fatalf("expected panic value to be forwarded to OnPredicatePanic, got %v", captured)
	}
}

// TestNewWithConfigZeroValueConfigDoesNotRecoverPanics is a characterization
// test for the actual (documented-by-code, not by comment) semantics:
// recovery happens when RecoverPredicatePanic is true OR OnPredicatePanic is
// set. A bare Config{Predicate: p} built directly (bypassing New()) does
// neither, so the panic propagates out of the returned handler uncaught.
// This locks in current behavior so a future change to the recovery
// heuristic is a deliberate, visible decision rather than an accident.
func TestNewWithConfigZeroValueConfigDoesNotRecoverPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected the predicate panic to propagate for a zero-value Config")
		}
	}()
	h := skip.NewWithConfig(func(fh.Ctx) error { return nil }, skip.Config{
		Predicate: func(fh.Ctx) bool { panic("boom") },
	})
	_ = h(nil)
}

func TestAlwaysNeverNot(t *testing.T) {
	if !skip.Always()(nil) {
		t.Fatal("Always should return true")
	}
	if skip.Never()(nil) {
		t.Fatal("Never should return false")
	}
	if !skip.Not(skip.Never())(nil) {
		t.Fatal("Not(Never) should return true")
	}
	if skip.Not(nil)(nil) {
		t.Fatal("Not(nil) should behave like Never")
	}
}

func TestAnyAllEmptyOrNilPredicatesNeverMatch(t *testing.T) {
	if skip.Any()(nil) {
		t.Fatal("Any() with no predicates should never match")
	}
	if skip.All()(nil) {
		t.Fatal("All() with no predicates should never match")
	}
	if skip.Any(nil, nil)(nil) {
		t.Fatal("Any() with only nil predicates should never match")
	}
}

func TestPaths(t *testing.T) {
	// app.Test shuts the app down after each call, so each request that
	// needs to complete gets its own app instance.
	var ranHealth bool
	app := fh.New()
	app.Use(skip.New(marker(&ranHealth), skip.Paths("/health", "/csrf-token")))
	app.Get("/health", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app, "/health")
	if ranHealth {
		t.Fatal("expected /health to be skipped")
	}

	var ranOther bool
	app2 := fh.New()
	app2.Use(skip.New(marker(&ranOther), skip.Paths("/health", "/csrf-token")))
	app2.Get("/other", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app2, "/other")
	if !ranOther {
		t.Fatal("expected /other to run the middleware")
	}
}

func TestPathCIIsCaseInsensitive(t *testing.T) {
	var ran bool
	app := fh.New()
	app.Use(skip.New(marker(&ran), skip.PathCI("/Health")))
	app.Get("/health", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app, "/health")
	if ran {
		t.Fatal("expected case-insensitive path match to skip")
	}
}

func TestPrefixesSuffixesContains(t *testing.T) {
	var ranPrefix, ranSuffix, ranContains bool

	app := fh.New()
	app.Use(skip.New(marker(&ranPrefix), skip.Prefixes("/static/")))
	app.Get("/static/app.js", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app, "/static/app.js")
	if ranPrefix {
		t.Fatal("expected prefix match to skip")
	}

	app2 := fh.New()
	app2.Use(skip.New(marker(&ranSuffix), skip.Suffixes(".css")))
	app2.Get("/assets/app.css", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app2, "/assets/app.css")
	if ranSuffix {
		t.Fatal("expected suffix match to skip")
	}

	app3 := fh.New()
	app3.Use(skip.New(marker(&ranContains), skip.Contains("internal")))
	app3.Get("/api/internal/debug", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app3, "/api/internal/debug")
	if ranContains {
		t.Fatal("expected contains match to skip")
	}
}

func TestGlobs(t *testing.T) {
	var ran bool
	app := fh.New()
	app.Use(skip.New(marker(&ran), skip.Globs("/assets/*.css")))
	app.Get("/assets/app.css", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app, "/assets/app.css")
	if ran {
		t.Fatal("expected glob match to skip")
	}
}

// TestGlobsInvalidPatternDoesNotPanic guards against path.Match's syntax
// errors (e.g. an unterminated character class) crashing the request; an
// invalid pattern must be treated as "does not match", not panic.
func TestGlobsInvalidPatternDoesNotPanic(t *testing.T) {
	var ran bool
	app := fh.New()
	app.Use(skip.New(marker(&ran), skip.Globs("[")))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })
	code := mustGet(t, app, "/x")
	if code != fh.StatusOK {
		t.Fatalf("expected malformed glob pattern to not fail the request, got %d", code)
	}
	if !ran {
		t.Fatal("expected middleware to run since a malformed pattern never matches")
	}
}

func TestMethodsIsCaseInsensitive(t *testing.T) {
	var ran bool
	app := fh.New()
	app.Use(skip.New(marker(&ran), skip.Methods("get")))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app, "/x")
	if ran {
		t.Fatal("expected case-insensitive method match to skip")
	}
}

func TestPreflightRequiresBothMethodAndHeader(t *testing.T) {
	var ranPreflight, ranPlainOptions bool

	app := fh.New()
	app.Use(skip.New(marker(&ranPreflight), skip.Preflight()))
	app.Options("/x", func(c fh.Ctx) error { return c.SendString("ok") })
	req := httptest.NewRequest("OPTIONS", "/x", nil)
	req.Header.Set("Access-Control-Request-Method", "POST")
	mustDo(t, app, req)
	if ranPreflight {
		t.Fatal("expected a real CORS preflight to be skipped")
	}

	app2 := fh.New()
	app2.Use(skip.New(marker(&ranPlainOptions), skip.Preflight()))
	app2.Options("/y", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app2, "/y")
	if !ranPlainOptions {
		t.Fatal("expected a plain OPTIONS request (no preflight header) to run the middleware")
	}
}

func TestHeaderPredicates(t *testing.T) {
	var ran bool
	app := fh.New()
	app.Use(skip.New(marker(&ran), skip.HeaderEquals("X-Skip", "yes")))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set("X-Skip", "yes")
	mustDo(t, app, req)
	if ran {
		t.Fatal("expected header-equals predicate to skip")
	}
}

func TestQueryPredicates(t *testing.T) {
	var ran bool
	app := fh.New()
	app.Use(skip.New(marker(&ran), skip.QueryEquals("skip", "1")))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app, "/x?skip=1")
	if ran {
		t.Fatal("expected query-equals predicate to skip")
	}
}

func TestStaticAndSafeMethods(t *testing.T) {
	var ranStatic, ranSafe bool

	app := fh.New()
	app.Use(skip.New(marker(&ranStatic), skip.Static()))
	app.Get("/app.css", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app, "/app.css")
	if ranStatic {
		t.Fatal("expected static asset path to skip")
	}

	app2 := fh.New()
	app2.Use(skip.New(marker(&ranSafe), skip.SafeMethods()))
	app2.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })
	mustGet(t, app2, "/x")
	if ranSafe {
		t.Fatal("expected safe method GET to skip")
	}
}

func TestCustomNilProtect(t *testing.T) {
	if skip.Custom(nil)(nil) {
		t.Fatal("Custom(nil) should behave like Never")
	}
}
