package rewrite

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func testServer(t *testing.T, app *fh.App) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { app.Shutdown() })
	go app.Serve(ln)
	time.Sleep(10 * time.Millisecond)
	return ln.Addr().String()
}

func doRequest(t *testing.T, addr, method, path string, headers map[string]string) (statusCode int, body string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := fmt.Sprintf("%s %s HTTP/1.1\r\n", method, path)
	if _, ok := headers["Host"]; !ok {
		req += "Host: localhost\r\n"
	}
	for k, v := range headers {
		req += k + ": " + v + "\r\n"
	}
	req += "Connection: close\r\n\r\n"

	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	raw := string(resp)
	head, rest, found := strings.Cut(raw, "\r\n\r\n")
	if !found {
		head = raw
	} else {
		body = rest
	}
	lines := strings.Split(head, "\r\n")
	if len(lines) > 0 {
		var proto, status string
		fmt.Sscan(lines[0], &proto, &status)
		fmt.Sscan(status, &statusCode)
	}
	return
}

// TestStaticRewriteRoutesToTarget proves a plain static rule internally
// reroutes the request so the target route's handler (not a 404) answers.
func TestStaticRewriteRoutesToTarget(t *testing.T) {
	app := fh.New()
	app.Use(New(Rule{From: "/legacy", To: "/new"}))
	app.Get("/new", func(c fh.Ctx) error { return c.SendString("new-handler") })
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "GET", "/legacy", nil)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body != "new-handler" {
		t.Fatalf("body = %q, want %q", body, "new-handler")
	}
}

// TestParamCaptureIsExpandedIntoTarget proves a named :param capture from
// From is substituted into To, both as :name and {name} spellings.
func TestParamCaptureIsExpandedIntoTarget(t *testing.T) {
	app := fh.New()
	app.Use(New(Rule{From: "/member/:id", To: "/users/{id}"}))
	app.Get("/users/:id", func(c fh.Ctx) error { return c.SendString("user-" + c.Param("id")) })
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "GET", "/member/42", nil)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body != "user-42" {
		t.Fatalf("body = %q, want %q", body, "user-42")
	}
}

// TestWildcardCaptureIsExpandedIntoTarget proves a terminal *name wildcard
// capture is substituted into the target.
func TestWildcardCaptureIsExpandedIntoTarget(t *testing.T) {
	app := fh.New()
	app.Use(New(Rule{From: "/docs/*path", To: "/tree/*path"}))
	app.Get("/tree/*path", func(c fh.Ctx) error { return c.SendString("tree:" + c.Param("path")) })
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "GET", "/docs/a/b/c", nil)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body != "tree:a/b/c" {
		t.Fatalf("body = %q, want %q", body, "tree:a/b/c")
	}
}

// TestFirstMatchingRuleWinsOverLaterRules proves rule precedence: when two
// rules could both match a request, the first one supplied wins and later
// rules are never consulted.
func TestFirstMatchingRuleWinsOverLaterRules(t *testing.T) {
	app := fh.New()
	app.Use(New(
		Rule{From: "/x", To: "/first"},
		Rule{From: "/x", To: "/second"},
	))
	app.Get("/first", func(c fh.Ctx) error { return c.SendString("first") })
	app.Get("/second", func(c fh.Ctx) error { return c.SendString("second") })
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "GET", "/x", nil)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body != "first" {
		t.Fatalf("body = %q, want %q (first matching rule should win)", body, "first")
	}
}

// TestMethodConstraintOnlyMatchesListedMethods proves a rule with Methods
// set does not apply to a request using a different method — the request
// falls through unrewritten to whatever route matches its original path.
func TestMethodConstraintOnlyMatchesListedMethods(t *testing.T) {
	app := fh.New()
	app.Use(New(Rule{From: "/member/:id", To: "/new/:id", Methods: []string{"GET"}}))
	app.Get("/new/:id", func(c fh.Ctx) error { return c.SendString("rewritten") })
	app.Post("/member/:id", func(c fh.Ctx) error { return c.SendString("original") })
	addr := testServer(t, app)

	if status, body := doRequest(t, addr, "GET", "/member/1", nil); status != fh.StatusOK || body != "rewritten" {
		t.Fatalf("GET: status=%d body=%q, want 200/\"rewritten\"", status, body)
	}
	if status, body := doRequest(t, addr, "POST", "/member/1", nil); status != fh.StatusOK || body != "original" {
		t.Fatalf("POST: status=%d body=%q, want 200/\"original\" (method-restricted rule must not apply)", status, body)
	}
}

// TestHeaderConstraintWildcardRequiresPresenceOnly proves that a Headers
// entry with the value "*" only requires the header to be present, not to
// equal any particular value.
func TestHeaderConstraintWildcardRequiresPresenceOnly(t *testing.T) {
	app := fh.New()
	app.Use(New(Rule{From: "/old", To: "/new", Headers: map[string]string{"X-Rewrite": "*"}}))
	app.Get("/new", func(c fh.Ctx) error { return c.SendString("new") })
	app.Get("/old", func(c fh.Ctx) error { return c.SendString("old") })
	addr := testServer(t, app)

	if status, body := doRequest(t, addr, "GET", "/old", map[string]string{"X-Rewrite": "anything"}); status != fh.StatusOK || body != "new" {
		t.Fatalf("with header: status=%d body=%q, want 200/\"new\"", status, body)
	}
	if status, body := doRequest(t, addr, "GET", "/old", nil); status != fh.StatusOK || body != "old" {
		t.Fatalf("without header: status=%d body=%q, want 200/\"old\" (rule must not apply)", status, body)
	}
}

// TestHeaderConstraintExactValueMustMatch proves a Headers entry with a
// concrete (non-"*") value requires an exact, case-insensitive match.
func TestHeaderConstraintExactValueMustMatch(t *testing.T) {
	app := fh.New()
	app.Use(New(Rule{From: "/old", To: "/new", Headers: map[string]string{"X-Rewrite": "yes"}}))
	app.Get("/new", func(c fh.Ctx) error { return c.SendString("new") })
	app.Get("/old", func(c fh.Ctx) error { return c.SendString("old") })
	addr := testServer(t, app)

	if status, body := doRequest(t, addr, "GET", "/old", map[string]string{"X-Rewrite": "YES"}); status != fh.StatusOK || body != "new" {
		t.Fatalf("case-insensitive match: status=%d body=%q, want 200/\"new\"", status, body)
	}
	if status, body := doRequest(t, addr, "GET", "/old", map[string]string{"X-Rewrite": "no"}); status != fh.StatusOK || body != "old" {
		t.Fatalf("wrong value: status=%d body=%q, want 200/\"old\"", status, body)
	}
}

// TestQueryConstraintWildcardRequiresPresenceOnly mirrors the header
// wildcard test for the Query constraint.
func TestQueryConstraintWildcardRequiresPresenceOnly(t *testing.T) {
	app := fh.New()
	app.Use(New(Rule{From: "/old", To: "/new", Query: map[string]string{"v": "*"}}))
	app.Get("/new", func(c fh.Ctx) error { return c.SendString("new") })
	app.Get("/old", func(c fh.Ctx) error { return c.SendString("old") })
	addr := testServer(t, app)

	if status, body := doRequest(t, addr, "GET", "/old?v=1", nil); status != fh.StatusOK || body != "new" {
		t.Fatalf("with query: status=%d body=%q, want 200/\"new\"", status, body)
	}
	if status, body := doRequest(t, addr, "GET", "/old", nil); status != fh.StatusOK || body != "old" {
		t.Fatalf("without query: status=%d body=%q, want 200/\"old\"", status, body)
	}
}

// TestWhenPredicateGatesTheRule proves an arbitrary When func participates
// in the match decision alongside the structural constraints.
func TestWhenPredicateGatesTheRule(t *testing.T) {
	app := fh.New()
	allow := false
	app.Use(New(Rule{From: "/old", To: "/new", When: func(c fh.Ctx) bool { return allow }}))
	app.Get("/new", func(c fh.Ctx) error { return c.SendString("new") })
	app.Get("/old", func(c fh.Ctx) error { return c.SendString("old") })
	addr := testServer(t, app)

	if status, body := doRequest(t, addr, "GET", "/old", nil); status != fh.StatusOK || body != "old" {
		t.Fatalf("When=false: status=%d body=%q, want 200/\"old\"", status, body)
	}
	allow = true
	if status, body := doRequest(t, addr, "GET", "/old", nil); status != fh.StatusOK || body != "new" {
		t.Fatalf("When=true: status=%d body=%q, want 200/\"new\"", status, body)
	}
}

// TestHostConstraintExactMatch proves a Host constraint compares the
// request Host header against the pattern with the port stripped.
func TestHostConstraintExactMatch(t *testing.T) {
	app := fh.New()
	app.Use(New(Rule{From: "/old", To: "/new", Host: "example.com"}))
	app.Get("/new", func(c fh.Ctx) error { return c.SendString("new") })
	app.Get("/old", func(c fh.Ctx) error { return c.SendString("old") })
	addr := testServer(t, app)

	if status, body := doRequest(t, addr, "GET", "/old", map[string]string{"Host": "example.com:8080"}); status != fh.StatusOK || body != "new" {
		t.Fatalf("matching host (with port): status=%d body=%q, want 200/\"new\"", status, body)
	}
	if status, body := doRequest(t, addr, "GET", "/old", map[string]string{"Host": "other.com"}); status != fh.StatusOK || body != "old" {
		t.Fatalf("non-matching host: status=%d body=%q, want 200/\"old\"", status, body)
	}
}

// TestHostConstraintWildcardSubdomain proves *.example.com matches any
// subdomain but never the bare apex domain itself.
func TestHostConstraintWildcardSubdomain(t *testing.T) {
	if !matchHost("*.example.com", "api.example.com") {
		t.Error("expected api.example.com to match *.example.com")
	}
	if matchHost("*.example.com", "example.com") {
		t.Error("expected the bare apex domain to NOT match *.example.com")
	}
	if matchHost("*.example.com", "evil-example.com") {
		t.Error("expected a suffix-only lookalike (no dot boundary) to NOT match")
	}
	if !matchHost("*.example.com", "API.EXAMPLE.COM") {
		t.Error("expected wildcard host matching to be case-insensitive")
	}
}

// TestWithoutPortStripsPortButKeepsBareHost is a direct unit test of the
// port-stripping helper, including the IPv6-bracket edge case.
func TestWithoutPortStripsPortButKeepsBareHost(t *testing.T) {
	cases := map[string]string{
		"example.com:8080": "example.com",
		"example.com":      "example.com",
		"[::1]:8080":       "::1",
		"[::1]":            "::1",
	}
	for in, want := range cases {
		if got := withoutPort(in); got != want {
			t.Errorf("withoutPort(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestNoRuleMatchesFallsThroughToNext proves an unmatched request proceeds
// normally rather than erroring or hanging.
func TestNoRuleMatchesFallsThroughToNext(t *testing.T) {
	app := fh.New()
	app.Use(New(Rule{From: "/only-this", To: "/elsewhere"}))
	app.Get("/other", func(c fh.Ctx) error { return c.SendString("other") })
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "GET", "/other", nil)
	if status != fh.StatusOK || body != "other" {
		t.Fatalf("status=%d body=%q, want 200/\"other\"", status, body)
	}
}

// TestPreserveQueryAppendsOriginalQueryString proves the default
// PreserveQuery: true behavior (via New) carries the caller's original
// query string onto a target that does not already specify one.
func TestPreserveQueryAppendsOriginalQueryString(t *testing.T) {
	app := fh.New()
	app.Use(New(Rule{From: "/old", To: "/new"}))
	app.Get("/new", func(c fh.Ctx) error { return c.SendString("q=" + c.Query("q")) })
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "GET", "/old?q=hello", nil)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body != "q=hello" {
		t.Fatalf("body = %q, want %q (original query string should be preserved)", body, "q=hello")
	}
}

// TestPreserveQueryFalseDropsOriginalQueryString proves WithConfig lets a
// caller opt out of query preservation.
func TestPreserveQueryFalseDropsOriginalQueryString(t *testing.T) {
	app := fh.New()
	app.Use(WithConfig(Config{Rules: []Rule{{From: "/old", To: "/new"}}, PreserveQuery: false}))
	app.Get("/new", func(c fh.Ctx) error { return c.SendString("q=" + c.Query("q")) })
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "GET", "/old?q=hello", nil)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body != "q=" {
		t.Fatalf("body = %q, want %q (query string should be dropped)", body, "q=")
	}
}

// TestPreserveQueryDoesNotDoubleUpWhenTargetHasItsOwnQuery proves that when
// To already contains a "?", the original request's query string is not
// appended on top of it.
func TestPreserveQueryDoesNotDoubleUpWhenTargetHasItsOwnQuery(t *testing.T) {
	app := fh.New()
	app.Use(New(Rule{From: "/old", To: "/new?fixed=1"}))
	app.Get("/new", func(c fh.Ctx) error { return c.SendString("fixed=" + c.Query("fixed") + " q=" + c.Query("q")) })
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "GET", "/old?q=hello", nil)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body != "fixed=1 q=" {
		t.Fatalf("body = %q, want %q (target's own query string should win, not be appended to)", body, "fixed=1 q=")
	}
}

// TestNextHookBypassesAllRules proves the Config.Next escape hatch skips
// rewriting entirely for requests it opts out.
func TestNextHookBypassesAllRules(t *testing.T) {
	app := fh.New()
	app.Use(WithConfig(Config{
		Rules: []Rule{{From: "/old", To: "/new"}},
		Next:  func(c fh.Ctx) bool { return c.Query("skip") == "1" },
	}))
	app.Get("/new", func(c fh.Ctx) error { return c.SendString("new") })
	app.Get("/old", func(c fh.Ctx) error { return c.SendString("old") })
	addr := testServer(t, app)

	if status, body := doRequest(t, addr, "GET", "/old?skip=1", nil); status != fh.StatusOK || body != "old" {
		t.Fatalf("skip=1: status=%d body=%q, want 200/\"old\"", status, body)
	}
	if status, body := doRequest(t, addr, "GET", "/old", nil); status != fh.StatusOK || body != "new" {
		t.Fatalf("no skip: status=%d body=%q, want 200/\"new\"", status, body)
	}
}

// TestCompileRuleWithEmptyFromPanics and TestCompileRuleWithRelativeToPanics
// prove the compile-time invariant guard: malformed rules are refused at
// startup (fail fast) instead of silently misrouting or panicking mid
// request.
func TestCompileRuleWithEmptyFromPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected New to panic on a rule with an empty From")
		}
	}()
	New(Rule{From: "", To: "/x"})
}

func TestCompileRuleWithRelativeToPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected New to panic on a rule whose To is not an absolute path")
		}
	}()
	New(Rule{From: "/x", To: "relative"})
}

// TestConstraintsMatchIsCaseInsensitiveOnMethod is a direct unit test that
// the compiled method set is matched by exact string set membership after
// upper-casing (constraintsMatch itself does not re-case c.Method(), so the
// caller-supplied methods list is normalized once at compile time).
func TestConstraintsMatchIsCaseInsensitiveOnMethod(t *testing.T) {
	app := fh.New()
	app.Use(New(Rule{From: "/old", To: "/new", Methods: []string{"get", " Post "}}))
	app.Get("/new", func(c fh.Ctx) error { return c.SendString("new") })
	app.Get("/old", func(c fh.Ctx) error { return c.SendString("old") })
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "GET", "/old", nil)
	if status != fh.StatusOK || body != "new" {
		t.Fatalf("status=%d body=%q, want 200/\"new\" (lowercase 'get' in Methods should still match GET)", status, body)
	}
}

// TestConcurrentRewritesDoNotRace exercises the compiled rule set (built
// once at New() time) under concurrent HTTP load, guarding against any
// future change that introduces mutable shared matching state.
func TestConcurrentRewritesDoNotRace(t *testing.T) {
	app := fh.New()
	app.Use(New(
		Rule{From: "/member/:id", To: "/users/:id"},
		Rule{From: "/legacy", To: "/modern"},
	))
	app.Get("/users/:id", func(c fh.Ctx) error { return c.SendString("user-" + c.Param("id")) })
	app.Get("/modern", func(c fh.Ctx) error { return c.SendString("modern") })
	addr := testServer(t, app)

	const n = 30
	var wg sync.WaitGroup
	wg.Add(n * 2)
	errs := make(chan string, n*2)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if status, body := doRequest(t, addr, "GET", "/member/7", nil); status != fh.StatusOK || body != "user-7" {
				errs <- fmt.Sprintf("member rewrite: status=%d body=%q", status, body)
			}
		}()
		go func() {
			defer wg.Done()
			if status, body := doRequest(t, addr, "GET", "/legacy", nil); status != fh.StatusOK || body != "modern" {
				errs <- fmt.Sprintf("legacy rewrite: status=%d body=%q", status, body)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}
