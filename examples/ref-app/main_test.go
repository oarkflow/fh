package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
	"github.com/oarkflow/fh/ref/debug"
	"github.com/oarkflow/fh/ref/intent"
)

// Tests come in two halves.
//
// Everything that can be proved without infrastructure is proved without it: the
// shape of the compiled plans, the token signer, the password comparison, the rate
// limiter, the configuration rules. These run on every `go test`.
//
// The lifecycle tests need PostgreSQL, because an application whose claim is "real
// transactions, real outbox" cannot be verified against a fake one. They run when
// REF_APP_TEST_DSN names a database and skip, loudly, when it does not.

// ---------------------------------------------------------------------------
// Plan shape — the tests that would have caught this example's worst bug
// ---------------------------------------------------------------------------

// stubDeps builds a Deps good enough to compile plans: capability constructors only
// close over it, so nothing here is ever dialled.
func stubDeps(t *testing.T) *Deps {
	t.Helper()
	cache := kv.NewMemoryStore()
	t.Cleanup(func() { _ = cache.Close() })

	tokens, err := NewTokenSigner([]byte(strings.Repeat("k", 48)), "ref-app-test", time.Hour)
	if err != nil {
		t.Fatalf("token signer: %v", err)
	}
	return &Deps{
		Config:  Config{RateLimit: 5, RateWindow: time.Minute, CatalogCacheTTL: time.Second},
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Cache:   cache,
		Tokens:  tokens,
		Metrics: NewMetrics(),
	}
}

func planNodes(t *testing.T, name string) []string {
	t.Helper()
	engine, err := BuildEngine(stubDeps(t))
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	plan, ok := engine.Plan(intent.Name(name))
	if !ok {
		t.Fatalf("no plan for %q", name)
	}
	summary := debug.InspectPlan(plan)
	encoded, err := json.Marshal(summary)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	var decoded struct {
		Nodes []struct {
			Name string `json:"name"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode plan: %v", err)
	}
	names := make([]string, 0, len(decoded.Nodes))
	for _, node := range decoded.Nodes {
		names = append(names, node.Name)
	}
	return names
}

// TestGovernedIntentsCompileTheirPolicies is the regression test for the mistake
// this example used to make: a policy capability that provided no fact was never
// scheduled, so the deny-dominant authorization it described did not run at all.
//
// A capability reaches a plan only by producing a fact the intent requires. These
// assertions are therefore the real security review: if authorization stops being a
// dependency of an order, it stops appearing here.
func TestGovernedIntentsCompileTheirPolicies(t *testing.T) {
	for _, name := range []string{"order.create", "order.get", "order.list", "order.cancel"} {
		t.Run(name, func(t *testing.T) {
			nodes := planNodes(t, name)
			for _, required := range []string{
				"app.auth",                // there is an identity
				"app.tenant",              // it is scoped to a validated tenant
				"app.policy.orders",       // policy decided, and a deny blocks every effect
				"app.ratelimit.principal", // the account is within its budget
			} {
				if !contains(nodes, required) {
					t.Errorf("%s does not run %s; its plan is %v", name, required, nodes)
				}
			}
		})
	}
}

// TestAnonymousIntentsDoNotRequireIdentity is the other half: the catalogue must
// stay readable without a token, so authentication must *not* be in its plan.
func TestAnonymousIntentsDoNotRequireIdentity(t *testing.T) {
	nodes := planNodes(t, "catalog.list")
	if contains(nodes, "app.auth") {
		t.Errorf("catalog.list requires authentication; its plan is %v", nodes)
	}
	for _, required := range []string{"app.tenant.public", "app.catalog", "app.ratelimit.address"} {
		if !contains(nodes, required) {
			t.Errorf("catalog.list does not run %s; its plan is %v", required, nodes)
		}
	}
	// The authenticated tenant capability must not appear either: it requires a
	// principal, so its presence here would mean the catalogue had stopped being
	// anonymous.
	if contains(nodes, "app.tenant") {
		t.Errorf("catalog.list resolves the account's tenant; its plan is %v", nodes)
	}
}

// TestLoginIsRateLimitedByAddress guards the unauthenticated endpoints: they cannot
// key a limiter on a principal, so they must key it on the address.
func TestLoginIsRateLimitedByAddress(t *testing.T) {
	for _, name := range []string{"auth.login", "auth.register"} {
		nodes := planNodes(t, name)
		if !contains(nodes, "app.ratelimit.address") {
			t.Errorf("%s is not rate limited; its plan is %v", name, nodes)
		}
		if contains(nodes, "app.auth") {
			t.Errorf("%s requires an identity, which nobody has yet; its plan is %v", name, nodes)
		}
	}
}

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

func TestTokenRoundTrip(t *testing.T) {
	signer, err := NewTokenSigner([]byte(strings.Repeat("s", 48)), "ref-app", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	token, expires, err := signer.Issue("usr-1", "acme", []string{"customer"})
	if err != nil {
		t.Fatal(err)
	}
	if !expires.After(time.Now()) {
		t.Fatal("the token expires in the past")
	}
	claims, err := signer.Verify(token)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if claims.Subject != "usr-1" || claims.TenantID != "acme" {
		t.Fatalf("claims did not survive the round trip: %+v", claims)
	}
}

func TestTokenRejectsTamperingAndForgery(t *testing.T) {
	signer, _ := NewTokenSigner([]byte(strings.Repeat("s", 48)), "ref-app", time.Hour)
	token, _, _ := signer.Issue("usr-1", "acme", []string{"customer"})
	parts := strings.Split(token, ".")

	// A payload edited to claim a different subject, keeping the old signature.
	forged := parts[0] + "." + base64url([]byte(`{"sub":"usr-2","iss":"ref-app","exp":9999999999}`)) + "." + parts[2]
	if _, err := signer.Verify(forged); err == nil {
		t.Fatal("a tampered payload verified")
	}

	// The alg=none attack: a header that says there is no signature.
	none := base64url([]byte(`{"alg":"none","typ":"JWT"}`)) + "." + parts[1] + "."
	if _, err := signer.Verify(none); err == nil {
		t.Fatal("an unsigned token verified")
	}

	// Another issuer's key.
	other, _ := NewTokenSigner([]byte(strings.Repeat("x", 48)), "ref-app", time.Hour)
	if _, err := other.Verify(token); err == nil {
		t.Fatal("a token signed with a different key verified")
	}
}

func TestTokenRejectsExpiry(t *testing.T) {
	signer, _ := NewTokenSigner([]byte(strings.Repeat("s", 48)), "ref-app", time.Minute)
	token, _, _ := signer.Issue("usr-1", "acme", nil)
	signer.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if _, err := signer.Verify(token); err == nil {
		t.Fatal("an expired token verified")
	}
}

func TestShortSigningKeyIsRefused(t *testing.T) {
	if _, err := NewTokenSigner([]byte("too-short"), "ref-app", time.Hour); err == nil {
		t.Fatal("a 9-byte signing key was accepted")
	}
}

// ---------------------------------------------------------------------------
// Passwords
// ---------------------------------------------------------------------------

func TestPasswordHashing(t *testing.T) {
	if _, err := hashPassword("short"); err == nil {
		t.Fatal("a 5-character password was accepted")
	}
	hash, err := hashPassword("correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyPassword(hash, "correct horse battery"); err != nil {
		t.Fatalf("the right password did not verify: %v", err)
	}
	if err := verifyPassword(hash, "wrong password here"); err == nil {
		t.Fatal("the wrong password verified")
	}
	// An unknown account: the comparison still happens, against the dummy hash, and
	// the answer is the same failure a wrong password gives.
	if err := verifyPassword("", "anything at all"); err != ErrBadCredentials {
		t.Fatalf("an unknown account produced %v, want ErrBadCredentials", err)
	}
}

// ---------------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------------

func TestFixedWindowLimiterCounts(t *testing.T) {
	cache := kv.NewMemoryStore()
	defer cache.Close()

	for attempt := 1; attempt <= 3; attempt++ {
		used, err := incrementWindow(cache, "rate:test", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if used != attempt {
			t.Fatalf("attempt %d counted as %d", attempt, used)
		}
	}
}

func TestLimiterWindowExpires(t *testing.T) {
	cache := kv.NewMemoryStore()
	defer cache.Close()

	if _, err := incrementWindow(cache, "rate:short", 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	used, err := incrementWindow(cache, "rate:short", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if used != 1 {
		t.Fatalf("the window did not reset: count is %d", used)
	}
}

// ---------------------------------------------------------------------------
// Configuration and roles
// ---------------------------------------------------------------------------

func TestConfigRefusesAnIncompleteEnvironment(t *testing.T) {
	_, err := LoadConfig(func(string) (string, bool) { return "", false })
	if err == nil {
		t.Fatal("an empty environment produced a usable configuration")
	}
	// Every problem at once, not the first one.
	for _, expected := range []string{"DATABASE_URL", "JWT_SECRET"} {
		if !strings.Contains(err.Error(), expected) {
			t.Errorf("the error does not mention %s: %v", expected, err)
		}
	}
}

func TestWebhookNotifierRefusesAHostOutsideItsAllowlist(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := NewNotifier(Config{
		Notifier:     "webhook",
		WebhookURL:   "https://evil.example.com/hook",
		WebhookHosts: []string{"notifications.example.com"},
	}, log)
	if err == nil {
		t.Fatal("a webhook URL outside the allowlist was accepted")
	}
}

func TestRoleModel(t *testing.T) {
	if !roleHas([]string{"customer"}, "order:create") {
		t.Error("a customer cannot place an order")
	}
	if roleHas([]string{"customer"}, "order:list") {
		t.Error("a customer can list the whole tenant's orders")
	}
	if !roleHas([]string{"agent"}, "order:list") {
		t.Error("an agent cannot list orders")
	}
}

func TestEffectWithoutATransactionFails(t *testing.T) {
	effect := &InsertOrderEffect{Source: emptyTxSource{}, ExecutionID: "inv-1"}
	if err := effect.Commit(context.Background()); err == nil {
		t.Fatal("an effect committed without a transaction")
	}
}

type emptyTxSource struct{}

func (emptyTxSource) Tx(string) (*sql.Tx, bool) { return nil, false }

// ---------------------------------------------------------------------------
// Lifecycle — needs PostgreSQL
// ---------------------------------------------------------------------------

type harness struct {
	t      *testing.T
	deps   *Deps
	base   string
	client *http.Client
	token  string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dsn := os.Getenv("REF_APP_TEST_DSN")
	if dsn == "" {
		t.Skip("REF_APP_TEST_DSN is not set; skipping the tests that need PostgreSQL")
	}
	cfg := Config{
		Addr:            "127.0.0.1:0",
		DatabaseURL:     dsn,
		DatabaseDriver:  "pgx",
		MaxOpenConns:    10,
		MaxIdleConns:    2,
		CacheDir:        t.TempDir(),
		QueueDir:        t.TempDir(),
		JWTSecret:       []byte(strings.Repeat("t", 48)),
		JWTIssuer:       "ref-app-test",
		TokenTTL:        time.Hour,
		CatalogCacheTTL: time.Second,
		RateLimit:       1000,
		RateWindow:      time.Minute,
		Notifier:        "stdout",
		OutboxAttempts:  3,
		OutboxInterval:  200 * time.Millisecond,
		Environment:     "test",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	deps, err := OpenDeps(context.Background(), cfg, log)
	if err != nil {
		t.Fatalf("open dependencies: %v", err)
	}
	t.Cleanup(func() { _ = deps.Close() })

	app := fh.NewFast()
	engine, err := BuildEngineWithApp(app, deps)
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	MountRoutes(app, engine, deps)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = app.Serve(listener)
	}()
	t.Cleanup(func() {
		_ = app.ShutdownWithTimeout(3 * time.Second)
		<-served
	})

	return &harness{
		t:      t,
		deps:   deps,
		base:   "http://" + listener.Addr().String(),
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (h *harness) do(method, path string, body any) (int, map[string]any) {
	h.t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, _ := json.Marshal(body)
		reader = strings.NewReader(string(encoded))
	}
	request, err := http.NewRequest(method, h.base+path, reader)
	if err != nil {
		h.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Tenant-ID", "acme")
	if h.token != "" {
		request.Header.Set("Authorization", "Bearer "+h.token)
	}
	response, err := h.client.Do(request)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	var decoded map[string]any
	_ = json.Unmarshal(raw, &decoded)
	return response.StatusCode, decoded
}

// register creates a fresh account and keeps its token.
func (h *harness) register() {
	h.t.Helper()
	email := fmt.Sprintf("ada-%s@example.com", newID(""))
	status, body := h.do(http.MethodPost, "/auth/register", map[string]any{
		"email": email, "name": "Ada", "password": "correct horse battery",
	})
	if status != 201 {
		h.t.Fatalf("register = %d %v", status, body)
	}
	token, _ := body["token"].(string)
	if token == "" {
		h.t.Fatalf("no token in %v", body)
	}
	h.token = token
}

func TestLifecycleOrderIsPlacedAndCancelled(t *testing.T) {
	h := newHarness(t)
	h.register()

	status, catalog := h.do(http.MethodGet, "/catalog", nil)
	if status != 200 {
		t.Fatalf("catalog = %d %v", status, catalog)
	}

	status, created := h.do(http.MethodPost, "/orders", map[string]any{"sku": "CABLE-1", "quantity": 2})
	if status != 201 {
		t.Fatalf("create = %d %v", status, created)
	}
	order, _ := created["order"].(map[string]any)
	orderID, _ := order["order_id"].(string)
	if orderID == "" {
		t.Fatalf("no order id in %v", created)
	}

	// The order, the stock movement and the outbox row committed together.
	var outboxRows int
	if err := h.deps.DB().QueryRow(
		`SELECT COUNT(*) FROM outbox WHERE payload->'data'->>'order_id' = $1`, orderID).Scan(&outboxRows); err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	if outboxRows != 1 {
		t.Errorf("the order committed with %d outbox rows, want 1", outboxRows)
	}

	status, list := h.do(http.MethodGet, "/orders", nil)
	if status != 200 {
		t.Fatalf("list = %d %v", status, list)
	}
	if orders, _ := list["orders"].([]any); len(orders) == 0 {
		t.Fatal("the caller cannot see their own order")
	}

	status, cancelled := h.do(http.MethodPost, "/orders/"+orderID+"/cancel", map[string]any{"reason": "changed my mind"})
	if status != 200 {
		t.Fatalf("cancel = %d %v", status, cancelled)
	}

	// Cancelling twice is a conflict, not a second restock.
	status, _ = h.do(http.MethodPost, "/orders/"+orderID+"/cancel", map[string]any{})
	if status != 409 {
		t.Fatalf("the second cancellation = %d, want 409", status)
	}
}

func TestAnonymousCallerCannotOrder(t *testing.T) {
	h := newHarness(t)
	status, body := h.do(http.MethodPost, "/orders", map[string]any{"sku": "CABLE-1", "quantity": 1})
	if status != 401 {
		t.Fatalf("an anonymous order = %d %v, want 401", status, body)
	}
}

func TestIdempotentCreateReturnsTheSameOrder(t *testing.T) {
	h := newHarness(t)
	h.register()
	key := newID("idem-")

	status, first := h.do(http.MethodPost, "/orders",
		map[string]any{"sku": "CABLE-1", "quantity": 1, "idempotency_key": key})
	if status != 201 {
		t.Fatalf("first create = %d %v", status, first)
	}
	status, second := h.do(http.MethodPost, "/orders",
		map[string]any{"sku": "CABLE-1", "quantity": 1, "idempotency_key": key})
	if status != 201 {
		t.Fatalf("repeat create = %d %v", status, second)
	}
	firstOrder, _ := first["order"].(map[string]any)
	secondOrder, _ := second["order"].(map[string]any)
	if firstOrder["order_id"] != secondOrder["order_id"] {
		t.Fatalf("the repeat created a second order: %v then %v", firstOrder["order_id"], secondOrder["order_id"])
	}
	if second["replayed"] != true {
		t.Errorf("the repeat was not reported as a replay: %v", second)
	}
}

func TestOversellIsRefused(t *testing.T) {
	h := newHarness(t)
	h.register()

	// LAPTOP-X is seeded with five units, so this asks for more than exists.
	status, body := h.do(http.MethodPost, "/orders", map[string]any{"sku": "LAPTOP-X", "quantity": 99})
	if status != 409 {
		t.Fatalf("an oversell = %d %v, want 409", status, body)
	}
}

func TestSuspendedTenantIsRefused(t *testing.T) {
	h := newHarness(t)
	request, _ := http.NewRequest(http.MethodGet, h.base+"/catalog", nil)
	request.Header.Set("X-Tenant-ID", "suspended-co")
	response, err := h.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode == 200 {
		t.Fatal("a suspended tenant was served")
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
