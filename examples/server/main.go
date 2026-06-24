package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/oarkflow/fh"
	tcpguardfh "github.com/oarkflow/fh/contrib/mw/tcpguard"
	"github.com/oarkflow/tcpguard"
	"github.com/oarkflow/tcpguard/bcl"
	_ "modernc.org/sqlite"
)

const hmacSecret = "tcpguard-demo-secret"

func exampleResponsePolicy() tcpguard.ResponseMessagePolicy {
	env := tcpguard.ParseResponseEnvironment(os.Getenv("TCPGUARD_ENV"))
	if env == "" {
		// Keep the example production-like by default. Set
		// TCPGUARD_ENV=development only when you need full local diagnostics.
		env = tcpguard.EnvironmentProduction
	}
	policy := tcpguard.DefaultResponseMessagePolicy(env)
	policy.SupportMessage = "Contact support with the request_id if this legitimate request was blocked."
	// Production output stays compact by default: no details array, no risk score,
	// no support URL, and no duplicated long description. Development/test can be
	// made verbose by setting TCPGUARD_ENV=development.
	if env == tcpguard.EnvironmentProduction {
		policy.DetailLevel = tcpguard.ResponseDetailsNone
		policy.IncludeRiskScore = false
		policy.IncludeActions = false
		policy.IncludeEvidence = false
		policy.IncludeDescription = false
		policy.LogLevel = tcpguard.DecisionLogCompact
		policy.MaxDetails = 0
	} else {
		policy.SupportURL = "https://docs.example.local/security/tcpguard"
	}
	return policy
}

func exampleDecisionRenderer(policy tcpguard.ResponseMessagePolicy) tcpguard.DecisionResponseRenderer {
	// Keep WithResponseRenderer in the example so applications can own a stable
	// response contract, but delegate the body to TCPGuard's compact/safe public
	// renderer instead of serializing raw decisions.
	return tcpguard.PublicDecisionResponseRenderer(policy)
}

func exampleLogPolicy() tcpguard.ResponseMessagePolicy {
	policy := exampleResponsePolicy()
	// Production logs should be debuggable, not noisy: one compact event containing
	// trigger/rule, reason, action summary, request ID, audit/incident
	// references, and safe entity references. Use TCPGUARD_ENV=development for full redacted traces.
	if policy.Environment == tcpguard.EnvironmentProduction {
		policy.LogLevel = tcpguard.DecisionLogCompact
		policy.IncludeActions = false
		policy.IncludeEvidence = false
		policy.IncludeDescription = false
		policy.MaxDetails = 3
	} else {
		policy.LogLevel = tcpguard.DecisionLogFull
		policy.IncludeRuleIDs = true
		policy.IncludeFindingMessages = true
	}
	return policy
}

func logDecision(event string, sec *tcpguard.Context, decision tcpguard.Decision) {
	entry := tcpguard.DecisionLogEntry(sec, decision, exampleLogPolicy())
	entry["event"] = event
	encoded, err := json.Marshal(entry)
	if err != nil {
		log.Printf("tcpguard decision log marshal error: %v", err)
		return
	}
	log.Print(string(encoded))
}

func logHTTPDecision(c *fh.Ctx, result tcpguard.HTTPRequestResult) {
	if !shouldLogHTTPDecision(result) {
		return
	}
	logDecision("tcpguard.http.decision", result.Context, result.Decision)
}

func shouldLogHTTPDecision(result tcpguard.HTTPRequestResult) bool {
	if strings.EqualFold(os.Getenv("TCPGUARD_LOG_ALLOWED"), "true") {
		return true
	}
	if result.Enforced {
		return true
	}
	switch strings.ToLower(string(result.Decision.Severity)) {
	case "", "info":
		return false
	default:
		return true
	}
}

func respondWithDecision(c *fh.Ctx, sec *tcpguard.Context, decision tcpguard.Decision) error {
	logDecision("tcpguard.demo_event.decision", sec, decision)
	response := exampleDecisionRenderer(exampleResponsePolicy())(sec, decision)
	for key, value := range response.Headers {
		c.Set(key, value)
	}
	return c.Status(response.Status).JSON(response.Body)
}

func main() {
	ctx := context.Background()
	dir := exampleDir()
	// TCPGuard v0.0.14 loads datasource and intel-feed paths relative to
	// the process working directory. Pin the demo to its own directory so all
	// README curl scenarios work whether the user runs `go run .` from this
	// folder, from the repository root, or through an IDE/debugger.
	must("enter tcpguard example directory", os.Chdir(dir))
	policyFile := filepath.Join(dir, "tcpguard.bcl")

	bundle, err := bcl.LoadTCPGuardBundleFile(ctx, policyFile)
	must("load tcpguard BCL", err)
	printBundleSummary(bundle)

	store := tcpguard.NewMemoryStore()
	metrics := tcpguard.NewMemoryMetrics()
	accountDB := openAccountDB()
	guard, err := tcpguard.New(
		tcpguard.WithBundle(bundle),
		tcpguard.WithResponseMessagePolicy(exampleResponsePolicy()),
		tcpguard.WithResponseRenderer(exampleDecisionRenderer(exampleResponsePolicy())),
		tcpguard.WithStore(store),
		tcpguard.WithMetrics(metrics),
		tcpguard.WithDataSource(tcpguard.MemoryDataSource{
			SourceID: "demo-cache",
			Values: map[string]any{
				"ban:user:banned-user": map[string]any{"reason": "manual SOC ban"},
			},
		}),
		tcpguard.WithSQLDataSource("account-db", accountDB),
		tcpguard.WithContextBuilder(tcpguard.HTTPContextBuilder{
			TrustedProxyHeaders: true,
			IdentityExtractor:   extractIdentity,
			BusinessExtractor:   extractBusiness,
		}),
		tcpguard.WithHMACSecretProvider(func(sec *tcpguard.Context) []byte {
			if sec.Request.Path == "/api/v1/transfers" || sec.Request.Headers["X-TCPGuard-Signature"] != "" {
				return []byte(hmacSecret)
			}
			return nil
		}),
	)
	must("create tcpguard", err)

	reloadable, err := tcpguard.NewReloadableGuard(ctx, policyFile, bcl.LoadTCPGuardBundleFile,
		tcpguard.WithResponseMessagePolicy(exampleResponsePolicy()),
		tcpguard.WithResponseRenderer(exampleDecisionRenderer(exampleResponsePolicy())),
		tcpguard.WithStore(store),
		tcpguard.WithMetrics(metrics),
		tcpguard.WithDataSource(tcpguard.MemoryDataSource{SourceID: "demo-cache", Values: map[string]any{
			"ban:user:banned-user": map[string]any{"reason": "manual SOC ban"},
		}}),
		tcpguard.WithSQLDataSource("account-db", accountDB),
		tcpguard.WithContextBuilder(tcpguard.HTTPContextBuilder{TrustedProxyHeaders: true, IdentityExtractor: extractIdentity, BusinessExtractor: extractBusiness}),
	)
	must("create reloadable tcpguard", err)

	management := tcpguard.NewManagementServer(reloadable, managementConfig())

	app := fh.New()
	app.Get("/", func(c *fh.Ctx) error {
		return c.JSON(map[string]any{
			"service": "tcpguard fh anomaly-detection demo",
			"try": []string{
				"GET /public",
				"GET /public?debug=true",
				"POST /_demo/auth/fail repeatedly with X-Forwarded-For and different X-User-ID values",
				"POST /api/v1/account/login with X-New-Device: true, X-Previous-Country: US, X-Country: NP",
				"POST /api/v1/functions/reconcile repeatedly",
				"POST /admin/users with X-User-Role: admin and X-Outside-Hours: true",
				"POST /api/v1/reports/export repeatedly or with a large body",
				"POST /api/v1/payments/approve with X-Business-Amount: 1500000",
				"POST /api/v1/transfers using signature from /_demo/sign",
				"External risk datasource is served separately at http://127.0.0.1:18186/risk-source",
				"PUT /api/users/user-2/order/order-9 with X-User-ID: user-1",
			},
		})
	})

	app.Post("/_demo/sign", func(c *fh.Ctx) error {
		method := firstNonEmpty(c.Get("X-Sign-Method"), http.MethodPost)
		path := firstNonEmpty(c.Get("X-Sign-Path"), "/api/v1/transfers")
		body := c.BodyRaw()
		return c.JSON(map[string]any{
			"method":    method,
			"path":      path,
			"signature": sign(method, path, body),
			"nonce":     "nonce-" + strconv.FormatInt(time.Now().UnixNano(), 10),
			"timestamp": time.Now().Unix(),
			"secret":    "server-side only in real deployments",
		})
	})
	app.Post("/_demo/auth/fail", func(c *fh.Ctx) error {
		sec := contextFromFH(c)
		decision := guard.Evaluate(c.Context(), tcpguard.Event{Type: "auth.login_failed", Source: "fh-demo"}, sec)
		return respondWithDecision(c, sec, decision)
	})
	app.Post("/_demo/auth/success", func(c *fh.Ctx) error {
		sec := contextFromFH(c)
		decision := guard.Evaluate(c.Context(), tcpguard.Event{Type: "auth.login_success", Source: "fh-demo"}, sec)
		return respondWithDecision(c, sec, decision)
	})
	app.Get("/_demo/metrics", func(c *fh.Ctx) error { return c.JSON(metrics.Snapshot()) })
	app.Get("/_demo/approvals", func(c *fh.Ctx) error {
		records, err := guard.ListApprovals(c.Context(), "")
		if err != nil {
			return c.Status(http.StatusInternalServerError).JSON(errorBody(err))
		}
		return c.JSON(records)
	})
	app.Get("/_demo/incidents", func(c *fh.Ctx) error {
		incidents, err := store.ListIncidents(c.Context())
		if err != nil {
			return c.Status(http.StatusInternalServerError).JSON(errorBody(err))
		}
		return c.JSON(incidents)
	})
	app.Get("/_demo/audit", func(c *fh.Ctx) error {
		envelopes, err := store.ListAuditEnvelopes(c.Context())
		if err != nil {
			return c.Status(http.StatusInternalServerError).JSON(errorBody(err))
		}
		if err := tcpguard.VerifyAuditChain(envelopes); err != nil {
			return c.Status(http.StatusInternalServerError).JSON(map[string]any{"valid": false, "error": err.Error(), "envelopes": envelopes})
		}
		return c.JSON(map[string]any{"valid": true, "envelopes": envelopes})
	})

	guardMiddleware := tcpguardfh.MiddlewareWithConfig(tcpguardfh.Config{
		Guard:          guard,
		ResponsePolicy: exampleResponsePolicy(),
		HeaderMode:     tcpguardfh.HeaderModeCompact,
		OnDecision:     logHTTPDecision,
	})

	// Route-level middleware is intentional: every business endpoint owns its
	// middleware chain and can add endpoint-specific behavior beside TCPGuard.
	app.Get("/public", guardMiddleware, ok("clean request allowed"))
	app.Get("/geo-restricted", guardMiddleware, ok("geo-restricted request allowed"))
	app.Post("/api/v1/account/login", guardMiddleware, ok("login accepted"))
	app.Post("/api/v1/reports/export", guardMiddleware, ok("export started"))
	app.Post("/api/v1/functions/reconcile", guardMiddleware, ok("function invoked"))
	app.Post("/admin/users", guardMiddleware, ok("admin change accepted"))
	app.Post("/api/v1/payments/approve", guardMiddleware, ok("payment approved"))
	app.Post("/api/v1/transfers", guardMiddleware, ok("signed transfer accepted"))
	app.Put("/api/users/user-2/order/order-9", guardMiddleware, ok("user/order update accepted"))

	appAddr := ":18184"
	adminAddr := "127.0.0.1:18185"
	riskAddr := "127.0.0.1:18186"
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		riskMux := http.NewServeMux()
		riskMux.HandleFunc("/risk-source", riskSourceHandler)
		fmt.Printf("TCPGuard demo risk datasource listening on http://%s\n", riskAddr)
		if err := http.ListenAndServe(riskAddr, riskMux); err != nil {
			log.Printf("risk datasource stopped: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		adminMux := http.NewServeMux()
		for _, path := range []string{"/health", "/reload", "/simulate", "/explain", "/incidents", "/audit", "/audit/verify", "/approvals", "/approvals/approve", "/approvals/reject"} {
			adminMux.Handle(path, management)
		}
		fmt.Printf("TCPGuard admin server listening on http://%s\n", adminAddr)
		if err := http.ListenAndServe(adminAddr, adminMux); err != nil {
			log.Printf("admin server stopped: %v", err)
		}
	}()

	fmt.Printf("TCPGuard fh demo listening on http://127.0.0.1%s\n", appAddr)
	fmt.Println("Open examples/tcpguard-fh-server/README.md for curl scenarios.")
	log.Fatal(app.Listen(appAddr))
}

func riskSourceHandler(w http.ResponseWriter, r *http.Request) {
	body := make([]byte, 0)
	if r.Body != nil {
		body, _ = io.ReadAll(io.LimitReader(r.Body, 1<<20))
	}
	score := 15
	label := "normal"
	if strings.Contains(string(body), "risky-http") {
		score = 88
		label = "elevated"
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"score": score, "label": label})
}

func ok(message string) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		return c.JSON(map[string]any{"ok": true, "message": message})
	}
}

func managementConfig() tcpguard.ManagementServerConfig {
	adminKey := os.Getenv("TCPGUARD_MGMT_API_KEY")
	if adminKey == "" {
		adminKey = "dev-management-key"
	}
	return tcpguard.ManagementServerConfig{
		AuthProvider: tcpguard.StaticAPIKeyAuth{Keys: map[string]tcpguard.ManagementPrincipal{adminKey: {Subject: "fh-admin", Roles: []string{"admin"}}}},
		Authorizer: tcpguard.RoleBasedAuthorizer{RolesByRoute: map[tcpguard.ManagementRoute][]string{
			tcpguard.ManagementRouteHealth:           {"admin"},
			tcpguard.ManagementRouteReload:           {"admin"},
			tcpguard.ManagementRouteSimulate:         {"admin"},
			tcpguard.ManagementRouteExplain:          {"admin"},
			tcpguard.ManagementRouteIncidents:        {"admin"},
			tcpguard.ManagementRouteAudit:            {"admin"},
			tcpguard.ManagementRouteAuditVerify:      {"admin"},
			tcpguard.ManagementRouteApprovals:        {"admin"},
			tcpguard.ManagementRouteApprovalsApprove: {"admin"},
			tcpguard.ManagementRouteApprovalsReject:  {"admin"},
		}},
		MaxBodyByRoute:  map[tcpguard.ManagementRoute]int64{tcpguard.ManagementRouteSimulate: 1 << 20, tcpguard.ManagementRouteExplain: 1 << 20, tcpguard.ManagementRouteApprovalsApprove: 16 << 10, tcpguard.ManagementRouteApprovalsReject: 16 << 10},
		ReadTimeout:     2 * time.Second,
		AllowedCIDRs:    []string{"127.0.0.0/8"},
		PerIPRateLimit:  120,
		RateLimitWindow: time.Minute,
	}
}

func extractIdentity(r *http.Request, sec *tcpguard.Context) {
	sec.Identity.ID = firstNonEmpty(r.Header.Get("X-User-ID"), "anonymous")
	sec.Identity.Role = firstNonEmpty(r.Header.Get("X-User-Role"), defaultRoleForUser(sec.Identity.ID))
	sec.Identity.Roles = []string{sec.Identity.Role}
	sec.Identity.Tenant = firstNonEmpty(r.Header.Get("X-Tenant-ID"), "demo-bank")
	sec.Tenant.ID = sec.Identity.Tenant
	sec.Session.ID = firstNonEmpty(r.Header.Get("X-Session-ID"), "session-"+sec.Identity.ID)
	sec.Session.DeviceID = r.Header.Get("X-Device-ID")
	sec.Session.UserAgent = r.Header.Get("X-Previous-User-Agent")
	sec.Session.PreviousCountry = r.Header.Get("X-Previous-Country")
	sec.Session.NewDevice = boolHeader(r, "X-New-Device")
	sec.Device.ID = sec.Session.DeviceID
	sec.Device.New = sec.Session.NewDevice
	if country := r.Header.Get("X-Country"); country != "" {
		// The demo accepts X-Country as an explicit test fixture so the geo
		// restriction scenario is deterministic without requiring a local GeoIP DB
		// match for documentation-reserved IP ranges.
		sec.Network.GeoFound = true
		sec.Network.Country = country
		sec.Network.CountryCode = country
	}
	sec.Network.ASN = r.Header.Get("X-ASN")
}

func defaultRoleForUser(userID string) string {
	switch userID {
	case "admin-1":
		return "admin"
	case "manager-1", "analyst-1":
		return "manager"
	default:
		return "member"
	}
}

func extractBusiness(r *http.Request, sec *tcpguard.Context) {
	sec.Request.Params = routeParams(r.Method, r.URL.Path)
	sec.Business.Action = firstNonEmpty(r.Header.Get("X-Business-Action"), actionFromPath(r.Method, r.URL.Path))
	sec.Business.Entity = r.Header.Get("X-Business-Entity")
	sec.Business.Workflow = r.Header.Get("X-Workflow")
	sec.Business.Sensitivity = r.Header.Get("X-Sensitivity")
	sec.Business.OutsideHours = boolHeader(r, "X-Outside-Hours") || sec.Business.OutsideHours
	if amount := r.Header.Get("X-Business-Amount"); amount != "" {
		sec.Business.Amount, _ = strconv.ParseFloat(amount, 64)
	}
}

func routeParams(method, path string) map[string]string {
	if method != http.MethodPut && method != http.MethodPatch && method != http.MethodDelete {
		return nil
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 5 && parts[0] == "api" && parts[1] == "users" && parts[3] == "order" {
		return map[string]string{
			"id":       parts[2],
			"order_id": parts[4],
		}
	}
	return nil
}

func actionFromPath(method, path string) string {
	switch {
	case method == http.MethodPost && path == "/api/v1/payments/approve":
		return "payment.approve"
	case method == http.MethodPost && path == "/admin/users":
		return "admin.user.update"
	case method == http.MethodPost && strings.HasPrefix(path, "/api/v1/reports/export"):
		return "report.export"
	case method == http.MethodPost && strings.HasPrefix(path, "/api/v1/functions/"):
		return "function.invoke"
	default:
		return ""
	}
}

func contextFromFH(c *fh.Ctx) *tcpguard.Context {
	r, _ := http.NewRequestWithContext(c.Context(), c.Method(), c.OriginalURL(), nil)
	r.RemoteAddr = c.IP() + ":0"
	for key, values := range c.GetReqHeaders() {
		for _, value := range values {
			r.Header.Add(key, value)
		}
	}
	if r.Header.Get("X-Request-ID") == "" {
		r.Header.Set("X-Request-ID", "demo-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	}
	sec, err := (tcpguard.HTTPContextBuilder{TrustedProxyHeaders: true, IdentityExtractor: extractIdentity, BusinessExtractor: extractBusiness}).BuildHTTP(c.Context(), r)
	if err == nil {
		return sec
	}
	return &tcpguard.Context{Request: tcpguard.RequestContext{ID: r.Header.Get("X-Request-ID"), Method: c.Method(), Path: r.URL.Path, Headers: map[string]string{}, Query: map[string]string{}, UserAgent: r.UserAgent()}, Network: tcpguard.NetworkContext{IP: c.IP()}, Runtime: tcpguard.RuntimeContext{Timestamp: time.Now().UTC()}, Security: map[string]any{}, Rate: map[string]any{}}
}

func openAccountDB() *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	must("open account sqlite", err)
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`
		CREATE TABLE accounts (id TEXT PRIMARY KEY, status TEXT, locked BOOLEAN);
		INSERT INTO accounts (id, status, locked) VALUES
			('manager-1', 'active', false),
			('user-1', 'active', false),
			('locked-user', 'locked', true),
			('risky-http', 'active', false),
			('banned-user', 'active', false);
	`)
	must("seed account sqlite", err)
	return db
}

func boolHeader(r *http.Request, key string) bool {
	switch strings.ToLower(strings.TrimSpace(r.Header.Get(key))) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func sign(method, path string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(hmacSecret))
	_, _ = mac.Write([]byte(method))
	_, _ = mac.Write([]byte("\n"))
	_, _ = mac.Write([]byte(path))
	_, _ = mac.Write([]byte("\n"))
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func httpStatus(effect tcpguard.DecisionEffect) int {
	switch effect {
	case tcpguard.DecisionBlock, tcpguard.DecisionDeny, tcpguard.DecisionRevoke:
		return http.StatusForbidden
	case tcpguard.DecisionThrottle:
		return http.StatusTooManyRequests
	case tcpguard.DecisionChallenge:
		return http.StatusUnauthorized
	default:
		return http.StatusOK
	}
}

func errorBody(err error) map[string]any { return map[string]any{"error": err.Error()} }

func exampleDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("resolve example directory")
	}
	return filepath.Dir(file)
}

func must(label string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", label, err)
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func printBundleSummary(bundle tcpguard.Bundle) {
	fmt.Println()
	fmt.Println("TCPGuard fh policy bundle")
	printKeyValueTable([][2]string{{"name", bundle.Name}, {"version", bundle.Version}, {"mode", string(bundle.Mode)}, {"timezone", bundle.Timezone}, {"rules", strconv.Itoa(len(bundle.Rules))}, {"datasources", strconv.Itoa(len(bundle.DataSources))}, {"lookups", strconv.Itoa(len(bundle.Lookups))}, {"actions", strconv.Itoa(len(bundle.Actions))}, {"detectors", strconv.Itoa(len(bundle.Detectors))}, {"intel_feeds", strconv.Itoa(len(bundle.IntelFeeds))}, {"triggers", strconv.Itoa(len(bundle.DerivedEvents))}})
	printRuleTable(bundle.Rules)
	fmt.Println()
}

func printKeyValueTable(rows [][2]string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "KEY\tVALUE")
	for _, row := range rows {
		fmt.Fprintf(w, "%s\t%s\n", row[0], emptyDash(row[1]))
	}
	_ = w.Flush()
}

func printRuleTable(rules []tcpguard.Rule) {
	if len(rules) == 0 {
		return
	}
	sort.SliceStable(rules, func(i, j int) bool {
		if rules[i].Priority == rules[j].Priority {
			return rules[i].ID < rules[j].ID
		}
		return rules[i].Priority > rules[j].Priority
	})
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "\nRULES")
	fmt.Fprintln(w, "PRIORITY\tSTATUS\tRULE\tTRIGGERS\tPATHS\tACTIONS")
	for _, rule := range rules {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\n", rule.Priority, emptyDash(string(rule.Status)), rule.ID, emptyDash(strings.Join(rule.Triggers, ",")), emptyDash(strings.Join(rule.Scope.Paths, ",")), emptyDash(formatRuleActions(rule.Actions)))
	}
	_ = w.Flush()
}

func formatRuleActions(actions map[tcpguard.Severity][]tcpguard.ActionRef) string {
	if len(actions) == 0 {
		return "-"
	}
	severities := make([]string, 0, len(actions))
	for severity := range actions {
		severities = append(severities, string(severity))
	}
	sort.Strings(severities)
	parts := make([]string, 0, len(severities))
	for _, severity := range severities {
		refs := actions[tcpguard.Severity(severity)]
		ids := make([]string, 0, len(refs))
		for _, ref := range refs {
			ids = append(ids, ref.ID)
		}
		parts = append(parts, severity+":"+strings.Join(ids, "+"))
	}
	return strings.Join(parts, ";")
}

func emptyDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}
