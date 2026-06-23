package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/oarkflow/authz"
	"github.com/oarkflow/fh"
	middleware "github.com/oarkflow/fh/contrib/mw/authz"
	"github.com/oarkflow/fh/contrib/mw/authorizer"
	"github.com/oarkflow/fh/mw/bodylimit"
	"github.com/oarkflow/fh/mw/compliance"
	"github.com/oarkflow/fh/mw/cors"
	"github.com/oarkflow/fh/mw/csrf"
	"github.com/oarkflow/fh/mw/logger"
	"github.com/oarkflow/fh/mw/ratelimiter"
	"github.com/oarkflow/fh/mw/recover"
	"github.com/oarkflow/fh/mw/replay"
	"github.com/oarkflow/fh/mw/requestid"
	"github.com/oarkflow/fh/mw/security"
	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/mw/signature"
	"github.com/oarkflow/fh/mw/tenant"
	"github.com/oarkflow/fh/mw/timeout"
	"github.com/oarkflow/fh/pkg/websocket"
	"github.com/oarkflow/tcpguard"
	"github.com/oarkflow/tcpguard/bcl"
	_ "modernc.org/sqlite"
)

const hmacSecret = "tcpguard-demo-secret"

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	flag.Parse()

	// ── 1. TCPGuard Setup ─────────────────────────────────────────────────
	ctx := context.Background()
	dir := exampleDir()

	bundle, err := bcl.LoadTCPGuardBundleFile(ctx, filepath.Join(dir, "tcpguard.bcl"))
	must("load tcpguard BCL", err)
	printBundleSummary(bundle)

	store := tcpguard.NewMemoryStore()
	accountDB := openAccountDB()
	guard, err := tcpguard.New(
		tcpguard.WithBundle(bundle),
		tcpguard.WithStore(store),
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
	reloadable, err := tcpguard.NewReloadableGuard(ctx, filepath.Join(dir, "tcpguard.bcl"), bcl.LoadTCPGuardBundleFile,
		tcpguard.WithStore(store),
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
	must("create reloadable tcpguard", err)
	adminKey := os.Getenv("TCPGUARD_MGMT_API_KEY")
	if adminKey == "" {
		adminKey = "dev-management-key"
	}
	management := tcpguard.NewManagementServer(reloadable, tcpguard.ManagementServerConfig{
		AuthProvider: tcpguard.StaticAPIKeyAuth{
			Keys: map[string]tcpguard.ManagementPrincipal{
				adminKey: {Subject: "fiber-admin", Roles: []string{"admin"}},
			},
		},
		Authorizer: tcpguard.RoleBasedAuthorizer{
			RolesByRoute: map[tcpguard.ManagementRoute][]string{
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
			},
		},
		MaxBodyByRoute: map[tcpguard.ManagementRoute]int64{
			tcpguard.ManagementRouteSimulate:         1 << 20,
			tcpguard.ManagementRouteExplain:          1 << 20,
			tcpguard.ManagementRouteApprovalsApprove: 16 << 10,
			tcpguard.ManagementRouteApprovalsReject:  16 << 10,
		},
		ReadTimeout:     2 * time.Second,
		AllowedCIDRs:    []string{"127.0.0.0/8"},
		PerIPRateLimit:  120,
		RateLimitWindow: time.Minute,
	})

	// ── 2. AuthZ Engine (oarkflow) ────────────────────────────────────────
	authzEngine, err := middleware.LoadEngineFromAuthzFile("config.authz")
	if err != nil {
		log.Fatalf("failed to load authz config: %v", err)
	}

	// ── 3. Session Store ───────────────────────────────────────────────────
	sessions := session.NewMemoryStore(10 * time.Minute)
	defer sessions.StopGC()
	sessionManager := session.NewSessionManager(sessions,
		session.SessionCookieName("fh_session"),
		session.SessionSecret([]byte("change-this-secret-in-production")),
		session.SessionMaxAge(24*time.Hour),
		session.SessionHTTPOnly(true),
		session.SessionSameSite(fh.SameSiteLax),
	)

	// ── 4. App with Enterprise Compliance ──────────────────────────────────
	app := fh.New(
		fh.WithCompliance(fh.ComplianceConfig{
			Enabled:         true,
			Profile:         fh.ComplianceEnterprise,
			ExposeEndpoints: true,
			EndpointPrefix:  "/_fh",
		}),
		fh.WithReliability(fh.ReliabilityConfig{
			Enabled: true, DataDir: ".fh-data",
			JournalEnabled: true, IdempotencyEnabled: true, QueueEnabled: true, QueueWorkers: 2,
		}),
	)

	// ── 5. Global Middleware Stack ─────────────────────────────────────────
	app.Use(recover.New())
	app.Use(requestid.New())
	app.Use(security.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: []string{"http://localhost:5173"},
		AllowMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders: []string{
			"Content-Type", "Authorization", "X-Tenant-ID", "X-Subject-ID",
			"X-Roles", "X-Clearance", "X-Request-ID", "Idempotency-Key",
			"X-Forwarded-For", "X-User-ID", "X-User-Role", "X-New-Device",
			"X-Device-ID", "X-Country", "X-Previous-Country", "X-API-Key",
			"X-Business-Action", "X-Business-Amount", "X-Outside-Hours",
			"X-TCPGuard-Signature", "X-TCPGuard-Nonce", "X-TCPGuard-Timestamp",
			"X-Approver", "X-Reason", "X-Session-ID",
		},
	}))
	app.Use(logger.New(logger.Config{FormatName: "json"}))
	app.Use(bodylimit.New(1 << 20))
	app.Use(timeout.New(30 * time.Second))
	app.Use(ratelimiter.New(ratelimiter.Config{Max: 120, Window: time.Minute}))
	app.Use(session.New(sessionManager))

	// ── 6. Tenant & Principal ─────────────────────────────────────────────
	app.Use(tenant.New(tenant.Config{Header: "X-Tenant-ID", Required: false}))
	app.Use(fh.UsePrincipal(
		fh.PrincipalExtractor(fh.PrincipalExtractors{
			ID:         fh.HeaderString("X-Subject-ID"),
			Type:       fh.StaticString("user"),
			TenantID:   fh.LocalString("tenant_id"),
			Roles:      fh.HeaderCSV("X-Roles"),
			Scopes:     fh.HeaderCSV("X-Scopes"),
			AuthMethod: fh.StaticString("header"),
		}),
		false,
	))

	// ── 7. AuthZ from contrib ─────────────────────────────────────────────
	app.Use(middleware.FHWithConfig(middleware.FHConfig{
		Engine: authzEngine,
		Subject: middleware.SubjectFromPrincipal(),
		Action:  middleware.ActionFromMethod(),
		Resource: func(c *fh.Ctx) (*authz.Resource, bool, error) {
			tenantStr, _ := c.Locals("tenant_id").(string)
			res := &authz.Resource{
				ID:       c.Method() + ":" + c.Path(),
				Type:     "route",
				TenantID: tenantStr,
			}
			parts := strings.Split(strings.Trim(c.Path(), "/"), "/")
			if len(parts) >= 3 && parts[0] == "api" && parts[1] == "users" {
				res.OwnerID = parts[len(parts)-1]
			}
			return res, true, nil
		},
		Environment: func(c *fh.Ctx) (*authz.Environment, bool, error) {
			tenantStr, _ := c.Locals("tenant_id").(string)
			return &authz.Environment{Time: time.Now(), TenantID: tenantStr}, true, nil
		},
		OnDenied: func(c *fh.Ctx, decision *authz.Decision) error {
			return c.Status(http.StatusForbidden).JSON(fh.Map{"error": "access_denied", "reason": decision.Reason})
		},
		OnError: func(c *fh.Ctx, err error) error {
			return c.Status(http.StatusInternalServerError).JSON(fh.Map{"error": "authorization_error"})
		},
		SkipPaths: []string{"/health", "/_fh/*", "/static/*", "/_demo/*", "/ws", "/error/*", "/upload", "/account*", "/webhooks/*", "/queue/*", "/public", "/geo-restricted", "/admin/*", "/api/v1/transfers", "/api/v1/account/login", "/api/v1/functions/*", "/api/v1/reports/export", "/api/v1/payments/approve", "/api/search", "/typed/*", "/redirect", "/"},
	}))

	// ── 8. TCPGuard Authorizer Middleware ────────────────────────────────
	tcpguardMiddleware := authorizer.New(guard)
	app.Use(func(c *fh.Ctx) error {
		path := c.Path()
		// TCPGuard guards only specific demo paths.
		if !strings.HasPrefix(path, "/public") &&
			!strings.HasPrefix(path, "/geo-restricted") &&
			!strings.HasPrefix(path, "/admin") &&
			!strings.HasPrefix(path, "/api/v1/transfers") &&
			!strings.HasPrefix(path, "/api/v1/account/login") &&
			!strings.HasPrefix(path, "/api/v1/functions") &&
			!strings.HasPrefix(path, "/api/v1/reports/export") &&
			!strings.HasPrefix(path, "/api/v1/payments/approve") &&
			!strings.HasPrefix(path, "/api/users") {
			return c.Next()
		}
		return tcpguardMiddleware(c)
	})

	// ── 9. Routes: TCPGuard Demo Endpoints ──────────────────────────────
	app.Get("/", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{
			"service": "fh comprehensive example",
			"endpoints": []string{
				"GET  /health", "GET  /static/*", "GET  /ws",
				"GET/POST/PUT/DELETE  /api/v1/users[/:id]",
				"GET/POST  /api/documents[/:id|/sensitive]",
				"GET  /api/admin/settings", "GET  /api/compliance/report",
				"GET  /api/search?q=", "POST  /webhooks/payments",
				"GET/POST  /account[/login]", "POST  /upload",
				"GET  /error/validation", "GET  /error/internal",
				"GET  /_fh/compliance[/controls|/findings]",
				"GET  /_fh/config/safe", "POST  /queue/email",
				"TCPGuard demo: /_demo/*, /public, /admin/*, /api/v1/*",
			},
		})
	})

	app.Post("/_demo/risk-source", func(c *fh.Ctx) error {
		var req tcpguard.LookupRequest
		_ = c.BodyParser(&req)
		score := 15
		label := "normal"
		if req.Key == "risky-http" {
			score = 88
			label = "elevated"
		}
		return c.JSON(fh.Map{"score": score, "label": label})
	})
	app.Post("/_demo/sign", func(c *fh.Ctx) error {
		method := c.Query("method", http.MethodPost)
		path := c.Query("path", "/api/v1/transfers")
		body := c.BodyRaw()
		return c.JSON(fh.Map{
			"method":    method,
			"path":      path,
			"signature": sign(method, path, body),
			"nonce":     "nonce-" + strconv.FormatInt(time.Now().UnixNano(), 10),
			"timestamp": time.Now().Unix(),
			"secret":    "server-side only in real deployments",
		})
	})
	app.Post("/_demo/auth/fail", func(c *fh.Ctx) error {
		sec := contextFromFiber(c)
		decision := guard.Evaluate(c.Context(), tcpguard.Event{Type: "auth.login_failed", Source: "fiber-demo"}, sec)
		return c.Status(httpStatus(decision.Effect)).JSON(decision)
	})
	app.Get("/_demo/approvals", func(c *fh.Ctx) error {
		status := tcpguard.ApprovalStatus(c.Query("status"))
		records, err := guard.ListApprovals(c.Context(), status)
		if err != nil {
			return c.Status(http.StatusInternalServerError).JSON(fh.Map{"error": err.Error()})
		}
		return c.JSON(records)
	})
	app.Post("/_demo/approvals/:id/approve", func(c *fh.Ctx) error {
		record, err := guard.Approve(c.Context(), c.Params("id"), c.Get("X-Approver", "security-admin"), c.Get("X-Reason", "approved"))
		if err != nil {
			return c.Status(http.StatusBadRequest).JSON(fh.Map{"error": err.Error()})
		}
		return c.JSON(record)
	})
	app.Post("/_demo/approvals/:id/reject", func(c *fh.Ctx) error {
		record, err := guard.Reject(c.Context(), c.Params("id"), c.Get("X-Approver", "security-admin"), c.Get("X-Reason", "rejected"))
		if err != nil {
			return c.Status(http.StatusBadRequest).JSON(fh.Map{"error": err.Error()})
		}
		return c.JSON(record)
	})
	app.Get("/_demo/incidents", func(c *fh.Ctx) error {
		incidents, err := store.ListIncidents(c.Context())
		if err != nil {
			return c.Status(http.StatusInternalServerError).JSON(fh.Map{"error": err.Error()})
		}
		return c.JSON(incidents)
	})
	app.Get("/_demo/audit", func(c *fh.Ctx) error {
		envelopes, err := store.ListAuditEnvelopes(c.Context())
		if err != nil {
			return c.Status(http.StatusInternalServerError).JSON(fh.Map{"error": err.Error()})
		}
		if err := tcpguard.VerifyAuditChain(envelopes); err != nil {
			return c.Status(http.StatusInternalServerError).JSON(fh.Map{"valid": false, "error": err.Error(), "envelopes": envelopes})
		}
		return c.JSON(fh.Map{"valid": true, "envelopes": envelopes})
	})

	// ── 10. Routes: TCPGuard Guarded Endpoints ──────────────────────────
	app.Get("/public", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"ok": true, "message": "clean request allowed", "risk": c.GetRespHeader("X-TCPGuard-Risk")})
	})
	app.Get("/geo-restricted", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"ok": true, "message": "geo-restricted request allowed"})
	})
	app.Post("/admin/users", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"ok": true, "message": "admin change accepted"})
	})
	app.Post("/api/v1/account/login", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"ok": true, "message": "login accepted"})
	})
	app.Post("/api/v1/reports/export", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"ok": true, "message": "export started"})
	})
	app.Post("/api/v1/functions/:name", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"ok": true, "function": c.Params("name"), "message": "function invoked"})
	})
	app.Put("/api/users/:id/order/:order_id", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"ok": true, "user": c.Params("id"), "order": c.Params("order_id")})
	})
	app.Post("/api/v1/payments/approve", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"ok": true, "message": "payment approved"})
	})
	app.Post("/api/v1/transfers", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"ok": true, "message": "signed transfer accepted"})
	})

	// ── 11. Routes: Standard Application Endpoints ──────────────────────
	app.Get("/health", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"status": "ok", "time": time.Now().UTC()})
	})

	app.Get("/static/*", func(c *fh.Ctx) error {
		return c.SendFile(filepath.Join("public", c.Param("*")))
	})

	// REST API — Users
	users := app.Group("/api/v1/users", fh.RequireAuth())
	users.Get("", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"users": []fh.Map{{"id": "usr_1", "name": "Alice"}, {"id": "usr_2", "name": "Bob"}}})
	})
	users.Get("/:id", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"user": c.Param("id"), "decision": middleware.FHDecision(c)})
	})
	users.Post("", func(c *fh.Ctx) error {
		var body fh.Map
		if err := c.BodyParser(&body); err != nil {
			return fh.BadRequest("invalid JSON body")
		}
		return c.Status(fh.StatusCreated).JSON(fh.Map{"id": "usr_new", "name": body["name"]})
	})
	users.Put("/:id", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"updated": c.Param("id")})
	})
	users.Delete("/:id", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"deleted": c.Param("id")})
	})

	// Tenant-scoped Documents
	docs := app.Group("/api/documents", fh.RequireAuth())
	docs.Get("", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"documents": []string{"doc_1", "doc_2"}, "tenant": c.Locals("tenant_id")})
	})
	docs.Get("/:id", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"document": c.Param("id"), "decision": middleware.FHDecision(c)})
	})
	docs.Post("/sensitive",
		compliance.New(compliance.Config{
			Security: fh.RouteSecurityConfig{AuthRequired: true, IdempotencyRequired: true},
			Data:     fh.DataPolicy{Sensitivity: "confidential", RedactLogs: true},
		}),
		func(c *fh.Ctx) error {
			return c.Status(http.StatusCreated).JSON(fh.Map{"status": "sensitive document created"})
		},
	)

	// Admin
	app.Get("/api/admin/settings",
		fh.RequireAuth(),
		compliance.New(compliance.Config{
			Security: fh.RouteSecurityConfig{AuthRequired: true, Roles: []string{"admin"}},
			Data:     fh.DataPolicy{Sensitivity: "confidential", RedactLogs: true},
		}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{"settings": "admin configuration", "decision": middleware.FHDecision(c)})
		},
	)

	// Compliance report
	app.Get("/api/compliance/report",
		fh.RequireAuth(),
		compliance.New(compliance.Config{
			Security: fh.RouteSecurityConfig{Roles: []string{"compliance-officer"}},
			Data:     fh.DataPolicy{Sensitivity: "confidential"},
		}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{"report": app.ComplianceReport(), "decision": middleware.FHDecision(c)})
		},
	)

	// Search
	app.Get("/api/search", func(c *fh.Ctx) error {
		q := c.Query("q")
		if q == "" {
			return fh.BadRequest("missing query parameter 'q'")
		}
		return c.JSON(fh.Map{"query": q, "results": []string{"result_1", "result_2"}})
	})

	// Webhook with HMAC signature + replay protection
	app.Post("/webhooks/payments",
		signature.New(signature.Config{
			Secret:    []byte("webhook-secret"),
			Tolerance: 5 * time.Minute,
		}),
		replay.New(replay.Config{Header: "X-Nonce", TTL: 10 * time.Minute}),
		func(c *fh.Ctx) error {
			return c.Status(http.StatusAccepted).JSON(fh.Map{"status": "webhook received"})
		},
	)

	// Session-based account
	csrfProtection := csrf.New(csrf.Config{TrustedOrigins: []string{"http://localhost:3000"}})
	app.Get("/account", csrfProtection, func(c *fh.Ctx) error {
		s := session.Get(c)
		return c.JSON(fh.Map{"user": s.Get("user"), "csrf_token": c.Locals("csrf_token")})
	})
	app.Post("/account/login", csrfProtection, func(c *fh.Ctx) error {
		s := session.Get(c)
		s.Set("user", "demo-user")
		if err := sessionManager.Regenerate(c, s); err != nil {
			return err
		}
		return c.JSON(fh.Map{"status": "signed_in"})
	})

	// File upload
	app.Post("/upload", func(c *fh.Ctx) error {
		file, err := c.FormFile("file")
		if err != nil {
			return fh.BadRequest("missing file field")
		}
		return c.JSON(fh.Map{"filename": file.FileName, "size": file.Size, "status": "uploaded"})
	})

	// WebSocket echo
	app.Get("/ws", websocket.New(func(conn *websocket.Conn) error {
		for {
			op, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			conn.WriteMessage(op, msg)
		}
		return nil
	}))

	// Error demonstration
	app.Get("/error/validation", func(c *fh.Ctx) error {
		return &fh.ValidationError{Fields: []fh.FieldError{
			{Field: "email", Message: "invalid email format"},
			{Field: "age", Message: "must be at least 18"},
		}}
	})
	app.Get("/error/internal", func(c *fh.Ctx) error {
		return fh.NewHTTPError(fh.StatusInternalServerError, "INTERNAL_ERROR", "something went wrong")
	})

	// Typed handler (from modern example)
	type HelloRequest struct {
		Name string `json:"name"`
	}
	type HelloResponse struct {
		Greeting string `json:"greeting"`
	}
	app.PostTyped("/typed/hello", func(c *fh.Ctx, req *HelloRequest) (*HelloResponse, error) {
		if req.Name == "" {
			req.Name = "World"
		}
		return &HelloResponse{Greeting: "Hello, " + req.Name + "!"}, nil
	})

	// Redirect (from redirect example)
	app.Get("/redirect", func(c *fh.Ctx) error {
		return c.Redirect("https://example.com", fh.StatusMovedPermanently)
	})

	// ── 12. Queue Worker ─────────────────────────────────────────────────
	app.Queue().Register("email.send", func(ctx context.Context, job *fh.QueueJob) error {
		log.Printf("email worker: job=%s payload=%s", job.ID, job.Payload)
		return nil
	})
	app.Post("/queue/email", func(c *fh.Ctx) error {
		jobID, err := app.Queue().Enqueue("email.send", fh.Map{"to": "user@example.com", "subject": "Hello"}, nil)
		if err != nil {
			return err
		}
		return c.JSON(fh.Map{"job_id": jobID, "status": "queued"})
	})

	// ── 13. TCPGuard Management Admin Server ────────────────────────────
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		adminMux := http.NewServeMux()
		adminMux.Handle("/health", management)
		adminMux.Handle("/reload", management)
		adminMux.Handle("/simulate", management)
		adminMux.Handle("/explain", management)
		adminMux.Handle("/incidents", management)
		adminMux.Handle("/audit", management)
		adminMux.Handle("/audit/verify", management)
		adminMux.Handle("/approvals", management)
		adminMux.Handle("/approvals/approve", management)
		adminMux.Handle("/approvals/reject", management)
		fmt.Printf("TCPGuard admin server listening on http://127.0.0.1:18183\n")
		if err := http.ListenAndServe("127.0.0.1:18183", adminMux); err != nil {
			log.Printf("admin server stopped: %v", err)
		}
	}()

	// ── 14. Start ─────────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		log.Println("shutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := app.ShutdownWithContext(ctx); err != nil {
			log.Printf("shutdown error: %v", err)
		}
	}()

	fmt.Printf("comprehensive example listening on %s\n", *addr)
	fmt.Printf("  compliance:    http://localhost%s/_fh/compliance\n", *addr)
	fmt.Printf("  health:        http://localhost%s/health\n", *addr)
	fmt.Printf("  static:        http://localhost%s/static/hello.txt\n", *addr)
	fmt.Printf("  websocket:     ws://localhost%s/ws\n", *addr)
	fmt.Printf("  tcpguard:      http://localhost%s/public\n", *addr)
	fmt.Printf("  tcpguard mgmt: http://127.0.0.1:18183/health  (X-API-Key: %s)\n", adminKey)
	fmt.Println("  press Ctrl+C to stop")
	if err := app.Listen(*addr); err != nil {
		log.Fatal(err)
	}
	wg.Wait()
}

// ── TCPGuard helpers ─────────────────────────────────────────────────────────

func extractIdentity(r *http.Request, sec *tcpguard.Context) {
	sec.Identity.ID = firstNonEmpty(r.Header.Get("X-User-ID"), "anonymous")
	sec.Identity.Role = firstNonEmpty(r.Header.Get("X-User-Role"), "member")
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
		sec.Network.Country = country
		sec.Network.CountryCode = country
	}
	sec.Network.ASN = r.Header.Get("X-ASN")
}

func extractBusiness(r *http.Request, sec *tcpguard.Context) {
	sec.Business.Action = r.Header.Get("X-Business-Action")
	sec.Business.Entity = r.Header.Get("X-Business-Entity")
	sec.Business.Workflow = r.Header.Get("X-Workflow")
	sec.Business.Sensitivity = r.Header.Get("X-Sensitivity")
	sec.Business.OutsideHours = boolHeader(r, "X-Outside-Hours") || sec.Business.OutsideHours
	if amount := r.Header.Get("X-Business-Amount"); amount != "" {
		sec.Business.Amount, _ = strconv.ParseFloat(amount, 64)
	}
}

func contextFromFiber(c *fh.Ctx) *tcpguard.Context {
	r := &http.Request{Header: http.Header{}}
	r.Method = c.Method()
	r.RemoteAddr = c.IP() + ":0"
	for key, value := range c.GetReqHeaders() {
		if len(value) > 0 {
			r.Header.Set(key, strings.Join(value, ","))
		}
	}
	r.Header.Set("X-Request-ID", firstNonEmpty(c.Get("X-Request-ID"), "demo-"+strconv.FormatInt(time.Now().UnixNano(), 10)))
	sec := &tcpguard.Context{
		Request: tcpguard.RequestContext{
			ID:        r.Header.Get("X-Request-ID"),
			Method:    c.Method(),
			Path:      c.Path(),
			Headers:   map[string]string{},
			Query:     map[string]string{},
			UserAgent: c.Get("User-Agent"),
		},
		Network:  tcpguard.NetworkContext{IP: firstNonEmpty(c.Get("X-Forwarded-For"), c.IP())},
		Runtime:  tcpguard.RuntimeContext{Timestamp: time.Now().UTC()},
		Security: map[string]any{},
		Rate:     map[string]any{},
	}
	for key, values := range r.Header {
		sec.Request.Headers[key] = strings.Join(values, ",")
	}
	extractIdentity(r, sec)
	extractBusiness(r, sec)
	return sec
}

func httpStatus(effect tcpguard.DecisionEffect) int {
	switch effect {
	case tcpguard.DecisionBlock, tcpguard.DecisionRevoke:
		return http.StatusForbidden
	case tcpguard.DecisionThrottle:
		return http.StatusTooManyRequests
	case tcpguard.DecisionChallenge:
		return http.StatusUnauthorized
	default:
		return http.StatusOK
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

// ── Bundle summary printer ──────────────────────────────────────────────────

func printBundleSummary(bundle tcpguard.Bundle) {
	fmt.Println()
	fmt.Println("TCPGuard policy bundle")
	printKeyValueTable([][2]string{
		{"name", bundle.Name},
		{"version", bundle.Version},
		{"mode", string(bundle.Mode)},
		{"timezone", bundle.Timezone},
		{"rules", strconv.Itoa(len(bundle.Rules))},
		{"datasources", strconv.Itoa(len(bundle.DataSources))},
		{"lookups", strconv.Itoa(len(bundle.Lookups))},
		{"actions", strconv.Itoa(len(bundle.Actions))},
		{"detectors", strconv.Itoa(len(bundle.Detectors))},
		{"intel_feeds", strconv.Itoa(len(bundle.IntelFeeds))},
		{"triggers", strconv.Itoa(len(bundle.DerivedEvents))},
	})
	printDataSourceTable(bundle.DataSources)
	printLookupTable(bundle.Lookups)
	printRuleTable(bundle.Rules)
	fmt.Println()
}

func printKeyValueTable(rows [][2]string) {
	w := newTabWriter()
	fmt.Fprintln(w, "KEY\tVALUE")
	for _, row := range rows {
		fmt.Fprintf(w, "%s\t%s\n", row[0], emptyDash(row[1]))
	}
	_ = w.Flush()
}

func printDataSourceTable(sources []tcpguard.DataSourceDefinition) {
	if len(sources) == 0 {
		return
	}
	w := newTabWriter()
	fmt.Fprintln(w, "\nDATASOURCES")
	fmt.Fprintln(w, "ID\tTYPE\tKEY\tTARGET")
	for _, source := range sources {
		target := firstNonEmpty(source.Path, source.URL, source.DSN, source.Prefix, "-")
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", source.ID, emptyDash(source.Type), emptyDash(source.Key), target)
	}
	_ = w.Flush()
}

func printLookupTable(lookups []tcpguard.LookupDefinition) {
	if len(lookups) == 0 {
		return
	}
	w := newTabWriter()
	fmt.Fprintln(w, "\nLOOKUPS")
	fmt.Fprintln(w, "ID\tSOURCE\tMODE\tFALLBACK\tOUTPUTS")
	for _, lookup := range lookups {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", lookup.ID, lookup.Source, emptyDash(lookup.Mode), emptyDash(string(lookup.Fallback.Policy)), joinMapValues(lookup.Outputs))
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
	w := newTabWriter()
	fmt.Fprintln(w, "\nRULES")
	fmt.Fprintln(w, "PRIORITY\tSTATUS\tRULE\tTRIGGERS\tPATHS\tACTIONS\tAPPROVAL")
	for _, rule := range rules {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			rule.Priority,
			emptyDash(string(rule.Status)),
			rule.ID,
			emptyDash(strings.Join(rule.Triggers, ",")),
			emptyDash(strings.Join(rule.Scope.Paths, ",")),
			emptyDash(formatRuleActions(rule.Actions)),
			formatApproval(rule.Approval),
		)
	}
	_ = w.Flush()
}

func newTabWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
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

func formatApproval(approval tcpguard.Approval) string {
	if !approval.Required {
		return "-"
	}
	if len(approval.Approvers) == 0 {
		return "required"
	}
	return "required:" + strings.Join(approval.Approvers, ",")
}

func joinMapValues(values map[string]string) string {
	if len(values) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"->"+values[key])
	}
	return strings.Join(out, ",")
}

func emptyDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

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
