package app

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oarkflow/authz"
	"github.com/oarkflow/fh"
	contriblogger "github.com/oarkflow/fh-contrib/logger"
	"github.com/oarkflow/fh/examples/template/internal/auth"
	"github.com/oarkflow/fh/examples/template/internal/config"
	"github.com/oarkflow/fh/examples/template/internal/repository"
	"github.com/oarkflow/fh/examples/template/internal/service"
	"github.com/oarkflow/fh/examples/template/internal/transport/httpapi"
	"github.com/oarkflow/fh/examples/template/internal/transport/web"
	"github.com/oarkflow/fh/mw/bodylimit"
	"github.com/oarkflow/fh/mw/correlationid"
	"github.com/oarkflow/fh/mw/csrf"
	"github.com/oarkflow/fh/mw/hostguard"
	"github.com/oarkflow/fh/mw/logger"
	"github.com/oarkflow/fh/mw/metrics"
	"github.com/oarkflow/fh/mw/ratelimiter"
	"github.com/oarkflow/fh/mw/recover"
	"github.com/oarkflow/fh/mw/requestid"
	"github.com/oarkflow/fh/mw/security"
	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/mw/timeout"
	"github.com/oarkflow/fh/pkg/storage/kv"
	"github.com/oarkflow/zlog"
)

type Server struct {
	app      *fh.App
	cfg      config.Config
	database *repository.Database
	logger   interface{ Close() error }
}

func New() (*Server, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	framework, err := config.Framework(cfg)
	if err != nil {
		return nil, err
	}
	framework.TemplateEngine = web.NewEngine()
	logSink := zlog.NewProductionLogger(cfg.Name, cfg.Environment)
	framework.Logger = &contriblogger.ZlogAdapter{Logger: logSink}
	app := fh.NewWithConfig(framework)
	app.Static("/wasm", findDirectory("web/wasm"), fh.StaticConfig{MaxAge: 31536000, CacheDuration: time.Minute})
	store := kv.NewMemoryStore(kv.WithShardCount(4), kv.WithMaxEntries(10000))
	sessionSecret := []byte(cfg.SessionSecret)
	if len(sessionSecret) == 0 {
		sessionSecret = randomSecret()
	}
	sessions := session.NewSessionManager(store, session.SessionSecrets(sessionSecret), session.SessionSecure(cfg.Environment == "production"))
	app.Use(requestid.New(requestid.Config{LocalKey: "request_id"}), correlationid.New(), logger.New(), recover.New(), security.New(), hostguard.New(hostguard.Config{Allowed: cfg.AllowedHosts}), bodylimit.New(cfg.MaxBodyBytes), timeout.New(cfg.RequestTimeout), ratelimiter.New(ratelimiter.Config{Max: cfg.RateLimit, Window: time.Minute, Store: kv.NewMemoryStore(), SendHeaders: true}))
	requestMetrics := metrics.New()
	app.Use(requestMetrics.Middleware())
	database, err := repository.OpenDatabase(cfg.DatabaseEnabled, cfg.DatabaseDriver, cfg.DatabaseDSN, cfg.DatabaseName)
	if err != nil {
		_ = logSink.Close()
		return nil, fmt.Errorf("database: %w", err)
	}
	var repo repository.Repository = repository.NewMemory()
	if cfg.DatabaseEnabled {
		repo = database
	}
	svc := service.New(repo)
	app.HealthCheck("/healthz", fh.HealthConfig{})
	app.Get("/readyz", func(c fh.Ctx) error {
		ok, checks := app.HealthStatus(c.Context())
		if !ok {
			return c.Status(fh.StatusServiceUnavailable).JSON(fh.Map{"status": "not_ready", "checks": checks})
		}
		return c.JSON(fh.Map{"status": "ready"})
	})
	app.AddHealthCheck("repository", time.Second, repo.Health)
	app.AddHealthCheck("database", time.Second, database.Health)
	jwtSecret := []byte(cfg.JWTSecret)
	if len(jwtSecret) == 0 {
		jwtSecret = randomSecret()
	}
	tokens := authz.NewTokenConfig(jwtSecret)
	tokens.Issuer = cfg.Name
	credentials, err := auth.NewCredentials(cfg.BootstrapUser, cfg.BootstrapPass)
	if err != nil {
		_ = database.Close()
		_ = logSink.Close()
		return nil, fmt.Errorf("credentials: %w", err)
	}
	var authenticators []auth.Authenticator
	if cfg.DevelopmentAuth {
		authenticators = append(authenticators, auth.DevAuthenticator{})
	}
	authenticators = append(authenticators, auth.BearerAuthenticator{Tokens: tokens})
	authenticator := auth.FirstAuthenticator{Authenticators: append(authenticators, auth.SessionAuthenticator{Manager: sessions})}
	secureCookie := cfg.Environment == "production"
	authCSRF := csrf.New(csrf.Config{
		CookieSecure:        secureCookie,
		AllowInsecureCookie: !secureCookie,
		RequireOriginHeader: secureCookie,
		AllowMissingOrigin:  !secureCookie,
		TrustedOrigins:      trustedOrigins(cfg.AllowedHosts, cfg.Addr),
	})
	httpapi.Register(app, svc, credentials, tokens, sessions, session.New(sessions), authCSRF, auth.Middleware(authenticator, nil, ""), auth.Middleware(authenticator, auth.RoleAuthorizer{Engine: auth.NewRBAC()}, "route:protected"))
	web.Register(app, cfg.Environment == "production", cfg.Name, cfg.Addr, cfg.AllowedHosts)
	app.EnableOpenAPI("/openapi.json", fh.OpenAPIConfig{Title: cfg.Name, Version: "1.0.0", Description: "Generated fh application API"})
	return &Server{app: app, cfg: cfg, database: database, logger: logSink}, nil
}

func trustedOrigins(hosts []string, addr string) []string {
	origins := make([]string, 0, len(hosts)*4)
	_, port, _ := net.SplitHostPort(addr)
	for _, host := range hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		if strings.HasPrefix(host, "http://") || strings.HasPrefix(host, "https://") {
			origins = append(origins, strings.TrimRight(host, "/"))
			continue
		}
		originHost := host
		if ip := net.ParseIP(host); ip != nil && strings.Contains(host, ":") {
			originHost = "[" + host + "]"
		}
		origins = append(origins, "http://"+originHost, "https://"+originHost)
		_, _, splitErr := net.SplitHostPort(host)
		if port != "" && splitErr != nil {
			origins = append(origins, "http://"+net.JoinHostPort(host, port), "https://"+net.JoinHostPort(host, port))
		}
	}
	return origins
}

func findDirectory(relative string) string {
	cwd, err := os.Getwd()
	if err != nil {
		return relative
	}
	for current := cwd; ; current = filepath.Dir(current) {
		candidate := filepath.Join(current, relative)
		if info, statErr := os.Stat(candidate); statErr == nil && info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(current)
		if parent == current {
			return relative
		}
	}
}

func randomSecret() []byte {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		panic(err)
	}
	return secret
}
func (s *Server) Run(ctx context.Context) error {
	defer func() { _ = s.Close() }()
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.app.Serve(ln) }()
	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		if err := s.app.ShutdownWithTimeout(10 * time.Second); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		return nil
	}
}

// Close releases application-owned database and logging resources. Run calls
// it automatically; tests and embedded users should call it explicitly.
func (s *Server) Close() error {
	var first error
	if s.database != nil {
		first = s.database.Close()
	}
	if s.logger != nil {
		if err := s.logger.Close(); first == nil {
			first = err
		}
	}
	return first
}
