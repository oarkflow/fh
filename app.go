package fh

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Map map[string]any

// ErrorHandler handles errors returned from route handlers and middleware.
type ErrorHandler func(Ctx, error)

// NotFoundHandler handles requests that do not match any route.
type NotFoundHandler func(Ctx) error

// MethodNotAllowedHandler handles requests whose path matches one or more
// routes but whose method is not allowed. allowed is already ordered for the
// Allow header.
type MethodNotAllowedHandler func(Ctx, []string) error

// OptionsHandler handles automatic OPTIONS responses for matched routes and
// server-wide OPTIONS * requests. allowed is already ordered for the Allow
// header.
type OptionsHandler func(Ctx, []string) error

// ── Lifecycle events ───────────────────────────────────────────────────────

// HookFunc is a lifecycle hook with optional error propagation.
type HookFunc func() error

// Hooks groups all application lifecycle hooks.
type Hooks struct {
	onListen   []HookFunc
	onShutdown []HookFunc
	onConnect  []func(net.Conn)
	onClose    []func(net.Conn)
	onError    []func(error)
}

// ── Config ─────────────────────────────────────────────────────────────────

// Config holds server configuration.
type Config struct {
	// SecureByDefault enables the framework's fail-closed protocol and response
	// baseline. It is resolved once while the app is built, so disabled servers
	// pay no request-path cost. Application-specific authentication,
	// authorization, CORS, CSRF, and rate-limit policies must still be installed
	// explicitly because the framework cannot infer them safely.
	SecureByDefault bool
	// Mode controls secure default and compliance validation behavior.
	Mode Mode
	// Compliance enables business/professional/enterprise/security evidence endpoints and profiles.
	Compliance ComplianceConfig
	// Audit configures compliance-grade business/security audit records.
	Audit AuditConfig
	// Redaction controls sensitive field masking across audit, logs, journals and examples.
	Redaction   RedactionConfig
	ReadTimeout time.Duration
	// ReadHeaderTimeout bounds request-line and header reads independently from
	// the body budget. It starts when the first request byte arrives, preventing
	// slowloris clients from extending a deadline one byte at a time.
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	// HandlerTimeout bounds handler execution through Ctx.Context. It is
	// independent from WriteTimeout so long-lived streaming responses can refresh
	// socket write deadlines without inheriting an arbitrary handler lifetime.
	// Zero means no framework-level handler deadline.
	HandlerTimeout time.Duration
	// RequestBodyTimeout is an absolute budget for receiving one request body.
	RequestBodyTimeout time.Duration
	// TLSHandshakeTimeout bounds the server-side TLS handshake.
	TLSHandshakeTimeout time.Duration
	// HTTP2IdleTimeout bounds the interval between HTTP/2 frames.
	HTTP2IdleTimeout time.Duration
	MaxConnections   int
	// DisablePanicRecovery removes the application-level panic recovery defer from
	// every request. It is useful for trusted benchmark/edge deployments that use
	// process supervision or explicit recover middleware. Leave false for robust
	// production defaults.
	DisablePanicRecovery bool
	// SafeParams forces route params to be copied into stable strings. Leave false
	// for high-throughput handlers; turn on only when params are stored after the request.
	SafeParams bool
	// CaptureResponseBody keeps a copy of every response for middleware/tests.
	// It is disabled by default; reliability/cache middleware opt in per request.
	CaptureResponseBody bool
	// SendDateHeader emits an RFC 9110 Date header on HTTP/1.1 responses. It is
	// disabled by default because modern high-throughput frameworks omit it on
	// benchmark hot paths and the Date line costs bytes plus append work on every
	// response. Enable it at your edge/origin boundary when required by policy.
	SendDateHeader bool
	// SendKeepAliveHeader emits an explicit Connection: keep-alive header for
	// HTTP/1.1 keep-alive responses. It is disabled by default because keep-alive
	// is implicit in HTTP/1.1; Connection: close is still emitted when needed.
	SendKeepAliveHeader bool
	// ServerHeader, when non-empty, is sent as the Server response header.
	// Empty by default (no Server header sent) for security.
	ServerHeader string
	// StrictHeaderValueValidation is retained for configuration compatibility.
	// RFC-invalid control bytes are now rejected unconditionally because allowing
	// them creates unsafe parser differentials.
	StrictHeaderValueValidation bool
	ReadBufferSize              int
	MaxRequestBodySize          int
	MaxHeaderListSize           int
	MaxHeaderCount              int
	MaxRequestLineSize          int
	MaxConcurrentStreams        uint32
	DisableKeepAlive            bool
	DisableHTTP2                bool
	// DisableH2C rejects cleartext HTTP/2 prior knowledge and HTTP/1.1 h2c
	// upgrades while retaining HTTP/2 over TLS/ALPN.
	DisableH2C bool
	// HSTSPreload enables the HSTS preload directive when SecureByDefault is active.
	// Submit the domain to hstspreload.org after enabling.
	HSTSPreload  bool
	ErrorHandler ErrorHandler
	NotFoundHandler  NotFoundHandler
	MethodNotAllowed MethodNotAllowedHandler
	OptionsHandler   OptionsHandler
	// RequestHeadHandler runs after request-line/header validation and route
	// matching but before 100-continue and body reads. Return nil without writing
	// a response to accept the body; return an error or write a response to reject
	// it early. The handler must not attempt to read Body.
	RequestHeadHandler HandlerFunc
	Logger             Logger
	TemplateEngine     TemplateEngine
	// Reliability enables request journal, idempotency, and durable async queue.
	Reliability ReliabilityConfig
	// Environment controls safe error exposure defaults. Use EnvDevelopment locally and EnvProduction in production.
	Environment Environment
	// ErrorOptions controls RFC 9457 problem details, redaction, and debug extensions.
	ErrorOptions ErrorOptions
	// Debug exposes private error causes in 500 responses. Keep disabled in production.
	Debug bool
	// ShutdownTimeout is the maximum duration to wait for active connections to
	// complete during graceful shutdown. Zero means wait indefinitely.
	ShutdownTimeout time.Duration
	// StartupBanner controls the optional pretty ASCII startup message printed
	// when Serve starts. It is enabled by default and can be disabled for tests,
	// embedded deployments, JSON-only logs, or process supervisors.
	StartupBanner StartupBannerConfig
}

var defaultConfig = Config{
	ReadTimeout:          10 * time.Second,
	WriteTimeout:         30 * time.Second,
	IdleTimeout:          120 * time.Second,
	ReadHeaderTimeout:    5 * time.Second,
	RequestBodyTimeout:   10 * time.Second,
	ReadBufferSize:       16384,
	MaxRequestBodySize:   4 << 20,
	MaxHeaderListSize:    64 << 10,
	MaxHeaderCount:       64,
	MaxRequestLineSize:   8 << 10,
	MaxConcurrentStreams: 128,
	MaxConnections:       10_000,
	TLSHandshakeTimeout:  10 * time.Second,
	HTTP2IdleTimeout:     120 * time.Second,
	Environment:          EnvProduction,
	ErrorOptions:         ErrorOptions{Environment: EnvProduction},
}

const maxServerHeaderCount = 1024

// Option is a functional option for configuring an App via New.
type Option func(*Config)

func WithReadTimeout(d time.Duration) Option {
	return func(c *Config) { c.ReadTimeout = d }
}
func WithReadHeaderTimeout(d time.Duration) Option {
	return func(c *Config) { c.ReadHeaderTimeout = d }
}
func WithWriteTimeout(d time.Duration) Option {
	return func(c *Config) { c.WriteTimeout = d }
}
func WithIdleTimeout(d time.Duration) Option {
	return func(c *Config) { c.IdleTimeout = d }
}
func WithHandlerTimeout(d time.Duration) Option {
	return func(c *Config) { c.HandlerTimeout = d }
}
func WithRequestBodyTimeout(d time.Duration) Option {
	return func(c *Config) { c.RequestBodyTimeout = d }
}
func WithTLSHandshakeTimeout(d time.Duration) Option {
	return func(c *Config) { c.TLSHandshakeTimeout = d }
}
func WithHTTP2IdleTimeout(d time.Duration) Option {
	return func(c *Config) { c.HTTP2IdleTimeout = d }
}
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *Config) { c.ShutdownTimeout = d }
}
func WithMaxConnections(n int) Option {
	return func(c *Config) { c.MaxConnections = n }
}
func WithReadBufferSize(n int) Option {
	return func(c *Config) { c.ReadBufferSize = n }
}
func WithMaxRequestBodySize(n int) Option {
	return func(c *Config) { c.MaxRequestBodySize = n }
}
func WithMaxHeaderListSize(n int) Option {
	return func(c *Config) { c.MaxHeaderListSize = n }
}
func WithMaxHeaderCount(n int) Option {
	return func(c *Config) { c.MaxHeaderCount = n }
}
func WithMaxRequestLineSize(n int) Option {
	return func(c *Config) { c.MaxRequestLineSize = n }
}
func WithMaxConcurrentStreams(n uint32) Option {
	return func(c *Config) { c.MaxConcurrentStreams = n }
}
func WithDisablePanicRecovery(disabled bool) Option {
	return func(c *Config) { c.DisablePanicRecovery = disabled }
}
func WithSafeParams(enabled bool) Option {
	return func(c *Config) { c.SafeParams = enabled }
}
func WithCaptureResponseBody(enabled bool) Option {
	return func(c *Config) { c.CaptureResponseBody = enabled }
}
func WithSendDateHeader(enabled bool) Option {
	return func(c *Config) { c.SendDateHeader = enabled }
}
func WithSendKeepAliveHeader(enabled bool) Option {
	return func(c *Config) { c.SendKeepAliveHeader = enabled }
}
func WithStrictHeaderValueValidation(enabled bool) Option {
	return func(c *Config) { c.StrictHeaderValueValidation = enabled }
}

// WithSecureByDefault enables the fail-closed framework security baseline.
// It is equivalent to setting Config.SecureByDefault.
func WithSecureByDefault(enabled bool) Option {
	return func(c *Config) { c.SecureByDefault = enabled }
}
func WithDisableKeepAlive(disabled bool) Option {
	return func(c *Config) { c.DisableKeepAlive = disabled }
}
func WithDisableHTTP2(disabled bool) Option {
	return func(c *Config) { c.DisableHTTP2 = disabled }
}
func WithDisableH2C(disabled bool) Option {
	return func(c *Config) { c.DisableH2C = disabled }
}
func WithDebug(enabled bool) Option {
	return func(c *Config) { c.Debug = enabled }
}
func WithErrorHandler(h ErrorHandler) Option {
	return func(c *Config) { c.ErrorHandler = h }
}
func WithNotFoundHandler(h NotFoundHandler) Option {
	return func(c *Config) { c.NotFoundHandler = h }
}
func WithMethodNotAllowedHandler(h MethodNotAllowedHandler) Option {
	return func(c *Config) { c.MethodNotAllowed = h }
}
func WithOptionsHandler(h OptionsHandler) Option {
	return func(c *Config) { c.OptionsHandler = h }
}
func WithRequestHeadHandler(h HandlerFunc) Option {
	return func(c *Config) { c.RequestHeadHandler = h }
}
func WithLogger(l Logger) Option {
	return func(c *Config) { c.Logger = l }
}
func WithTemplateEngine(te TemplateEngine) Option {
	return func(c *Config) { c.TemplateEngine = te }
}
func WithReliability(r ReliabilityConfig) Option {
	return func(c *Config) { c.Reliability = r }
}
func WithEnvironment(env Environment) Option {
	return func(c *Config) { c.Environment = env }
}
func WithErrorOptions(eo ErrorOptions) Option {
	return func(c *Config) { c.ErrorOptions = eo }
}
func WithCompliance(cc ComplianceConfig) Option {
	return func(c *Config) { c.Compliance = cc }
}

// WithComplianceEndpointAuth sets (without disturbing any other Compliance
// field already set by an earlier option, e.g. NewEnterprise's defaults)
// the auth middleware guarding the /_fh/* compliance/health/runtime
// introspection endpoints mounted when Compliance.ExposeEndpoints is true.
// Apply this after NewEnterprise/NewProduction, or ExposeEndpoints mounts
// those routes with no authentication.
func WithComplianceEndpointAuth(middleware ...HandlerFunc) Option {
	return func(c *Config) { c.Compliance.EndpointAuth = middleware }
}

// WithMode selects the runtime profile. ModeFast keeps benchmark-oriented
// defaults; ModeProduction and ModeStrict enable safer network defaults.
func WithMode(mode Mode) Option {
	return func(c *Config) { c.Mode = mode }
}

func WithServerHeader(header string) Option {
	return func(c *Config) { c.ServerHeader = header }
}

// NewFast creates an app with benchmark-oriented defaults. Use this only behind
// a trusted edge or for controlled latency/RPS benchmarks. To avoid request-hot
// activity atomics, shutdown closes HTTP/1 connections immediately; use
// NewProduction when graceful completion of in-flight requests is required.
//
// WriteTimeout defaults to 0 here (unlike New/NewProduction's 30s): a non-zero
// WriteTimeout makes serveConn set a socket write deadline AND derive a fresh
// per-request context.Context via context.WithTimeout on every request, which
// costs real allocation, mutex, and GC overhead in the hot path. Pass
// WithWriteTimeout after NewFast's options to restore write deadlines and
// handler-visible cancellation if this app is exposed beyond a trusted edge.
func NewFast(opts ...Option) *App {
	all := append([]Option{WithMode(ModeFast), WithWriteTimeout(0), WithMaxConnections(0)}, opts...)
	return New(all...)
}

// NewProduction creates an app with production-safe protocol defaults while
// keeping the request hot path allocation-sensitive.
func NewProduction(opts ...Option) *App {
	all := append([]Option{WithMode(ModeProduction)}, opts...)
	return New(all...)
}

// NewEnterprise creates an app with strict protocol validation, audit,
// reliability, redaction and compliance evidence endpoints enabled.
//
// The compliance/health/runtime endpoints this mounts (/_fh/compliance,
// /_fh/routes, /_fh/runtime, /_fh/config/safe, ...) expose security posture
// details including the full route table annotated with which routes lack
// auth. Pass fh.WithComplianceEndpointAuth(yourAuthMiddleware) as one of
// opts so these routes aren't reachable unauthenticated; omitting it logs a
// startup warning and surfaces a critical finding from ValidateSecurity.
func NewEnterprise(opts ...Option) *App {
	all := append([]Option{WithMode(ModeEnterprise), WithCompliance(ComplianceConfig{Enabled: true, Profile: ComplianceEnterprise, Strict: true, ExposeEndpoints: true})}, opts...)
	return New(all...)
}

var ErrAppAlreadyStarted = errors.New("fh: app has already been started")
var ErrRewrite = errors.New("fh: reroute rewritten request")

// ── App ────────────────────────────────────────────────────────────────────

// App is the top-level application object. Create with New().
type App struct {
	cfg           Config
	router        *Router
	hooks         Hooks
	logger        Logger
	middleware    []HandlerFunc
	sem           chan struct{}
	listener      net.Listener
	activeConn    sync.WaitGroup
	closed        atomic.Bool
	draining      atomic.Bool
	connMu        sync.Mutex
	conns         map[net.Conn]*connState
	shutdownOnce  sync.Once
	started       atomic.Bool
	buildMu       sync.Mutex
	groups        []*Group
	lastRoute     namedRoute
	errorCounts   sync.Map // error code -> *atomic.Uint64
	routeMetaMu   sync.RWMutex
	routeMeta     []RouteInfo
	healthMu      sync.RWMutex
	healthChecks  []registeredHealthCheck
	openapi       OpenAPIConfig
	hasMiddleware bool
	reliability   *Reliability
	audit         AuditSink
	staticRoots   []*os.Root
	drainingCh    chan struct{} // closed when draining starts, signals all connection contexts
	fastHTTP1     bool          // trusted ModeFast: omit graceful activity bookkeeping
}

type connState struct {
	active atomic.Bool
	h2     *h2Conn
	// HTTP/1 requests on one connection are serialized. Keep the response
	// assembly buffer here instead of borrowing/returning a global pool item for
	// every request.
	writeBuf []byte
	ctx      *DefaultCtx
}

// New creates a new App with functional options. Call with zero options to use
// defaults. Example:
//
//	app := fh.New(
//	    fh.WithReadTimeout(5*time.Second),
//	    fh.WithWriteTimeout(10*time.Second),
//	    fh.WithDebug(true),
//	)
func New(opts ...Option) *App {
	cfg := defaultConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return buildApp(cfg)
}

// NewWithConfig creates a new App from a Config struct. Non-zero fields
// override defaults; nil handlers are replaced with built-in defaults. This
// is a convenience for users who prefer a single config object.
//
//	app := fh.NewWithConfig(fh.Config{
//	    ReadTimeout: 5 * time.Second,
//	    WriteTimeout: 10 * time.Second,
//	})
func NewWithConfig(cfg Config) *App {
	applyConfigDefaults(&cfg)
	return buildApp(cfg)
}

// applyConfigDefaults fills fields whose zero value means "not configured".
// Boolean fields are intentionally left untouched because false can be an
// explicit choice.
func applyConfigDefaults(cfg *Config) {
	if cfg.ReadBufferSize <= 0 {
		cfg.ReadBufferSize = defaultConfig.ReadBufferSize
	}
	if cfg.MaxRequestBodySize <= 0 {
		cfg.MaxRequestBodySize = defaultConfig.MaxRequestBodySize
	}
	if cfg.MaxHeaderListSize <= 0 {
		cfg.MaxHeaderListSize = defaultConfig.MaxHeaderListSize
	}
	if cfg.MaxHeaderCount <= 0 {
		cfg.MaxHeaderCount = defaultConfig.MaxHeaderCount
	}
	if cfg.MaxRequestLineSize <= 0 {
		cfg.MaxRequestLineSize = defaultConfig.MaxRequestLineSize
	}
	if cfg.MaxConcurrentStreams == 0 {
		cfg.MaxConcurrentStreams = defaultConfig.MaxConcurrentStreams
	}
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = defaultConfig.MaxConnections
	}
	if cfg.Environment == "" {
		cfg.Environment = defaultConfig.Environment
	}
	if cfg.TLSHandshakeTimeout <= 0 {
		cfg.TLSHandshakeTimeout = defaultConfig.TLSHandshakeTimeout
	}
	if cfg.HTTP2IdleTimeout <= 0 {
		cfg.HTTP2IdleTimeout = defaultConfig.HTTP2IdleTimeout
	}
	if cfg.RequestBodyTimeout <= 0 {
		cfg.RequestBodyTimeout = defaultConfig.RequestBodyTimeout
	}
}

// buildApp constructs an *App from a fully resolved Config. It applies default
// handlers and initializes the app struct together with the reliability
// subsystem when enabled.
func buildApp(cfg Config) *App {
	applySecureDefaults(&cfg)
	applyComplianceDefaults(&cfg)
	if cfg.MaxHeaderCount > maxServerHeaderCount {
		cfg.MaxHeaderCount = maxServerHeaderCount
	}
	if cfg.ErrorHandler == nil {
		cfg.ErrorHandler = defaultErrorHandler
	}
	if cfg.ErrorOptions.Environment == "" {
		cfg.ErrorOptions.Environment = cfg.Environment
	}
	if cfg.NotFoundHandler == nil {
		cfg.NotFoundHandler = defaultNotFoundHandler
	}
	if cfg.MethodNotAllowed == nil {
		cfg.MethodNotAllowed = defaultMethodNotAllowedHandler
	}
	if cfg.OptionsHandler == nil {
		cfg.OptionsHandler = defaultOptionsHandler
	}

	logger := cfg.Logger
	if logger == nil {
		logger = newSlogLogger()
	}

	prod := cfg.Mode == ModeProduction || cfg.Mode == ModeStrict || cfg.Mode == ModeEnterprise || cfg.Compliance.Enabled
	if prod {
		if cfg.ReadTimeout == 0 {
			logger.Warn("fh: ReadTimeout is 0 in production mode — server is vulnerable to slowloris attacks; set Config.ReadTimeout")
		}
		if cfg.WriteTimeout == 0 {
			logger.Warn("fh: WriteTimeout is 0 in production mode — responses may never complete; set Config.WriteTimeout")
		}
		if cfg.ReadHeaderTimeout == 0 {
			logger.Warn("fh: ReadHeaderTimeout is 0 in production mode — header reads are unbounded; set Config.ReadHeaderTimeout")
		}
	}

	app := &App{
		cfg:        cfg,
		router:     newRouter(),
		logger:     logger,
		conns:      make(map[net.Conn]*connState),
		drainingCh: make(chan struct{}),
		fastHTTP1:  cfg.Mode == ModeFast,
	}

	if cfg.Audit.Enabled {
		if cfg.Audit.Sink != nil {
			app.audit = cfg.Audit.Sink
		} else {
			sink, err := OpenFileAuditSink(cfg.Audit.FilePath)
			if err != nil {
				panic(err)
			}
			app.audit = sink
		}
	}

	if cfg.Reliability.Enabled {
		reliability, err := NewReliability(cfg.Reliability)
		if err != nil {
			panic(err)
		}
		app.reliability = reliability
	}

	if hm := defaultHardeningMiddleware(cfg); hm != nil {
		app.middleware = append([]HandlerFunc{hm}, app.middleware...)
	}

	if !cfg.DisablePanicRecovery {
		app.middleware = append([]HandlerFunc{app.defaultRecoveryMiddleware()}, app.middleware...)
	}

	if cfg.Compliance.ExposeEndpoints {
		if len(cfg.Compliance.EndpointAuth) == 0 {
			app.Logger().Warn("fh: Compliance.ExposeEndpoints is enabled with no Compliance.EndpointAuth — no /_fh/* endpoints were mounted; set Config.Compliance.EndpointAuth or fh.WithComplianceEndpointAuth")
		} else {
			app.EnableComplianceEndpoints(cfg.Compliance.EndpointPrefix, cfg.Compliance.EndpointAuth...)
			app.EnableHealth(cfg.Compliance.EndpointPrefix, cfg.Compliance.EndpointAuth...)
			app.EnableRuntime(cfg.Compliance.EndpointPrefix, cfg.Compliance.EndpointAuth...)
		}
	}
	if cfg.Compliance.FailOnCritical && hasCritical(app.ValidateSecurity()) {
		panic("fh: critical compliance/security findings")
	}

	app.router.UnsafeParams = !cfg.SafeParams
	app.hasMiddleware = len(app.middleware) > 0 || app.reliability != nil

	if cfg.MaxConnections > 0 {
		app.sem = make(chan struct{}, cfg.MaxConnections)
	}

	return app
}

// ── Routing methods ────────────────────────────────────────────────────────

func (a *App) Add(method, path string, handlers ...HandlerFunc) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	if len(handlers) == 0 {
		panic("fh: route requires at least one handler")
	}
	for _, handler := range handlers {
		if handler == nil {
			panic("fh: nil route handler")
		}
	}
	routeHandler := a.chain(handlers)
	if len(a.middleware) == 0 && a.reliability == nil && len(handlers) == 1 {
		if pre := prebuiltResponseForHandler(handlers[0]); pre != nil {
			a.router.registerPrebuiltResponse(method, path, pre)
		}
	}
	a.router.Add(method, path, routeHandler)
	a.registerRouteInfo(RouteInfo{Method: strings.ToUpper(strings.TrimSpace(method)), Path: normalizeRoutePath(strings.ToUpper(strings.TrimSpace(method)), path)})
	a.lastRoute = namedRoute{method: strings.ToUpper(strings.TrimSpace(method)), path: normalizeRoutePath(strings.ToUpper(strings.TrimSpace(method)), path)}
	return a
}

// Name names the most recently registered route, allowing fluent usage such
// as app.Get("/users/:id", handler).Name("users.show").
func (a *App) Name(name string) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	if a.lastRoute.method == "" {
		panic("fh: no route available to name")
	}
	a.router.Name(a.lastRoute.method, a.lastRoute.path, name)
	return a
}

// URL generates a URL path for a named route.
func (a *App) URL(name string, params ...map[string]string) (string, error) {
	return a.router.URL(name, params...)
}

func (a *App) Get(path string, handlers ...HandlerFunc) *App {
	return a.Add("GET", path, handlers...)
}

func (a *App) Post(path string, handlers ...HandlerFunc) *App {
	return a.Add("POST", path, handlers...)
}

func (a *App) Put(path string, handlers ...HandlerFunc) *App {
	return a.Add("PUT", path, handlers...)
}

func (a *App) Delete(path string, handlers ...HandlerFunc) *App {
	return a.Add("DELETE", path, handlers...)
}

func (a *App) Patch(path string, handlers ...HandlerFunc) *App {
	return a.Add("PATCH", path, handlers...)
}

func (a *App) Head(path string, handlers ...HandlerFunc) *App {
	return a.Add("HEAD", path, handlers...)
}

func (a *App) Options(path string, handlers ...HandlerFunc) *App {
	return a.Add("OPTIONS", path, handlers...)
}

func (a *App) Connect(path string, handlers ...HandlerFunc) *App {
	return a.Add("CONNECT", path, handlers...)
}
func (a *App) Trace(path string, handlers ...HandlerFunc) *App {
	return a.Add("TRACE", path, handlers...)
}
func (a *App) Query(path string, handlers ...HandlerFunc) *App {
	return a.Add("QUERY", path, handlers...)
}

func (a *App) All(path string, handlers ...HandlerFunc) *App {
	for _, m := range []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE", "QUERY"} {
		a.Add(m, path, handlers...)
	}
	return a
}

// Use registers global middleware (applied to all routes).
func (a *App) Use(handlers ...HandlerFunc) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	a.middleware = append(a.middleware, handlers...)
	a.hasMiddleware = true
	return a
}

// Group creates a route group with a shared prefix and optional middleware.
func (a *App) Group(prefix string, handlers ...HandlerFunc) *Group {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	g := &Group{app: a, prefix: prefix, middleware: handlers}
	a.groups = append(a.groups, g)
	return g
}

// ── Lifecycle hooks ────────────────────────────────────────────────────────

func (a *App) OnListen(fn HookFunc) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	a.hooks.onListen = append(a.hooks.onListen, fn)
	return a
}

func (a *App) OnShutdown(fn HookFunc) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	a.hooks.onShutdown = append(a.hooks.onShutdown, fn)
	return a
}

func (a *App) OnConnect(fn func(net.Conn)) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	a.hooks.onConnect = append(a.hooks.onConnect, fn)
	return a
}

func (a *App) OnClose(fn func(net.Conn)) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	a.hooks.onClose = append(a.hooks.onClose, fn)
	return a
}

func (a *App) OnError(fn func(error)) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	a.hooks.onError = append(a.hooks.onError, fn)
	return a
}

// ── Listen ─────────────────────────────────────────────────────────────────

func (a *App) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return a.Serve(ln)
}

// ListenTLS serves HTTPS using the standard library TLS stack.
func (a *App) ListenTLS(addr, certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return a.ServeTLS(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})
}

// ServeTLS wraps ln with TLS and advertises HTTP/2 through ALPN when enabled.
func (a *App) ServeTLS(ln net.Listener, config *tls.Config) error {
	if err := validateTLSConfig(config); err != nil {
		return err
	}
	if config == nil {
		return errors.New("nil TLS config")
	}
	cfg := config.Clone()
	if len(cfg.NextProtos) == 0 {
		if a.cfg.DisableHTTP2 {
			cfg.NextProtos = []string{"http/1.1"}
		} else {
			cfg.NextProtos = []string{"h2", "http/1.1"}
		}
	}
	return a.Serve(tls.NewListener(ln, cfg))
}

// ListenWithGracefulShutdown starts the server and blocks until SIGINT or
// SIGTERM is received, then performs a graceful shutdown. If ShutdownTimeout
// is configured, the server will force-close remaining connections after that
// duration. Use OnShutdown to register cleanup hooks.
func (a *App) ListenWithGracefulShutdown(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return a.serveWithSignal(ln, a.Serve)
}

// ListenTLSWithGracefulShutdown is the TLS counterpart to
// ListenWithGracefulShutdown. It loads the certificate pair, negotiates HTTP/2
// through ALPN when enabled, and drains on SIGINT or SIGTERM.
func (a *App) ListenTLSWithGracefulShutdown(addr, certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
	return a.serveWithSignal(ln, func(listener net.Listener) error { return a.ServeTLS(listener, tlsConfig) })
}

func (a *App) serveWithSignal(ln net.Listener, serve func(net.Listener) error) error {

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		a.logger.Info("signal received, starting graceful shutdown")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), a.effectiveShutdownTimeout())
		defer cancel()
		if err := a.ShutdownWithContext(shutdownCtx); err != nil {
			a.logger.Error("graceful shutdown error", "error", err)
		}
	}()

	return serve(ln)
}

// effectiveShutdownTimeout returns the configured shutdown timeout or a default
// of 30 seconds when Shutdown() is called without an explicit timeout via the
// ListenWithGracefulShutdown flow. It is only used internally.
func (a *App) effectiveShutdownTimeout() time.Duration {
	if a.cfg.ShutdownTimeout > 0 {
		return a.cfg.ShutdownTimeout
	}
	return 30 * time.Second
}

func (a *App) Serve(ln net.Listener) error {
	a.buildMu.Lock()
	if !a.started.CompareAndSwap(false, true) {
		a.buildMu.Unlock()
		return ErrAppAlreadyStarted
	}
	a.buildMu.Unlock()
	a.connMu.Lock()
	a.listener = ln
	a.connMu.Unlock()
	a.closed.Store(false)
	a.draining.Store(false)
	if a.reliability != nil {
		if err := a.reliability.Start(); err != nil {
			_ = ln.Close()
			return err
		}
	}
	// Freeze routes once the server starts. This removes router RWMutex
	// traffic from every request while preserving build-time safety.
	a.router.Freeze()

	for _, fn := range a.hooks.onListen {
		if err := fn(); err != nil {
			_ = ln.Close()
			return err
		}
	}

	a.printStartupBanner(ln)
	a.logger.Info("listening", "addr", ln.Addr())

	var acceptErr error
	for {
		conn, err := ln.Accept()
		if err != nil {
			if a.closed.Load() {
				break
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				a.logger.Warn("accept timeout", "error", err)
				continue
			}
			acceptErr = err
			_ = a.beginShutdown()
			break
		}

		if a.sem != nil {
			select {
			case a.sem <- struct{}{}:
			default:
				_ = writeAll(conn, serverError503)
				_ = conn.Close()
				continue
			}
		}

		a.connMu.Lock()
		if a.draining.Load() {
			a.connMu.Unlock()
			_ = conn.Close()
			if a.sem != nil {
				<-a.sem
			}
			continue
		}
		a.activeConn.Add(1)
		a.conns[conn] = &connState{writeBuf: make([]byte, 0, 4096)}
		a.connMu.Unlock()
		go a.serveConn(conn)
	}

	a.activeConn.Wait()
	a.runShutdownHooks()
	return acceptErr
}

func (a *App) assertMutable() {
	if a.started.Load() {
		panic(ErrAppAlreadyStarted)
	}
}

func (a *App) Shutdown() error {
	if a.cfg.ShutdownTimeout > 0 {
		return a.ShutdownWithTimeout(a.cfg.ShutdownTimeout)
	}
	err := a.beginShutdown()
	a.activeConn.Wait()
	a.runShutdownHooks()
	return err
}

func (a *App) ShutdownWithTimeout(d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return a.ShutdownWithContext(ctx)
}

func (a *App) ShutdownWithContext(ctx context.Context) error {
	if err := a.beginShutdown(); err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { recover() }()
		a.activeConn.Wait()
	}()
	select {
	case <-done:
		a.runShutdownHooks()
		return nil
	case <-ctx.Done():
		a.closeAllConnections()
		return ctx.Err()
	}
}

func (a *App) beginShutdown() error {
	a.closed.Store(true)
	a.draining.Store(true)
	// Signal all connection contexts that draining has started.
	// Safe to close multiple times — subsequent closes are recovered.
	func() {
		defer func() { recover() }()
		close(a.drainingCh)
	}()
	var err error
	a.connMu.Lock()
	listener := a.listener
	a.connMu.Unlock()
	if listener != nil {
		err = listener.Close()
	}
	var h2conns []*h2Conn
	a.connMu.Lock()
	for conn, state := range a.conns {
		if state.h2 != nil {
			h2conns = append(h2conns, state.h2)
		} else if !state.active.Load() {
			_ = conn.Close()
		}
	}
	a.connMu.Unlock()
	for _, h2c := range h2conns {
		h2c.startDrain()
	}
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (a *App) closeAllConnections() {
	a.connMu.Lock()
	for conn := range a.conns {
		_ = conn.Close()
	}
	a.connMu.Unlock()
}

func (a *App) runShutdownHooks() {
	a.shutdownOnce.Do(func() {
		for _, root := range a.staticRoots {
			if err := root.Close(); err != nil {
				a.logger.Error("static root shutdown error", "error", err)
			}
		}
		if a.audit != nil {
			if closer, ok := a.audit.(AuditSinkCloser); ok {
				if err := closer.Close(); err != nil {
					a.logger.Error("audit shutdown error", "error", err)
				}
			}
		}
		if a.reliability != nil {
			if err := a.reliability.Close(); err != nil {
				a.logger.Error("reliability shutdown error", "error", err)
			}
		}
		for _, fn := range a.hooks.onShutdown {
			if err := fn(); err != nil {
				a.logger.Error("shutdown hook error", "error", err)
			}
		}
	})
}

func (a *App) setH2Conn(conn net.Conn, h2c *h2Conn) {
	a.connMu.Lock()
	if state := a.conns[conn]; state != nil {
		state.active.Store(true)
		state.h2 = h2c
	}
	a.connMu.Unlock()
}

// ── Connection handler ─────────────────────────────────────────────────────

func (a *App) serveConn(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			args := []any{"panic", "connection hook panic"}
			if a.cfg.Debug || a.cfg.ErrorOptions.LogInternal {
				args = append(args, "detail", RedactSecrets(fmt.Sprint(r)), "stack", RedactSecrets(string(debug.Stack())))
			}
			a.logger.Error("connection hook panic", args...)
		}
		conn.Close()
		a.connMu.Lock()
		delete(a.conns, conn)
		a.connMu.Unlock()
		for _, fn := range a.hooks.onClose {
			func() {
				defer func() {
					if r := recover(); r != nil {
						args := []any{"panic", "close hook panic"}
						if a.cfg.Debug || a.cfg.ErrorOptions.LogInternal {
							args = append(args, "detail", RedactSecrets(fmt.Sprint(r)))
						}
						a.logger.Error("close hook panic", args...)
					}
				}()
				fn(conn)
			}()
		}
		if a.sem != nil {
			<-a.sem
		}
		a.activeConn.Done()
	}()

	// Connection-level context: cancelled when the TCP connection terminates
	// (client disconnect, idle close, or I/O error). Per-request contexts are
	// derived from this so that handlers see cancellation on connection death.
	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel()

	for _, fn := range a.hooks.onConnect {
		fn(conn)
	}
	a.connMu.Lock()
	state := a.conns[conn]
	a.connMu.Unlock()
	if tc, ok := conn.(*tls.Conn); ok && !a.cfg.DisableHTTP2 {
		if a.cfg.TLSHandshakeTimeout > 0 {
			_ = tc.SetDeadline(time.Now().Add(a.cfg.TLSHandshakeTimeout))
		}
		if err := tc.Handshake(); err != nil {
			if !isExpectedConnErr(err) {
				a.emitError(err)
			}
			return
		}
		_ = tc.SetDeadline(time.Time{})
		tlsState := tc.ConnectionState()
		connCtx = WithTLSState(connCtx, tlsState)
		if tlsState.NegotiatedProtocol == "h2" {
			// RFC 9113 §9.1: TLS 1.2 or higher required for HTTP/2
			if tlsState.Version < tls.VersionTLS12 {
				a.emitError(errors.New("http2: TLS version 1.2 or higher required"))
				return
			}
			h2c := newH2Conn(a, conn)
			a.setH2Conn(conn, h2c)
			h2c.serve(nil, false)
			return
		}
	} else if tc, ok := conn.(*tls.Conn); ok {
		// HTTP/2 may be disabled, but TLS state (especially verified client
		// certificates) must still be visible to HTTP/1 middleware.
		if a.cfg.TLSHandshakeTimeout > 0 {
			_ = tc.SetDeadline(time.Now().Add(a.cfg.TLSHandshakeTimeout))
		}
		if err := tc.Handshake(); err != nil {
			if !isExpectedConnErr(err) {
				a.emitError(err)
			}
			return
		}
		_ = tc.SetDeadline(time.Time{})
		connCtx = WithTLSState(connCtx, tc.ConnectionState())
	}

	rawBuf := getBuf(a.cfg.ReadBufferSize)
	defer putBuf(rawBuf)
	buf := *rawBuf

	accumulated := buf[:0]
	readDeadlineArmed := false

	for {
		var requestStart time.Time
		headerBudget := a.cfg.ReadHeaderTimeout
		if headerBudget <= 0 {
			headerBudget = a.cfg.ReadTimeout
		}
		if len(accumulated) > 0 && headerBudget > 0 {
			requestStart = time.Now()
			if err := conn.SetReadDeadline(requestStart.Add(headerBudget)); err != nil {
				return
			}
			readDeadlineArmed = true
		} else if len(accumulated) == 0 && a.cfg.IdleTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(a.cfg.IdleTimeout)); err != nil {
				return
			}
			readDeadlineArmed = true
		} else if readDeadlineArmed {
			if err := conn.SetReadDeadline(time.Time{}); err != nil {
				return
			}
			readDeadlineArmed = false
		}

		headEnd := findHeaderEnd(accumulated)
		for headEnd < 0 {
			if len(accumulated) == cap(buf) {
				maxHead := a.cfg.MaxRequestLineSize + a.cfg.MaxHeaderListSize + 4
				if maxHead <= 4 || len(accumulated) >= maxHead {
					_ = writeAll(conn, serverError431)
					return
				}
				newCap := cap(buf) * 2
				if newCap > maxHead {
					newCap = maxHead
				}
				grown := make([]byte, len(accumulated), newCap)
				copy(grown, accumulated)
				buf = grown[:cap(grown)]
				accumulated = buf[:len(grown)]
			}

			n, err := conn.Read(buf[len(accumulated):cap(buf)])
			if n > 0 {
				accumulated = buf[:len(accumulated)+n]
				if requestStart.IsZero() && headerBudget > 0 {
					requestStart = time.Now()
					if deadlineErr := conn.SetReadDeadline(requestStart.Add(headerBudget)); deadlineErr != nil {
						return
					}
					readDeadlineArmed = true
				}
			}
			if err != nil {
				if isTimeoutErr(err) && !a.closed.Load() {
					_ = writeAll(conn, serverError408)
				} else if err != io.EOF && !isExpectedConnErr(err) {
					a.emitError(err)
				}
				return
			}

			if !a.cfg.DisableHTTP2 && !a.cfg.DisableH2C && len(accumulated) <= len(h2ClientPreface) && bytes.Equal(accumulated, h2ClientPreface[:len(accumulated)]) {
				if len(accumulated) < len(h2ClientPreface) {
					continue
				}
				h2c := newH2Conn(a, conn)
				a.setH2Conn(conn, h2c)
				_ = conn.SetReadDeadline(time.Time{})
				h2c.serve(nil, true)
				return
			}
			if !a.cfg.DisableHTTP2 && !a.cfg.DisableH2C && len(accumulated) > len(h2ClientPreface) && bytes.Equal(accumulated[:len(h2ClientPreface)], h2ClientPreface) {
				h2c := newH2Conn(a, conn)
				a.setH2Conn(conn, h2c)
				_ = conn.SetReadDeadline(time.Time{})
				h2c.serve(accumulated[len(h2ClientPreface):], true)
				return
			}
			headEnd = findHeaderEnd(accumulated)
		}
		if state != nil && !a.fastHTTP1 {
			state.active.Store(true)
		}

		// ── Parse request head ────────────────────────────────────────
		ctx := acquireHTTP1Ctx(conn, a, state)

		consumed, err := parseRequestLine(accumulated, &ctx.Header, a.cfg.MaxRequestLineSize)
		if err != nil {
			releaseCtx(ctx)
			if errors.Is(err, ErrRequestLineTooLarge) {
				_ = writeAll(conn, serverError431)
			} else {
				var httpErr *HTTPError
				if errors.As(err, &httpErr) && httpErr.Status == 505 {
					_ = writeAll(conn, serverError505)
				} else {
					_ = writeAll(conn, serverError400)
				}
			}
			return
		}
		if a.cfg.MaxHeaderListSize > 0 && headEnd+4-consumed > a.cfg.MaxHeaderListSize {
			releaseCtx(ctx)
			_ = writeAll(conn, serverError431)
			return
		}

		_, err = parseHeadersLimit(accumulated[consumed:headEnd+4], &ctx.Header, a.cfg.MaxHeaderCount, a.cfg.StrictHeaderValueValidation)
		if err != nil {
			releaseCtx(ctx)
			_ = writeAll(conn, serverError400)
			return
		}
		if ctx.Header.UnsupportedTransferEncoding {
			releaseCtx(ctx)
			_ = writeAll(conn, serverError400)
			return
		}
		if ctx.Header.Chunked && ctx.Header.HasContentLength {
			releaseCtx(ctx)
			_ = writeAll(conn, serverError400)
			return
		}
		if ctx.Header.HasContentLength && ctx.Header.ContentLength < 0 {
			releaseCtx(ctx)
			_ = writeAll(conn, serverError400)
			return
		}
		// The request buffer stays alive until the handler completes, so zero-copy request state can
		// preserve OriginalURL without copying bytes on every request. Rewrite assigns
		// Header.URI to a separate target slice, leaving originalURI intact.
		origTarget := ctx.Header.RequestTarget
		if len(origTarget) == 0 {
			origTarget = ctx.Header.URI
		}
		if a.cfg.SafeParams {
			ctx.originalURI = append(ctx.originalURI[:0], origTarget...)
		} else {
			ctx.originalURI = origTarget
		}
		if ctx.Header.ContentLength > a.cfg.MaxRequestBodySize {
			releaseCtx(ctx)
			_ = writeAll(conn, serverError413)
			return
		}
		if a.cfg.RequestHeadHandler != nil {
			headCtx := connCtx
			cancelHead := func() {}
			if a.cfg.HandlerTimeout > 0 {
				headCtx, cancelHead = context.WithTimeout(connCtx, a.cfg.HandlerTimeout)
			}
			ctx.SetContext(headCtx)
			if a.cfg.WriteTimeout > 0 {
				_ = conn.SetWriteDeadline(time.Now().Add(a.cfg.WriteTimeout))
			}
			keepAlive := ctx.Header.KeepAlive
			ctx.Header.KeepAlive = false // an early response must close because the body remains unread
			accepted := a.runRequestHeadHandler(ctx)
			cancelHead()
			if !accepted {
				releaseCtx(ctx)
				return
			}
			ctx.Header.KeepAlive = keepAlive
		}

		// h2c upgrade: HTTP/1.1 Upgrade: h2c, Connection: Upgrade, HTTP2-Settings
		if !a.cfg.DisableHTTP2 && hasUpgradeH2C(ctx) {
			if a.cfg.DisableH2C {
				releaseCtx(ctx)
				_ = writeAll(conn, serverError400)
				return
			}
			leftover, bodyErr := readH2CUpgradeBody(conn, ctx, accumulated, headEnd+4, a.cfg.MaxRequestBodySize, a.cfg.RequestBodyTimeout)
			if bodyErr != nil {
				releaseCtx(ctx)
				if errors.Is(bodyErr, ErrBodyTooLarge) {
					_ = writeAll(conn, serverError413)
				} else {
					_ = writeAll(conn, serverError400)
				}
				return
			}
			h2c := newH2Conn(a, conn)
			if err := h2c.prepareUpgrade(ctx); err != nil {
				releaseCtx(ctx)
				_ = writeAll(conn, serverError400)
				return
			}
			upgrade := []byte("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: h2c\r\n\r\n")
			if err := writeAll(conn, upgrade); err != nil {
				releaseCtx(ctx)
				return
			}
			// Clear the read deadline set during HTTP/1.1 header parsing.
			// HTTP/2 manages its own deadlines.
			_ = conn.SetReadDeadline(time.Time{})
			// Pass any data read beyond the HTTP/1.1 headers (e.g. the start
			// of the HTTP/2 client preface) so it is not lost.
			a.setH2Conn(conn, h2c)
			releaseCtx(ctx)
			h2c.serve(leftover, false)
			return
		}

		expect := trimOWS(ctx.Header.Expect)
		if len(expect) > 0 {
			if !strEqFold(expect, "100-continue") || (!ctx.Header.Chunked && ctx.Header.ContentLength == 0) {
				releaseCtx(ctx)
				_ = writeAll(conn, serverError417)
				return
			}
			if err := writeAll(conn, []byte("HTTP/1.1 100 Continue\r\n\r\n")); err != nil {
				releaseCtx(ctx)
				return
			}
		}
		bodyStart := headEnd + 4
		bodyLen := ctx.Header.ContentLength
		var nextData []byte
		chunkedBody := ctx.Header.Chunked
		bodyBudget := a.cfg.RequestBodyTimeout
		if bodyBudget <= 0 {
			bodyBudget = a.cfg.ReadTimeout
		}
		if (chunkedBody || bodyLen > 0) && bodyBudget > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(bodyBudget)); err != nil {
				releaseCtx(ctx)
				return
			}
			readDeadlineArmed = true
		}

		if chunkedBody {
			body, leftover, trailers, readErr := readChunkedBody(conn, accumulated[bodyStart:], a.cfg.MaxRequestBodySize, bodyBudget)
			if readErr != nil {
				releaseCtx(ctx)
				if isTimeoutErr(readErr) {
					_ = writeAll(conn, serverError408)
				} else if errors.Is(readErr, ErrBodyTooLarge) {
					_ = writeAll(conn, serverError413)
				} else {
					_ = writeAll(conn, serverError400)
				}
				return
			}
			ctx.body, ctx.trailers, nextData = body, trailers, leftover
		} else if bodyLen > 0 {
			messageEnd := bodyStart + bodyLen
			if messageEnd <= cap(buf) {
				for len(accumulated) < messageEnd {
					n, err := conn.Read(buf[len(accumulated):cap(buf)])
					if n > 0 {
						accumulated = buf[:len(accumulated)+n]
					}
					if err != nil {
						if len(accumulated) >= messageEnd {
							break
						}
						if isTimeoutErr(err) && !a.closed.Load() {
							_ = writeAll(conn, serverError408)
						} else if err != io.EOF && !isExpectedConnErr(err) {
							a.emitError(err)
						}
						releaseCtx(ctx)
						return
					}
				}
				ctx.body = accumulated[bodyStart:messageEnd]
			} else {
				grown := make([]byte, messageEnd)
				copy(grown, accumulated)
				if len(accumulated) < messageEnd {
					if _, err := io.ReadFull(conn, grown[len(accumulated):]); err != nil {
						releaseCtx(ctx)
						if isTimeoutErr(err) && !a.closed.Load() {
							_ = writeAll(conn, serverError408)
						}
						return
					}
				}
				buf = grown
				accumulated = grown
				ctx.body = grown[bodyStart:messageEnd]
			}
		}
		if chunkedBody {
			ctx.upgradeBuffered = nextData
		} else {
			nextStart := bodyStart + bodyLen
			if nextStart < len(accumulated) {
				ctx.upgradeBuffered = accumulated[nextStart:]
			}
		}
		if !a.fastHTTP1 && a.draining.Load() {
			ctx.Header.KeepAlive = false
		}

		if a.cfg.WriteTimeout > 0 {
			if err := conn.SetWriteDeadline(time.Now().Add(a.cfg.WriteTimeout)); err != nil {
				releaseCtx(ctx)
				return
			}
		}

		if len(ctx.Header.Method) == 4 && ctx.Header.Method[0] == 'H' && ctx.Header.Method[1] == 'E' && ctx.Header.Method[2] == 'A' && ctx.Header.Method[3] == 'D' {
			ctx.flags |= ctxFlagHEAD
		}

		// Derive a per-request context only when it adds observable value. In the
		// default request path there is no per-request timeout, so reusing the
		// connection context avoids context.WithCancel allocation/work on every
		// request while still cancelling handlers on connection shutdown.
		if a.cfg.HandlerTimeout == 0 {
			ctx.SetContext(connCtx)
			a.dispatch(ctx)
		} else {
			reqCtx, reqCancel := context.WithTimeout(connCtx, a.cfg.HandlerTimeout)
			ctx.SetContext(reqCtx)
			a.dispatch(ctx)
			reqCancel()
		}
		keepAlive := ctx.Header.KeepAlive && !ctx.forceClose && !ctx.upgraded && !a.cfg.DisableKeepAlive
		if keepAlive && !a.fastHTTP1 {
			keepAlive = !a.draining.Load()
		}
		upgraded := ctx.upgraded

		releaseCtx(ctx)

		if upgraded || !keepAlive {
			return
		}

		if chunkedBody {
			if len(nextData) > cap(buf) {
				buf = make([]byte, len(nextData))
			}
			copy(buf, nextData)
			accumulated = buf[:len(nextData)]
		} else {
			nextStart := bodyStart + bodyLen
			if nextStart < len(accumulated) {
				copy(buf, accumulated[nextStart:])
				accumulated = buf[:len(accumulated)-nextStart]
			} else {
				accumulated = buf[:0]
			}
		}
		if state != nil && !a.fastHTTP1 {
			state.active.Store(false)
		}
	}
}

func (a *App) runRequestHeadHandler(ctx *DefaultCtx) (accepted bool) {
	defer func() {
		if r := recover(); r != nil {
			if !ctx.responded {
				a.cfg.ErrorHandler(ctx, NewPanicError(r))
			}
			accepted = false
		}
	}()
	ctx.params = ctx.params[:0]
	path := ctx.path()
	if a.router != nil {
		handler := a.router.FindBytes(ctx.Header.Method, path, &ctx.params)
		if handler == nil && bytesEqualFold(ctx.Header.Method, MethodHEADBytes) {
			ctx.params = ctx.params[:0]
			_ = a.router.FindBytes(MethodGETBytes, path, &ctx.params)
		}
	}
	err := a.cfg.RequestHeadHandler(ctx)
	if err != nil && !ctx.responded {
		a.cfg.ErrorHandler(ctx, err)
	}
	if err != nil && !ctx.responded {
		_ = ctx.Status(StatusInternalServerError).SendStatus(StatusInternalServerError)
	}
	return err == nil && !ctx.responded
}

func (a *App) defaultRecoveryMiddleware() HandlerFunc {
	return func(c Ctx) error {
		defer func() {
			if r := recover(); r != nil {
				if dc, ok := c.(*DefaultCtx); ok && !dc.responded {
					a.cfg.ErrorHandler(c, NewPanicError(r))
				} else {
					args := []any{"method", c.Method(), "path", c.Path(), "ip", c.IP(), "panic", "handler panic after response"}
					if a.cfg.Debug || a.cfg.ErrorOptions.LogInternal {
						args = append(args, "detail", RedactSecrets(fmt.Sprint(r)), "stack", RedactSecrets(string(debug.Stack())))
					}
					a.logger.Error("panic recovered", args...)
				}
			}
		}()
		return c.Next()
	}
}

func (a *App) dispatch(ctx *DefaultCtx) {
	a.dispatchCore(ctx)
}

func (a *App) dispatchCore(ctx *DefaultCtx) {
	ctx.params = ctx.params[:0]
	path := ctx.path()

	if a.router.hasPrebuilt && ctx.canRouteStaticPrebuilt() {
		if resp := a.router.FindPrebuiltResponseBytes(ctx.Header.Method, path); resp != nil {
			ctx.responded = true
			_ = writeAll(ctx.conn, resp)
			return
		}
	}

	handler := a.router.FindBytes(ctx.Header.Method, path, &ctx.params)
	if handler == nil && bytesEqualFold(ctx.Header.Method, MethodHEADBytes) {
		ctx.params = ctx.params[:0]
		handler = a.router.FindBytes(MethodGETBytes, path, &ctx.params)
	}

	if handler == nil {
		allowed := a.router.Allowed(path)
		if bytesEqualFold(ctx.Header.Method, MethodOPTIONSBytes) && len(path) == 1 && path[0] == '*' {
			allowed = a.router.Methods()
		}
		fallback := func(ctx Ctx) error {
			if len(allowed) == 0 {
				return a.cfg.NotFoundHandler(ctx)
			}
			ctx.Set("Allow", strings.Join(allowed, ", "))
			if bytesEqualFold(ctx.RequestHeader().Method, MethodOPTIONSBytes) {
				return a.cfg.OptionsHandler(ctx, allowed)
			}
			return a.cfg.MethodNotAllowed(ctx, allowed)
		}
		if a.hasMiddleware {
			handler = a.chain([]HandlerFunc{fallback})
		} else {
			handler = fallback
		}
	}

	err := handler(ctx)
	if errors.Is(err, ErrRewrite) {
		if ctx.responded {
			return
		}
		for rewrites := 1; ; rewrites++ {
			if rewrites > 8 {
				a.cfg.ErrorHandler(ctx, NewHTTPError(StatusLoopDetected, "REWRITE_LOOP", "Too many internal rewrites"))
				return
			}
			ctx.params = ctx.params[:0]
			path = ctx.path()
			handler = a.router.FindBytes(ctx.Header.Method, path, &ctx.params)
			if handler == nil && bytesEqualFold(ctx.Header.Method, MethodHEADBytes) {
				ctx.params = ctx.params[:0]
				handler = a.router.FindBytes(MethodGETBytes, path, &ctx.params)
			}
			if handler == nil {
				allowed := a.router.Allowed(path)
				fallback := func(ctx Ctx) error {
					if len(allowed) == 0 {
						return a.cfg.NotFoundHandler(ctx)
					}
					ctx.Set("Allow", strings.Join(allowed, ", "))
					if bytesEqualFold(ctx.RequestHeader().Method, MethodOPTIONSBytes) {
						return a.cfg.OptionsHandler(ctx, allowed)
					}
					return a.cfg.MethodNotAllowed(ctx, allowed)
				}
				if a.hasMiddleware {
					handler = a.chain([]HandlerFunc{fallback})
				} else {
					handler = fallback
				}
			}
			err = handler(ctx)
			if !errors.Is(err, ErrRewrite) {
				break
			}
			if ctx.responded {
				return
			}
		}
	}

	if err != nil {
		if !ctx.responded {
			a.cfg.ErrorHandler(ctx, err)
		}
	} else if !ctx.responded {
		_ = ctx.SendStatus(200)
	}
}

// chain combines global middleware + route-specific handlers into one HandlerFunc.
// For the common case (no middleware, single handler), returns handler directly — zero alloc.
func (a *App) chain(handlers []HandlerFunc) HandlerFunc {
	if len(a.middleware) == 0 && a.reliability == nil && len(handlers) == 1 {
		return handlers[0]
	}

	extra := 0
	if a.reliability != nil && len(handlers) > 0 {
		extra = 1
	}
	all := make([]HandlerFunc, 0, len(a.middleware)+len(handlers)+extra)
	all = append(all, a.middleware...)
	// Reliability needs the authenticated principal and normalized client IP,
	// but must still run before the endpoint performs business work. Treat the
	// final route handler as the endpoint and place reliability after all global
	// and route-level middleware.
	if a.reliability != nil && len(handlers) > 0 {
		all = append(all, handlers[:len(handlers)-1]...)
		all = append(all, a.reliability.Middleware())
		all = append(all, handlers[len(handlers)-1])
	} else {
		all = append(all, handlers...)
	}

	return func(ctx Ctx) error {
		dc, ok := ctx.(*DefaultCtx)
		if !ok {
			return NewHTTPError(StatusInternalServerError, "INVALID_CTX", "handler context does not support middleware chaining")
		}
		dc.handlers = all
		dc.handlerIndex = 0
		return dc.Next()
	}
}

// ── Helpers ────────────────────────────────────────────────────────────────

func findHeaderEnd(b []byte) int {
	// bytes.Index uses optimized runtime routines on supported architectures and
	// is materially cheaper than a Go byte-by-byte loop for pipelined traffic.
	return bytes.Index(b, strHeaderEnd)
}

func (a *App) emitError(err error) {
	for _, fn := range a.hooks.onError {
		fn(err)
	}
}

func hasUpgradeH2C(ctx *DefaultCtx) bool {
	upgrade := trimOWS(ctx.Header.Upgrade)
	if len(upgrade) == 0 || !strEqFold(upgrade, "h2c") {
		return false
	}
	settings := trimOWS(ctx.Header.HTTP2Settings)
	if len(settings) == 0 {
		return false
	}
	conn := trimOWS(ctx.Header.Peek(HeaderConnectionBytes))
	return hasHeaderToken(conn, "upgrade") && hasHeaderToken(conn, "http2-settings")
}

func readH2CUpgradeBody(conn net.Conn, ctx *DefaultCtx, buffered []byte, bodyStart, maxBody int, timeout time.Duration) ([]byte, error) {
	data := buffered[bodyStart:]
	if ctx.Header.Chunked {
		body, leftover, trailers, err := readChunkedBody(conn, data, maxBody, timeout)
		if err != nil {
			return nil, err
		}
		ctx.body, ctx.trailers = body, trailers
		return leftover, nil
	}
	length := ctx.Header.ContentLength
	if length > maxBody {
		return nil, ErrBodyTooLarge
	}
	if length == 0 {
		return data, nil
	}
	if len(data) < length {
		body := make([]byte, length)
		copy(body, data)
		if timeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(timeout))
		}
		if _, err := io.ReadFull(conn, body[len(data):]); err != nil {
			return nil, err
		}
		ctx.body = body
		return nil, nil
	}
	ctx.body = data[:length]
	return data[length:], nil
}

func isExpectedConnErr(err error) bool {
	return errors.Is(err, net.ErrClosed)
}

func isTimeoutErr(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func defaultErrorHandler(ctx Ctx, err error) {
	_ = ctx.SafeErrorResponse(err)
}

func defaultNotFoundHandler(ctx Ctx) error {
	// Skip logging for static file requests (js, css, json, png, jpg, etc.)
	if isStaticFile(ctx.Path()) {
		return ctx.SendStatus(404)
	}
	return NotFound("Resource not found")
}

func isStaticFile(path string) bool {
	n := len(path)
	if n < 3 {
		return false
	}

	// Find extension start from the end.
	// Stop at slash so `/api/users.v1/list` does not scan too far.
	dot := -1
	for i := n - 1; i >= 0; i-- {
		switch path[i] {
		case '.':
			dot = i
			i = -1
		case '/', '\\':
			i = -1
		}
	}

	if dot < 0 || dot == n-1 {
		return false
	}

	ext := path[dot:]

	switch len(ext) {
	case 3:
		return eqExt(ext, ".js") ||
			eqExt(ext, ".gz")

	case 4:
		switch lower(ext[1]) {
		case 'c':
			return eqExt(ext, ".css") || eqExt(ext, ".csv")
		case 'g':
			return eqExt(ext, ".gif")
		case 'i':
			return eqExt(ext, ".ico")
		case 'j':
			return eqExt(ext, ".jpg")
		case 'm':
			return eqExt(ext, ".mp3") || eqExt(ext, ".mp4")
		case 'o':
			return eqExt(ext, ".ogg") || eqExt(ext, ".otf")
		case 'p':
			return eqExt(ext, ".png") || eqExt(ext, ".pdf")
		case 's':
			return eqExt(ext, ".svg")
		case 't':
			return eqExt(ext, ".ttf") || eqExt(ext, ".txt") || eqExt(ext, ".tar")
		case 'w':
			return eqExt(ext, ".wav")
		case 'x':
			return eqExt(ext, ".xml")
		case 'z':
			return eqExt(ext, ".zip")
		}

	case 5:
		switch lower(ext[1]) {
		case 'h':
			return eqExt(ext, ".html")
		case 'j':
			return eqExt(ext, ".json") || eqExt(ext, ".jpeg")
		case 'w':
			return eqExt(ext, ".webm") || eqExt(ext, ".webp") || eqExt(ext, ".woff")
		}

	case 6:
		return eqExt(ext, ".woff2")
	}

	return false
}

func eqExt(s, ext string) bool {
	if len(s) != len(ext) {
		return false
	}
	for i := 0; i < len(ext); i++ {
		if lower(s[i]) != ext[i] {
			return false
		}
	}
	return true
}

func lower(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}

func defaultMethodNotAllowedHandler(ctx Ctx, allowed []string) error {
	ctx.Set("Allow", strings.Join(allowed, ", "))
	return MethodNotAllowed("Method not allowed")
}

func defaultOptionsHandler(ctx Ctx, allowed []string) error {
	ctx.Set("Allow", strings.Join(allowed, ", "))
	return ctx.SendStatus(StatusNoContent)
}

func (a *App) recordError(code string) {
	if code == "" {
		code = "UNKNOWN"
	}
	v, _ := a.errorCounts.LoadOrStore(code, &atomic.Uint64{})
	v.(*atomic.Uint64).Add(1)
}

// ErrorCount returns the number of errors rendered for a stable error code.
func (a *App) ErrorCount(code string) uint64 {
	v, ok := a.errorCounts.Load(code)
	if !ok {
		return 0
	}
	return v.(*atomic.Uint64).Load()
}

// Reliability returns the configured reliability runtime, if enabled.
func (a *App) Reliability() *Reliability { return a.reliability }

// Queue returns the embedded durable queue when reliability queue support is enabled.
func (a *App) Queue() *DurableQueue {
	if a.reliability == nil {
		return nil
	}
	return a.reliability.Queue()
}

// Pre-allocated 400 error response
var serverError400 = []byte("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
var serverError408 = []byte("HTTP/1.1 408 Request Timeout\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
var serverError413 = []byte("HTTP/1.1 413 Content Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
var serverError417 = []byte("HTTP/1.1 417 Expectation Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
var serverError431 = []byte("HTTP/1.1 431 Request Header Fields Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
var serverError503 = []byte("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
var serverError505 = []byte("HTTP/1.1 505 HTTP Version Not Supported\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
var plainTextCT = []byte("text/plain; charset=utf-8")
