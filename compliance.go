package fh

import (
	"crypto/tls"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

// Mode controls framework safety defaults and config validation severity.
type Mode string

const (
	// ModeFast keeps raw throughput defaults for trusted benchmark/edge use.
	ModeFast        Mode = "fast"
	ModeDevelopment Mode = "development"
	ModeProduction  Mode = "production"
	// ModeEnterprise enables strict protocol, audit/reliability and compliance evidence defaults.
	ModeEnterprise Mode = "enterprise"
	ModeStrict     Mode = "strict"
)

// ComplianceProfile selects a built-in security/compliance baseline. fh does
// not certify an application; profiles enable controls and expose evidence that
// maps to common enterprise/security review requirements.
type ComplianceProfile string

const (
	ComplianceBusiness       ComplianceProfile = "business"
	ComplianceProfessional   ComplianceProfile = "professional"
	ComplianceEnterprise     ComplianceProfile = "enterprise"
	ComplianceSecurityStrict ComplianceProfile = "security_strict"
	ComplianceFinancial      ComplianceProfile = "financial"
	ComplianceHealthcare     ComplianceProfile = "healthcare"
	ComplianceGovernment     ComplianceProfile = "government"
	ComplianceInternal       ComplianceProfile = "internal_service"
	CompliancePublicAPI      ComplianceProfile = "public_api"
	ComplianceWebhook        ComplianceProfile = "webhook_receiver"
)

// ComplianceConfig configures compliance-first runtime behavior.
type ComplianceConfig struct {
	Enabled         bool              `json:"enabled"`
	Profile         ComplianceProfile `json:"profile,omitempty"`
	Strict          bool              `json:"strict,omitempty"`
	ExposeEndpoints bool              `json:"expose_endpoints,omitempty"`
	EndpointPrefix  string            `json:"endpoint_prefix,omitempty"`

	// EndpointAuth guards the compliance/health/runtime introspection routes
	// mounted by ExposeEndpoints. These routes expose the full route table
	// (including which routes lack auth), config internals, and queue
	// depth — a reconnaissance goldmine if left unauthenticated. Set this
	// (e.g. to an mw/basicauth, mw/apikey, or IP-allowlist handler) before
	// enabling ExposeEndpoints in any deployment reachable from outside a
	// trusted network. Left empty, ValidateSecurity reports a critical
	// finding and a startup warning is logged.
	EndpointAuth []HandlerFunc `json:"-"`

	// FailOnCritical panics at app construction when ValidateSecurity finds a
	// critical production issue. This is useful in CI and strict deployments.
	FailOnCritical bool `json:"fail_on_critical,omitempty"`

	// SecurityContact is the contact email/address for the security.txt endpoint.
	SecurityContact string `json:"security_contact,omitempty"`
	// SecurityPolicyURL is the URL of the security policy for security.txt.
	SecurityPolicyURL string `json:"security_policy_url,omitempty"`
}

// ComplianceControl maps an fh runtime capability to an external control family.
type ComplianceControl struct {
	ID          string   `json:"id"`
	Standard    string   `json:"standard"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Implemented bool     `json:"implemented"`
	Components  []string `json:"components,omitempty"`
	Evidence    []string `json:"evidence,omitempty"`
}

// SecurityFinding is returned by ValidateSecurity and the compliance endpoints.
type SecurityFinding struct {
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Message  string `json:"message"`
	Fix      string `json:"fix,omitempty"`
	Route    string `json:"route,omitempty"`
}

// ComplianceReport is the complete runtime compliance evidence document.
type ComplianceReport struct {
	GeneratedAt time.Time           `json:"generated_at"`
	Mode        Mode                `json:"mode"`
	Profile     ComplianceProfile   `json:"profile"`
	Controls    []ComplianceControl `json:"controls"`
	Findings    []SecurityFinding   `json:"findings"`
	Routes      []RouteInfo         `json:"routes"`
	Config      SafeConfig          `json:"config"`
}

// SafeConfig is a redacted summary of runtime configuration.
type SafeConfig struct {
	SecureByDefault       bool              `json:"secure_by_default"`
	ReadTimeout           string            `json:"read_timeout,omitempty"`
	ReadHeaderTimeout     string            `json:"read_header_timeout,omitempty"`
	RequestBodyTimeout    string            `json:"request_body_timeout,omitempty"`
	WriteTimeout          string            `json:"write_timeout,omitempty"`
	HandlerTimeout        string            `json:"handler_timeout,omitempty"`
	IdleTimeout           string            `json:"idle_timeout,omitempty"`
	TLSHandshakeTimeout   string            `json:"tls_handshake_timeout,omitempty"`
	HTTP2IdleTimeout      string            `json:"http2_idle_timeout,omitempty"`
	MaxConnections        int               `json:"max_connections"`
	MaxConnectionsPerIP   int               `json:"max_connections_per_ip"`
	H2CEnabled            bool              `json:"h2c_enabled"`
	MaxRequestBodySize    int               `json:"max_request_body_size"`
	MaxHeaderListSize     int               `json:"max_header_list_size"`
	MaxHeaderCount        int               `json:"max_header_count"`
	MaxRequestLineSize    int               `json:"max_request_line_size"`
	ReliabilityEnabled    bool              `json:"reliability_enabled"`
	JournalEnabled        bool              `json:"journal_enabled"`
	IdempotencyEnabled    bool              `json:"idempotency_enabled"`
	QueueEnabled          bool              `json:"queue_enabled"`
	AuditEnabled          bool              `json:"audit_enabled"`
	RedactionEnabled      bool              `json:"redaction_enabled"`
	ComplianceEnabled     bool              `json:"compliance_enabled"`
	ComplianceProfile     ComplianceProfile `json:"compliance_profile,omitempty"`
	SecurityEventEndpoint bool              `json:"security_event_endpoint"`
	KernelEnabled         bool              `json:"kernel_enabled"`
	KernelBackend         KernelBackend     `json:"kernel_backend,omitempty"`
}

func applyComplianceDefaults(cfg *Config) {
	if cfg == nil {
		return
	}
	if cfg.Mode == "" {
		cfg.Mode = ModeProduction
	}
	if cfg.Compliance.EndpointPrefix == "" {
		cfg.Compliance.EndpointPrefix = "/_fh"
	}
	if cfg.Compliance.ExposeEndpoints && len(cfg.Compliance.EndpointAuth) == 0 {
		log.Printf("fh: Compliance.ExposeEndpoints is enabled with no Compliance.EndpointAuth — compliance endpoints will require auth middleware to be mounted")
	}
	if cfg.Compliance.Enabled && cfg.Compliance.Profile == "" {
		cfg.Compliance.Profile = ComplianceProfessional
	}
	if !cfg.Redaction.Enabled && (cfg.Compliance.Enabled || cfg.Mode == ModeProduction || cfg.Mode == ModeStrict || cfg.Mode == ModeEnterprise) {
		cfg.Redaction = DefaultRedactionConfig()
	}
	if cfg.Mode == ModeProduction || cfg.Mode == ModeEnterprise || cfg.Mode == ModeStrict {
		if cfg.MaxConnectionsPerIP <= 0 {
			cfg.MaxConnectionsPerIP = 100
		}
		if cfg.ReadHeaderTimeout == 0 {
			cfg.ReadHeaderTimeout = 5 * time.Second
		}
		if cfg.ReadTimeout == 0 {
			cfg.ReadTimeout = 10 * time.Second
		}
		if cfg.WriteTimeout == 0 {
			cfg.WriteTimeout = 30 * time.Second
		}
		if cfg.IdleTimeout == 0 {
			cfg.IdleTimeout = 60 * time.Second
		}
		if cfg.RequestBodyTimeout == 0 {
			cfg.RequestBodyTimeout = 10 * time.Second
		}
		if cfg.TLSHandshakeTimeout == 0 {
			cfg.TLSHandshakeTimeout = 10 * time.Second
		}
		if cfg.HTTP2IdleTimeout == 0 {
			cfg.HTTP2IdleTimeout = 60 * time.Second
		}
		cfg.SendDateHeader = true
	}
	if cfg.Mode == ModeFast {
		cfg.SendDateHeader = false
	}
	if cfg.Mode == ModeEnterprise {
		cfg.Compliance.Enabled = true
		if cfg.Compliance.Profile == "" {
			cfg.Compliance.Profile = ComplianceEnterprise
		}
		cfg.Compliance.Strict = true
		cfg.Compliance.ExposeEndpoints = true
	}
	if cfg.Compliance.Enabled {
		if cfg.ReadHeaderTimeout == 0 {
			cfg.ReadHeaderTimeout = 5 * time.Second
		}
		if cfg.ReadTimeout == 0 {
			cfg.ReadTimeout = 10 * time.Second
		}
		if cfg.WriteTimeout == 0 {
			cfg.WriteTimeout = 30 * time.Second
		}
		if cfg.IdleTimeout == 0 {
			cfg.IdleTimeout = 60 * time.Second
		}
		if cfg.MaxRequestBodySize <= 0 || cfg.MaxRequestBodySize > 10<<20 {
			cfg.MaxRequestBodySize = 10 << 20
		}
		if cfg.MaxHeaderListSize <= 0 || cfg.MaxHeaderListSize > 32<<10 {
			cfg.MaxHeaderListSize = 32 << 10
		}
		if cfg.MaxHeaderCount <= 0 || cfg.MaxHeaderCount > 100 {
			cfg.MaxHeaderCount = 100
		}
		if cfg.MaxRequestLineSize <= 0 || cfg.MaxRequestLineSize > 8<<10 {
			cfg.MaxRequestLineSize = 8 << 10
		}
		if !cfg.Reliability.Enabled {
			cfg.Reliability.Enabled = true
		}
		if !cfg.Reliability.JournalEnabled && !cfg.Reliability.IdempotencyEnabled && !cfg.Reliability.QueueEnabled {
			cfg.Reliability.JournalEnabled = true
			cfg.Reliability.IdempotencyEnabled = true
			cfg.Reliability.QueueEnabled = true
		}
		if !cfg.Audit.Enabled {
			cfg.Audit.Enabled = true
		}
	}
	if cfg.Mode == ModeStrict || cfg.Mode == ModeEnterprise || cfg.Compliance.Strict || cfg.Compliance.Profile == ComplianceSecurityStrict || cfg.Compliance.Profile == ComplianceFinancial || cfg.Compliance.Profile == ComplianceHealthcare || cfg.Compliance.Profile == ComplianceGovernment {
		cfg.Compliance.Strict = true
		cfg.SendDateHeader = true
		if cfg.Reliability.IdempotencyTTL == 0 {
			cfg.Reliability.IdempotencyTTL = 24 * time.Hour
		}
		if cfg.Audit.Retention == 0 {
			cfg.Audit.Retention = 365 * 24 * time.Hour
		}
	}
}

// applySecureDefaults resolves the opt-in security baseline at construction
// time. Keep this free of request-time feature switches: parsers and response
// writers should only observe the resulting concrete configuration.
func applySecureDefaults(cfg *Config) {
	if cfg == nil || !cfg.SecureByDefault {
		return
	}

	// SecureByDefault wins over benchmark/development modes. Enterprise already
	// includes the strict baseline plus its compliance facilities.
	if cfg.Mode != ModeEnterprise {
		cfg.Mode = ModeStrict
	}
	cfg.Environment = EnvProduction
	cfg.Debug = false
	cfg.ErrorOptions.Environment = EnvProduction
	cfg.ErrorOptions.ExposeDebug = false
	cfg.ErrorOptions.ExposeStackTrace = false
	cfg.ErrorOptions.ExposeCauses = false
	cfg.DisablePanicRecovery = false
	cfg.SendDateHeader = true
	cfg.ServerHeader = ""
	cfg.DisableH2C = true

	// Bound every untrusted request dimension. Preserve stricter caller values,
	// but do not allow permissive values to weaken the baseline.
	if cfg.ReadBufferSize <= 0 || cfg.ReadBufferSize > 64<<10 {
		cfg.ReadBufferSize = 16 << 10
	}
	if cfg.MaxConnections <= 0 || cfg.MaxConnections > 10_000 {
		cfg.MaxConnections = 10_000
	}
	if cfg.MaxConnectionsPerIP <= 0 || cfg.MaxConnectionsPerIP > 100 {
		cfg.MaxConnectionsPerIP = 100
	}
	if cfg.MaxRequestBodySize <= 0 || cfg.MaxRequestBodySize > 4<<20 {
		cfg.MaxRequestBodySize = 4 << 20
	}
	if cfg.MaxHeaderListSize <= 0 || cfg.MaxHeaderListSize > 32<<10 {
		cfg.MaxHeaderListSize = 32 << 10
	}
	if cfg.MaxHeaderCount <= 0 || cfg.MaxHeaderCount > 64 {
		cfg.MaxHeaderCount = 64
	}
	if cfg.MaxRequestLineSize <= 0 || cfg.MaxRequestLineSize > 8<<10 {
		cfg.MaxRequestLineSize = 8 << 10
	}
	if cfg.MaxConcurrentStreams == 0 || cfg.MaxConcurrentStreams > 128 {
		cfg.MaxConcurrentStreams = 128
	}
	if cfg.ReadHeaderTimeout <= 0 || cfg.ReadHeaderTimeout > 5*time.Second {
		cfg.ReadHeaderTimeout = 5 * time.Second
	}
	if cfg.ReadTimeout <= 0 || cfg.ReadTimeout > 10*time.Second {
		cfg.ReadTimeout = 10 * time.Second
	}
	if cfg.WriteTimeout <= 0 || cfg.WriteTimeout > 30*time.Second {
		cfg.WriteTimeout = 30 * time.Second
	}
	if cfg.IdleTimeout <= 0 || cfg.IdleTimeout > 60*time.Second {
		cfg.IdleTimeout = 60 * time.Second
	}
	if cfg.RequestBodyTimeout <= 0 || cfg.RequestBodyTimeout > 10*time.Second {
		cfg.RequestBodyTimeout = 10 * time.Second
	}
	if cfg.TLSHandshakeTimeout <= 0 || cfg.TLSHandshakeTimeout > 10*time.Second {
		cfg.TLSHandshakeTimeout = 10 * time.Second
	}
	if cfg.HTTP2IdleTimeout <= 0 || cfg.HTTP2IdleTimeout > 60*time.Second {
		cfg.HTTP2IdleTimeout = 60 * time.Second
	}
}

func (a *App) SafeConfig() SafeConfig {
	if a == nil {
		return SafeConfig{}
	}
	return SafeConfig{
		SecureByDefault:       a.cfg.SecureByDefault,
		ReadTimeout:           a.cfg.ReadTimeout.String(),
		ReadHeaderTimeout:     a.cfg.ReadHeaderTimeout.String(),
		RequestBodyTimeout:    a.cfg.RequestBodyTimeout.String(),
		WriteTimeout:          a.cfg.WriteTimeout.String(),
		HandlerTimeout:        a.cfg.HandlerTimeout.String(),
		IdleTimeout:           a.cfg.IdleTimeout.String(),
		TLSHandshakeTimeout:   a.cfg.TLSHandshakeTimeout.String(),
		HTTP2IdleTimeout:      a.cfg.HTTP2IdleTimeout.String(),
		MaxConnections:        a.cfg.MaxConnections,
		MaxConnectionsPerIP:   a.cfg.MaxConnectionsPerIP,
		H2CEnabled:            !a.cfg.DisableHTTP2 && !a.cfg.DisableH2C,
		MaxRequestBodySize:    a.cfg.MaxRequestBodySize,
		MaxHeaderListSize:     a.cfg.MaxHeaderListSize,
		MaxHeaderCount:        a.cfg.MaxHeaderCount,
		MaxRequestLineSize:    a.cfg.MaxRequestLineSize,
		ReliabilityEnabled:    a.cfg.Reliability.Enabled,
		JournalEnabled:        a.cfg.Reliability.JournalEnabled,
		IdempotencyEnabled:    a.cfg.Reliability.IdempotencyEnabled,
		QueueEnabled:          a.cfg.Reliability.QueueEnabled,
		AuditEnabled:          a.cfg.Audit.Enabled,
		RedactionEnabled:      a.cfg.Redaction.Enabled,
		ComplianceEnabled:     a.cfg.Compliance.Enabled,
		ComplianceProfile:     a.cfg.Compliance.Profile,
		SecurityEventEndpoint: a.cfg.Compliance.ExposeEndpoints,
		KernelEnabled:         a.cfg.Kernel.Enabled,
		KernelBackend:         a.cfg.Kernel.Backend,
	}
}

func (a *App) ComplianceControls() []ComplianceControl {
	if a == nil {
		return nil
	}
	controls := []ComplianceControl{
		{"FH-SEC-001", "OWASP API2/ASVS", "Authentication primitives", "Principal model, API key/session/Bearer/mTLS integration hooks and auth middleware.", true, []string{"Principal", "RequireAuth", "mw/apikey", "mw/session"}, []string{"/_fh/routes", "/_fh/compliance"}},
		{"FH-SEC-002", "OWASP API1/API5", "Authorization primitives", "Scope, role, object and tenant-aware authorization middleware and route policy metadata.", true, []string{"RequireScope", "Authorize", "TenantResolver", "RouteSecurity"}, []string{"/_fh/routes"}},
		{"FH-SEC-003", "OWASP API4", "Resource governance", "Timeouts, body/header/request-line limits, rate-limit middleware and queue payload limits.", a.cfg.MaxRequestBodySize > 0 && a.cfg.MaxHeaderListSize > 0, []string{"Config", "mw/timeout", "mw/bodylimit", "mw/ratelimiter"}, []string{"/_fh/config/safe"}},
		{"FH-SEC-004", "OWASP API8", "Transport and security headers", "TLS/mTLS capable listener and strict browser/API security headers middleware.", true, []string{"ListenTLS", "ServeTLS", "mw/security"}, []string{"/_fh/config/safe"}},
		{"FH-AUD-001", "SOC2/ISO/NIST", "Request and audit trail", "Append-only request journal plus business audit ledger helpers with redaction support.", a.cfg.Reliability.JournalEnabled || a.cfg.Audit.Enabled, []string{"RequestJournal", "AuditSink", "Ledger"}, []string{"/_fh/audit", "/_fh/compliance"}},
		{"FH-REL-001", "Enterprise reliability", "Idempotency and response replay", "Unsafe-method idempotency, request hash conflict detection, response replay and TTL cleanup hooks.", a.cfg.Reliability.IdempotencyEnabled, []string{"IdempotencyStore", "ReliabilityPolicy"}, []string{"/_fh/config/safe"}},
		{"FH-REL-002", "Enterprise reliability", "Durable async queue", "Embedded durable queue with pending/processing/done/failed states, event log, retries and crash recovery.", a.cfg.Reliability.QueueEnabled, []string{"DurableQueue", "FileQueueStorage", "Outbox", "Inbox"}, []string{"/_fh/queue/stats"}},
		{"FH-PRV-001", "GDPR-style privacy", "Data classification and redaction", "Route data policy, sensitive key redaction, secure envelope and encrypted-at-rest helpers.", a.cfg.Redaction.Enabled, []string{"DataPolicy", "Redactor", "SecureEnvelope"}, []string{"/_fh/routes"}},
		{"FH-OBS-001", "NIST/SOC2", "Health and runtime evidence", "Health, readiness, route inventory, OpenAPI, runtime and compliance endpoints.", true, []string{"EnableHealth", "EnableRouteList", "EnableOpenAPI", "EnableRuntime"}, []string{"/_fh/health", "/_fh/ready", "/_fh/runtime"}},
	}
	return controls
}

func (a *App) ValidateSecurity() []SecurityFinding {
	if a == nil {
		return nil
	}
	var f []SecurityFinding
	prod := a.cfg.Mode == ModeProduction || a.cfg.Mode == ModeStrict || a.cfg.Mode == ModeEnterprise || a.cfg.Compliance.Enabled
	if prod && a.cfg.Debug {
		f = append(f, SecurityFinding{"critical", "DEBUG_ENABLED", "debug error exposure is enabled in production/compliance mode", "disable Config.Debug", ""})
	}
	if prod && a.cfg.ReadTimeout == 0 {
		f = append(f, SecurityFinding{"high", "READ_TIMEOUT_MISSING", "ReadTimeout is disabled", "set Config.ReadTimeout", ""})
	}
	if prod && a.cfg.ReadHeaderTimeout == 0 {
		f = append(f, SecurityFinding{"high", "READ_HEADER_TIMEOUT_MISSING", "ReadHeaderTimeout is disabled", "set Config.ReadHeaderTimeout", ""})
	}
	if prod && a.cfg.WriteTimeout == 0 {
		f = append(f, SecurityFinding{"high", "WRITE_TIMEOUT_MISSING", "WriteTimeout is disabled", "set Config.WriteTimeout", ""})
	}
	if prod && a.cfg.RequestBodyTimeout <= 0 {
		f = append(f, SecurityFinding{"high", "BODY_READ_TIMEOUT_MISSING", "RequestBodyTimeout is disabled", "set Config.RequestBodyTimeout", ""})
	}
	if prod && a.cfg.TLSHandshakeTimeout <= 0 {
		f = append(f, SecurityFinding{"high", "TLS_HANDSHAKE_TIMEOUT_MISSING", "TLSHandshakeTimeout is disabled", "set Config.TLSHandshakeTimeout", ""})
	}
	if prod && !a.cfg.DisableHTTP2 && a.cfg.HTTP2IdleTimeout <= 0 {
		f = append(f, SecurityFinding{"high", "HTTP2_IDLE_TIMEOUT_MISSING", "HTTP2IdleTimeout is disabled", "set Config.HTTP2IdleTimeout", ""})
	}
	if prod && a.cfg.MaxConnections <= 0 {
		f = append(f, SecurityFinding{"high", "CONNECTION_LIMIT_MISSING", "MaxConnections is unbounded", "set Config.MaxConnections", ""})
	}
	if prod && a.cfg.MaxConnectionsPerIP <= 0 {
		f = append(f, SecurityFinding{"medium", "PER_IP_CONNECTION_LIMIT_MISSING", "simultaneous connections from one socket peer are unbounded", "set Config.MaxConnectionsPerIP; use identity-aware limits behind shared NAT", ""})
	}
	if prod && !a.cfg.DisableHTTP2 && !a.cfg.DisableH2C {
		f = append(f, SecurityFinding{"medium", "H2C_ENABLED", "cleartext HTTP/2 prior knowledge and upgrade are enabled", "disable h2c on public listeners with Config.DisableH2C", ""})
	}
	if prod && a.cfg.MaxRequestBodySize <= 0 {
		f = append(f, SecurityFinding{"critical", "BODY_LIMIT_MISSING", "MaxRequestBodySize is not enforced", "set Config.MaxRequestBodySize", ""})
	}
	if prod && !a.cfg.Redaction.Enabled {
		f = append(f, SecurityFinding{"high", "REDACTION_DISABLED", "redaction is disabled", "enable Config.Redaction", ""})
	}
	if a.cfg.Compliance.Enabled && !a.cfg.Reliability.Enabled {
		f = append(f, SecurityFinding{"high", "RELIABILITY_DISABLED", "compliance mode should enable reliability", "enable Config.Reliability", ""})
	}
	if a.cfg.Compliance.ExposeEndpoints && len(a.cfg.Compliance.EndpointAuth) == 0 {
		f = append(f, SecurityFinding{"critical", "COMPLIANCE_ENDPOINTS_UNAUTHENTICATED", "compliance endpoint exposure was requested without authentication; fail-closed endpoint mounting left the endpoints disabled", "set Config.Compliance.EndpointAuth (or fh.WithComplianceEndpointAuth) to an auth middleware", ""})
	}
	for _, r := range a.Routes() {
		unsafe := r.Method == "POST" || r.Method == "PUT" || r.Method == "PATCH" || r.Method == "DELETE"
		if unsafe && r.Security.IdempotencyRequired && !a.cfg.Reliability.IdempotencyEnabled {
			f = append(f, SecurityFinding{"high", "ROUTE_IDEMPOTENCY_UNAVAILABLE", "route requires idempotency but idempotency store is disabled", "enable Reliability.IdempotencyEnabled", r.Method + " " + r.Path})
		}
		if a.cfg.Compliance.Strict && strings.Contains(r.Path, "admin") && !r.Security.AuthRequired {
			f = append(f, SecurityFinding{"medium", "ADMIN_ROUTE_AUTH_UNKNOWN", "admin-like route has no auth metadata", "attach fh.RouteSecurity/AuthRequired or auth middleware", r.Method + " " + r.Path})
		}
	}
	sort.Slice(f, func(i, j int) bool { return severityRank(f[i].Severity) > severityRank(f[j].Severity) })
	return f
}

func severityRank(s string) int {
	switch strings.ToLower(s) {
	case "critical":
		return 4
	case "high":
		return 3
	case "medium":
		return 2
	case "low":
		return 1
	default:
		return 0
	}
}

func (a *App) ComplianceReport() ComplianceReport {
	return ComplianceReport{GeneratedAt: time.Now().UTC(), Mode: a.cfg.Mode, Profile: a.cfg.Compliance.Profile, Controls: a.ComplianceControls(), Findings: a.ValidateSecurity(), Routes: a.Routes(), Config: a.SafeConfig()}
}

// withHandlers returns a fresh slice combining middleware with h, safe to
// pass to multiple a.Get calls without the handlers aliasing/overwriting
// each other's slot in a shared backing array.
func withHandlers(middleware []HandlerFunc, h HandlerFunc) []HandlerFunc {
	out := make([]HandlerFunc, len(middleware)+1)
	copy(out, middleware)
	out[len(middleware)] = h
	return out
}

// EnableComplianceEndpoints mounts compliance/config introspection routes.
// These expose security posture details (which routes require auth, redaction
// /audit status, config limits) — pass an auth middleware (e.g. mw/basicauth,
// mw/apikey, IP allowlist) so this route group isn't reachable by anyone who
// can reach the server.
func (a *App) EnableComplianceEndpoints(prefix string, middleware ...HandlerFunc) *App {
	if len(middleware) == 0 && len(a.cfg.Compliance.EndpointAuth) == 0 {
		log.Printf("fh: EnableComplianceEndpoints called with no auth middleware and no Compliance.EndpointAuth — /_fh/* endpoints NOT mounted; set Config.Compliance.EndpointAuth or pass auth middleware to enable")
		return a
	}
	if len(middleware) == 0 {
		middleware = a.cfg.Compliance.EndpointAuth
	}
	a.cfg.Compliance.EndpointAuth = middleware
	if prefix == "" {
		prefix = "/_fh"
	}
	prefix = strings.TrimRight(prefix, "/")
	a.Get(prefix+"/compliance", withHandlers(middleware, func(c Ctx) error { return c.JSON(a.ComplianceReport()) })...)
	a.Get(prefix+"/compliance/controls", withHandlers(middleware, func(c Ctx) error { return c.JSON(a.ComplianceControls()) })...)
	a.Get(prefix+"/compliance/findings", withHandlers(middleware, func(c Ctx) error { return c.JSON(a.ValidateSecurity()) })...)
	a.Get(prefix+"/config/safe", withHandlers(middleware, func(c Ctx) error { return c.JSON(a.SafeConfig()) })...)
	contact := a.cfg.Compliance.SecurityContact
	if contact == "" {
		contact = "security@example.com"
	}
	policyURL := a.cfg.Compliance.SecurityPolicyURL
	if policyURL == "" {
		policyURL = "https://example.com/security-policy"
	}
	a.Get(prefix+"/.well-known/security.txt", withHandlers(middleware, func(c Ctx) error {
		c.Set("Content-Type", "text/plain; charset=utf-8")
		c.Set("Cache-Control", "no-store")
		return c.SendString("Contact: " + contact + "\nExpires: " + time.Now().Add(365*24*time.Hour).Format(time.RFC3339) + "\nPreferred-Languages: en\nPolicy: " + policyURL + "\n")
	})...)
	return a
}

func hasCritical(findings []SecurityFinding) bool {
	for _, f := range findings {
		if strings.EqualFold(f.Severity, "critical") {
			return true
		}
	}
	return false
}

func validateTLSConfig(cfg *tls.Config) error {
	if cfg == nil {
		return nil
	}
	if cfg.MinVersion != 0 && cfg.MinVersion < tls.VersionTLS12 {
		return fmt.Errorf("fh: TLS MinVersion must be TLS 1.2 or newer")
	}
	if cfg.MaxVersion != 0 && cfg.MinVersion != 0 && cfg.MaxVersion < cfg.MinVersion {
		return fmt.Errorf("fh: TLS MaxVersion must not be lower than MinVersion")
	}
	return nil
}
