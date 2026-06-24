package fh

import (
	"crypto/tls"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Mode controls framework safety defaults and config validation severity.
type Mode string

const (
	ModeDevelopment Mode = "development"
	ModeProduction  Mode = "production"
	ModeStrict      Mode = "strict"
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

	// FailOnCritical panics at app construction when ValidateSecurity finds a
	// critical production issue. This is useful in CI and strict deployments.
	FailOnCritical bool `json:"fail_on_critical,omitempty"`
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
	ReadTimeout           string            `json:"read_timeout,omitempty"`
	WriteTimeout          string            `json:"write_timeout,omitempty"`
	IdleTimeout           string            `json:"idle_timeout,omitempty"`
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
}

func applyComplianceDefaults(cfg *Config) {
	if cfg == nil {
		return
	}
	if cfg.Mode == "" {
		if cfg.Compliance.Enabled || cfg.Compliance.Profile != "" {
			cfg.Mode = ModeProduction
		} else {
			cfg.Mode = ModeProduction
		}
	}
	if cfg.Compliance.EndpointPrefix == "" {
		cfg.Compliance.EndpointPrefix = "/_fh"
	}
	if cfg.Compliance.Enabled && cfg.Compliance.Profile == "" {
		cfg.Compliance.Profile = ComplianceProfessional
	}
	if !cfg.Redaction.Enabled && (cfg.Compliance.Enabled || cfg.Mode == ModeProduction || cfg.Mode == ModeStrict) {
		cfg.Redaction = DefaultRedactionConfig()
	}
	if cfg.Compliance.Enabled {
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
	if cfg.Mode == ModeStrict || cfg.Compliance.Strict || cfg.Compliance.Profile == ComplianceSecurityStrict || cfg.Compliance.Profile == ComplianceFinancial || cfg.Compliance.Profile == ComplianceHealthcare || cfg.Compliance.Profile == ComplianceGovernment {
		cfg.Compliance.Strict = true
		cfg.StrictHeaderValueValidation = true
		cfg.SendDateHeader = true
		if cfg.Reliability.IdempotencyTTL == 0 {
			cfg.Reliability.IdempotencyTTL = 24 * time.Hour
		}
		if cfg.Audit.Retention == 0 {
			cfg.Audit.Retention = 365 * 24 * time.Hour
		}
	}
}

func (a *App) SafeConfig() SafeConfig {
	if a == nil {
		return SafeConfig{}
	}
	return SafeConfig{
		ReadTimeout:           a.cfg.ReadTimeout.String(),
		WriteTimeout:          a.cfg.WriteTimeout.String(),
		IdleTimeout:           a.cfg.IdleTimeout.String(),
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
	prod := a.cfg.Mode == ModeProduction || a.cfg.Mode == ModeStrict || a.cfg.Compliance.Enabled
	if prod && a.cfg.Debug {
		f = append(f, SecurityFinding{"critical", "DEBUG_ENABLED", "debug error exposure is enabled in production/compliance mode", "disable Config.Debug", ""})
	}
	if prod && a.cfg.ReadTimeout == 0 {
		f = append(f, SecurityFinding{"high", "READ_TIMEOUT_MISSING", "ReadTimeout is disabled", "set Config.ReadTimeout", ""})
	}
	if prod && a.cfg.WriteTimeout == 0 {
		f = append(f, SecurityFinding{"high", "WRITE_TIMEOUT_MISSING", "WriteTimeout is disabled", "set Config.WriteTimeout", ""})
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

func (a *App) EnableComplianceEndpoints(prefix string) *App {
	if prefix == "" {
		prefix = "/_fh"
	}
	prefix = strings.TrimRight(prefix, "/")
	a.Get(prefix+"/compliance", func(c Ctx) error { return c.JSON(a.ComplianceReport()) })
	a.Get(prefix+"/compliance/controls", func(c Ctx) error { return c.JSON(a.ComplianceControls()) })
	a.Get(prefix+"/compliance/findings", func(c Ctx) error { return c.JSON(a.ValidateSecurity()) })
	a.Get(prefix+"/config/safe", func(c Ctx) error { return c.JSON(a.SafeConfig()) })
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
	return nil
}
