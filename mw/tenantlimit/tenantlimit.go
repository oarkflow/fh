package tenantlimit

import (
	"sync"

	"github.com/oarkflow/fh"
)

const defaultMaxTenantIDBytes = 256
const defaultMaxTenants = 10_000

type TenantFunc func(fh.Ctx) string

type Config struct {
	Limit    int
	Tenant   TenantFunc
	Header   string
	LocalKey string
	// MaxTenantIDBytes bounds tenant identifiers retained in the active map.
	// Zero uses the 256-byte default.
	MaxTenantIDBytes int
	// MaxTenants bounds distinct tenants with active requests. Zero uses 10000.
	MaxTenants int
	Error      func(fh.Ctx, string) error
}

type limiter struct {
	mu     sync.Mutex
	active map[string]int
	cfg    Config
}

func New(cfg Config) fh.HandlerFunc {
	if cfg.Limit <= 0 {
		cfg.Limit = 100
	}
	if cfg.Header == "" {
		cfg.Header = "X-Tenant-ID"
	}
	if cfg.MaxTenantIDBytes <= 0 {
		cfg.MaxTenantIDBytes = defaultMaxTenantIDBytes
	}
	if cfg.MaxTenants <= 0 {
		cfg.MaxTenants = defaultMaxTenants
	}
	if cfg.Error == nil {
		cfg.Error = func(c fh.Ctx, t string) error {
			return c.Status(fh.StatusTooManyRequests).JSON(fh.Map{"error": "tenant_limit_exceeded", "tenant": t})
		}
	}
	l := &limiter{active: make(map[string]int, min(cfg.MaxTenants, 256)), cfg: cfg}
	return l.Handle
}
func (l *limiter) Handle(c fh.Ctx) error {
	tenant := l.tenant(c)
	if tenant == "" {
		tenant = "default"
	}
	if len(tenant) > l.cfg.MaxTenantIDBytes {
		return fh.NewHTTPError(fh.StatusBadRequest, "TENANT_ID_TOO_LARGE", "tenant identifier is too long")
	}
	l.mu.Lock()
	active := l.active[tenant]
	if active >= l.cfg.Limit {
		l.mu.Unlock()
		return l.cfg.Error(c, tenant)
	}
	if active == 0 && len(l.active) >= l.cfg.MaxTenants {
		l.mu.Unlock()
		return fh.NewHTTPError(fh.StatusServiceUnavailable, "TENANT_CAPACITY", "active tenant capacity reached")
	}
	l.active[tenant]++
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.active[tenant]--
		if l.active[tenant] <= 0 {
			delete(l.active, tenant)
		}
		l.mu.Unlock()
	}()
	return c.Next()
}
func (l *limiter) tenant(c fh.Ctx) string {
	if l.cfg.Tenant != nil {
		return l.cfg.Tenant(c)
	}
	if l.cfg.LocalKey != "" {
		if s, _ := c.Locals(l.cfg.LocalKey).(string); s != "" {
			return s
		}
	}
	return c.Get(l.cfg.Header)
}
