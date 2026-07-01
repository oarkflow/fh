package tenantlimit

import (
	"github.com/oarkflow/fh"
	"sync"
)

type TenantFunc func(fh.Ctx) string

type Config struct {
	Limit    int
	Tenant   TenantFunc
	Header   string
	LocalKey string
	Error    func(fh.Ctx, string) error
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
	if cfg.Error == nil {
		cfg.Error = func(c fh.Ctx, t string) error {
			return c.Status(fh.StatusTooManyRequests).JSON(fh.Map{"error": "tenant_limit_exceeded", "tenant": t})
		}
	}
	l := &limiter{active: map[string]int{}, cfg: cfg}
	return l.Handle
}
func (l *limiter) Handle(c fh.Ctx) error {
	tenant := l.tenant(c)
	if tenant == "" {
		tenant = "default"
	}
	l.mu.Lock()
	if l.active[tenant] >= l.cfg.Limit {
		l.mu.Unlock()
		return l.cfg.Error(c, tenant)
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
