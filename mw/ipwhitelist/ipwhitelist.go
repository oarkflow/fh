package ipwhitelist

import (
	"errors"
	"net"
	"strings"
	"sync"

	"github.com/oarkflow/fh"
)

var ErrForbidden = errors.New("ipwhitelist: forbidden")

type KeyFunc func(ctx *fh.Ctx) string

type ForbiddenHandler func(ctx *fh.Ctx) error

type Store interface {
	Allowed(ip net.IP) bool
}

type Config struct {
	Allowed []string
	Blocked []string

	Store      Store
	BlockStore Store

	KeyFunc KeyFunc

	// TrustProxyHeaders should only be enabled behind trusted proxies.
	TrustProxyHeaders bool

	Forbidden ForbiddenHandler
}

func New(allowed ...string) fh.HandlerFunc {
	return NewWithConfig(Config{
		Allowed: allowed,
	})
}

func NewWithConfig(config Config) fh.HandlerFunc {
	cfg, err := normalize(config)
	if err != nil {
		panic(err)
	}

	return func(ctx *fh.Ctx) error {
		rawIP := ""

		if cfg.KeyFunc != nil {
			rawIP = cfg.KeyFunc(ctx)
		}

		if rawIP == "" {
			rawIP = clientIP(ctx, cfg.TrustProxyHeaders)
		}

		ip := net.ParseIP(rawIP)
		if ip == nil {
			return cfg.Forbidden(ctx)
		}

		if cfg.BlockStore != nil && cfg.BlockStore.Allowed(ip) {
			return cfg.Forbidden(ctx)
		}

		if cfg.Store == nil || cfg.Store.Allowed(ip) {
			return ctx.Next()
		}

		return cfg.Forbidden(ctx)
	}
}

func normalize(cfg Config) (Config, error) {
	if cfg.Forbidden == nil {
		cfg.Forbidden = DefaultForbiddenHandler
	}
	if cfg.Store == nil && len(cfg.Allowed) > 0 {
		store, err := NewMemoryStore(cfg.Allowed...)
		if err != nil {
			return cfg, err
		}
		cfg.Store = store
	}
	if cfg.BlockStore == nil && len(cfg.Blocked) > 0 {
		store, err := NewMemoryStore(cfg.Blocked...)
		if err != nil {
			return cfg, err
		}
		cfg.BlockStore = store
	}
	return cfg, nil
}

func DefaultForbiddenHandler(ctx *fh.Ctx) error {
	ctx.Set("Content-Type", "text/plain; charset=utf-8")
	return ctx.Status(403).SendString("Forbidden")
}

func clientIP(ctx *fh.Ctx, trustProxy bool) string {
	if trustProxy {
		if ip := firstForwardedIP(ctx.Get("X-Forwarded-For")); ip != "" {
			return ip
		}
		if ip := strings.TrimSpace(ctx.Get("X-Real-IP")); ip != "" {
			return ip
		}
	}

	return ctx.IP()
}

func firstForwardedIP(v string) string {
	if v == "" {
		return ""
	}

	if i := strings.IndexByte(v, ','); i >= 0 {
		return strings.TrimSpace(v[:i])
	}

	return strings.TrimSpace(v)
}

// -----------------------------------------------------------------------------
// In-memory whitelist store
// -----------------------------------------------------------------------------

type MemoryStore struct {
	mu       sync.RWMutex
	ips      []net.IP
	networks []*net.IPNet
}

func NewMemoryStore(allowed ...string) (*MemoryStore, error) {
	s := &MemoryStore{}
	if err := s.Set(allowed...); err != nil {
		return nil, err
	}
	return s, nil
}

func MustMemoryStore(allowed ...string) *MemoryStore {
	s, err := NewMemoryStore(allowed...)
	if err != nil {
		panic(err)
	}
	return s
}

func (s *MemoryStore) Set(allowed ...string) error {
	ips := make([]net.IP, 0, len(allowed))
	networks := make([]*net.IPNet, 0, len(allowed))

	for _, item := range allowed {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		if strings.Contains(item, "/") {
			ip, network, err := net.ParseCIDR(item)
			if err != nil {
				return err
			}
			network.IP = ip
			networks = append(networks, network)
			continue
		}

		ip := net.ParseIP(item)
		if ip == nil {
			return errors.New("ipwhitelist: invalid IP: " + item)
		}

		ips = append(ips, ip)
	}

	s.mu.Lock()
	s.ips = ips
	s.networks = networks
	s.mu.Unlock()

	return nil
}

func (s *MemoryStore) Allowed(ip net.IP) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, allowed := range s.ips {
		if allowed.Equal(ip) {
			return true
		}
	}

	for _, network := range s.networks {
		if network.Contains(ip) {
			return true
		}
	}

	return false
}
