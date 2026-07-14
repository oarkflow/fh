package circuitbreaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

var ErrRegistryFull = errors.New("circuitbreaker: registry capacity reached")

// RegistryConfig configures a bounded collection of per-dependency breakers.
type RegistryConfig struct {
	Breaker    Config
	MaxEntries int
	IdleTTL    time.Duration
}

type registryEntry struct {
	breaker *Breaker
	lastUse atomic.Int64
}

// Registry prevents unrelated routes or dependencies from sharing one global
// failure domain. It is safe for concurrent use and bounded by MaxEntries.
type Registry struct {
	cfg RegistryConfig
	now func() time.Time

	mu      sync.RWMutex
	entries map[string]*registryEntry
}

func NewRegistry(cfg RegistryConfig) (*Registry, error) {
	if cfg.MaxEntries == 0 {
		cfg.MaxEntries = 1024
	}
	if cfg.MaxEntries < 1 {
		return nil, errors.New("circuitbreaker: registry MaxEntries must be positive")
	}
	if cfg.IdleTTL < 0 {
		return nil, errors.New("circuitbreaker: registry IdleTTL cannot be negative")
	}
	if _, err := normalizeConfig(cfg.Breaker); err != nil {
		return nil, err
	}
	now := cfg.Breaker.Now
	if now == nil {
		now = time.Now
	}
	return &Registry{
		cfg:     cfg,
		now:     now,
		entries: make(map[string]*registryEntry),
	}, nil
}

func (r *Registry) GetOrCreate(key string) (*Breaker, error) {
	if key == "" {
		return nil, errors.New("circuitbreaker: registry key cannot be empty")
	}
	nowNS := r.now().UnixNano()

	r.mu.RLock()
	entry := r.entries[key]
	if entry != nil {
		entry.lastUse.Store(nowNS)
	}
	r.mu.RUnlock()
	if entry != nil {
		return entry.breaker, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if entry = r.entries[key]; entry != nil {
		entry.lastUse.Store(nowNS)
		return entry.breaker, nil
	}
	if len(r.entries) >= r.cfg.MaxEntries && !r.evictOneLocked(nowNS) {
		return nil, ErrRegistryFull
	}
	breaker, err := NewChecked(r.cfg.Breaker)
	if err != nil {
		return nil, err
	}
	entry = &registryEntry{breaker: breaker}
	entry.lastUse.Store(nowNS)
	r.entries[key] = entry
	return breaker, nil
}

func (r *Registry) evictOneLocked(nowNS int64) bool {
	var candidateKey string
	var candidateLastUse int64 = 1<<63 - 1
	for key, entry := range r.entries {
		if entry.breaker.State() != StateClosed {
			continue
		}
		lastUse := entry.lastUse.Load()
		if r.cfg.IdleTTL > 0 && time.Duration(nowNS-lastUse) < r.cfg.IdleTTL {
			continue
		}
		if lastUse < candidateLastUse {
			candidateKey = key
			candidateLastUse = lastUse
		}
	}
	if candidateKey == "" {
		return false
	}
	delete(r.entries, candidateKey)
	return true
}

// Sweep removes closed entries idle for at least IdleTTL.
func (r *Registry) Sweep() int {
	if r.cfg.IdleTTL <= 0 {
		return 0
	}
	cutoff := r.now().Add(-r.cfg.IdleTTL).UnixNano()
	removed := 0
	r.mu.Lock()
	for key, entry := range r.entries {
		if entry.lastUse.Load() <= cutoff && entry.breaker.State() == StateClosed {
			delete(r.entries, key)
			removed++
		}
	}
	r.mu.Unlock()
	return removed
}

func (r *Registry) Delete(key string) bool {
	r.mu.Lock()
	_, found := r.entries[key]
	delete(r.entries, key)
	r.mu.Unlock()
	return found
}

func (r *Registry) Len() int {
	r.mu.RLock()
	length := len(r.entries)
	r.mu.RUnlock()
	return length
}

// Handler selects a breaker by dependency, route, tenant, or operation key.
func (r *Registry) Handler(key func(fh.Ctx) string) fh.HandlerFunc {
	if key == nil {
		panic("circuitbreaker: registry key function cannot be nil")
	}
	return func(c fh.Ctx) error {
		breaker, err := r.GetOrCreate(key(c))
		if err != nil {
			return fh.NewHTTPError(
				fh.StatusServiceUnavailable,
				"CIRCUIT_REGISTRY_UNAVAILABLE",
				err.Error(),
			)
		}
		return breaker.handle(c)
	}
}
