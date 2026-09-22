package platform

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
	"github.com/oarkflow/fh/ref/platform/spi"
)

// Coordination primitives: locks, rate limiters and circuit breakers.
//
// All three are built on one thing — kv.Store.Mutate, an atomic
// read-modify-write under the store's own per-key lock. That single primitive is
// what lets the same code be correct in-process (cache.memory), across
// processes on a host (cache.file) and across replicas (cache.sql), with only
// the backing store changing.
//
// The consequence worth stating plainly: a lock or limiter over cache.memory is
// only correct within one replica. The compiler cannot know how many replicas a
// deployment runs, so it cannot reject that — but the catalog says so on every
// provider, and a deployment that means to coordinate globally must name a
// shared store.

func registerCoordinationResources(r *Registry) {
	mustResource(r, "lock.memory", ResourceFactoryFunc(openMemoryLock), ResourceKindInfo{
		Family:   "lock",
		Summary:  "Leased mutual exclusion within one replica. Correct only for single-process deployments.",
		Provides: []string{"Locker"},
		Config: []ConfigField{
			{Name: "max_entries", Type: "int", Default: "10000"},
		},
	})

	mustResource(r, "lock.store", ResourceFactoryFunc(openStoreLock), ResourceKindInfo{
		Family:   "lock",
		Summary:  "Leased mutual exclusion over any cache resource. Name a shared cache (cache.sql) to coordinate across replicas.",
		Provides: []string{"Locker"},
		Config: []ConfigField{
			{Name: "cache", Type: "resource", Required: true, Summary: "The cache resource holding the leases"},
			{Name: "prefix", Type: "string", Default: "lock:"},
			{Name: "ttl", Type: "duration", Default: "30s", Summary: "Default lease length when a caller does not ask for one"},
		},
	})

	mustResource(r, "ratelimit.memory", ResourceFactoryFunc(openMemoryRateLimiter), ResourceKindInfo{
		Family:   "ratelimit",
		Summary:  "Fixed-window rate limiter within one replica. N replicas allow N times the configured limit.",
		Provides: []string{"RateLimiter"},
		Config: []ConfigField{
			{Name: "max_entries", Type: "int", Default: "100000"},
		},
	})

	mustResource(r, "ratelimit.store", ResourceFactoryFunc(openStoreRateLimiter), ResourceKindInfo{
		Family:   "ratelimit",
		Summary:  "Fixed-window rate limiter over any cache resource. Name a shared cache for a deployment-wide limit.",
		Provides: []string{"RateLimiter"},
		Config: []ConfigField{
			{Name: "cache", Type: "resource", Required: true},
			{Name: "prefix", Type: "string", Default: "rate:"},
		},
	})

	mustResource(r, "circuit_breaker.store", ResourceFactoryFunc(openStoreCircuitBreaker), ResourceKindInfo{
		Family:   "circuit_breaker",
		Summary:  "Failure-counting breaker over any cache resource, with a half-open probe after the reset window.",
		Provides: []string{"CircuitBreaker"},
		Config: []ConfigField{
			{Name: "cache", Type: "resource", Required: true},
			{Name: "prefix", Type: "string", Default: "cb:"},
			{Name: "failure_threshold", Type: "int", Default: "5", Summary: "Consecutive failures that open the circuit"},
			{Name: "reset_after", Type: "duration", Default: "30s", Summary: "How long the circuit stays open before a probe is admitted"},
		},
	})
}

// ---------------------------------------------------------------------------
// Locks
// ---------------------------------------------------------------------------

// storeLock is a leased lock over a kv.Store.
//
// Every lock carries a random token and an expiry. Release and Refresh present
// the token and are ignored when it does not match, which closes the classic
// lease bug: a holder whose lease already expired, and whose key another process
// has since taken, must not be able to release somebody else's lock.
type storeLock struct {
	store      kv.Store
	prefix     string
	defaultTTL time.Duration
}

type lockRecord struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

func openMemoryLock(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("lock.memory", spec.Config, "max_entries", "ttl"); err != nil {
		return nil, nil, err
	}
	maxEntries, err := configInt(spec.Config, "max_entries", 10000)
	if err != nil {
		return nil, nil, err
	}
	ttl, err := configDuration(spec.Config, "ttl", 30*time.Second)
	if err != nil {
		return nil, nil, err
	}
	store := kv.NewMemoryStore(kv.WithMaxEntries(maxEntries), kv.WithGCInterval(time.Minute))
	return &storeLock{store: store, prefix: "lock:", defaultTTL: ttl}, store, nil
}

func openStoreLock(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("lock.store", spec.Config, "cache", "prefix", "ttl"); err != nil {
		return nil, nil, err
	}
	store, err := requireMutableStore(spec, "cache")
	if err != nil {
		return nil, nil, err
	}
	ttl, err := configDuration(spec.Config, "ttl", 30*time.Second)
	if err != nil {
		return nil, nil, err
	}
	return &storeLock{store: store, prefix: configString(spec.Config, "prefix", "lock:"), defaultTTL: ttl}, nil, nil
}

// Acquire implements spi.Locker.
func (l *storeLock) Acquire(_ context.Context, key string, ttl time.Duration) (string, bool, error) {
	if ttl <= 0 {
		ttl = l.defaultTTL
	}
	token := newToken(16)
	acquired := false
	err := l.store.Mutate(l.prefix+key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if exists && len(current) > 0 {
			var held lockRecord
			// A record we cannot parse is treated as a live lock rather than a
			// free one: failing to acquire is recoverable, acquiring a lock
			// somebody else holds is not.
			if err := json.Unmarshal(current, &held); err != nil {
				return nil, 0, false, nil
			}
			if held.ExpiresAt.After(time.Now().UTC()) {
				return nil, 0, false, nil
			}
		}
		encoded, err := json.Marshal(lockRecord{Token: token, ExpiresAt: time.Now().UTC().Add(ttl)})
		if err != nil {
			return nil, 0, false, err
		}
		acquired = true
		return encoded, ttl, true, nil
	})
	if err != nil {
		return "", false, err
	}
	if !acquired {
		return "", false, nil
	}
	return token, true, nil
}

// Release implements spi.Locker.
func (l *storeLock) Release(_ context.Context, key, token string) error {
	return l.store.Mutate(l.prefix+key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if !exists || len(current) == 0 {
			return nil, 0, false, nil
		}
		var held lockRecord
		if err := json.Unmarshal(current, &held); err != nil {
			return nil, 0, false, nil
		}
		if subtle.ConstantTimeCompare([]byte(held.Token), []byte(token)) != 1 {
			return nil, 0, false, nil
		}
		// A zero-length value with an immediate expiry is how this store
		// expresses a delete from inside Mutate.
		return nil, time.Nanosecond, true, nil
	})
}

// Refresh implements spi.Locker, extending a lease the holder still owns.
func (l *storeLock) Refresh(_ context.Context, key, token string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = l.defaultTTL
	}
	refreshed := false
	err := l.store.Mutate(l.prefix+key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if !exists || len(current) == 0 {
			return nil, 0, false, nil
		}
		var held lockRecord
		if err := json.Unmarshal(current, &held); err != nil {
			return nil, 0, false, nil
		}
		if subtle.ConstantTimeCompare([]byte(held.Token), []byte(token)) != 1 {
			return nil, 0, false, nil
		}
		held.ExpiresAt = time.Now().UTC().Add(ttl)
		encoded, err := json.Marshal(held)
		if err != nil {
			return nil, 0, false, err
		}
		refreshed = true
		return encoded, ttl, true, nil
	})
	return refreshed, err
}

// ---------------------------------------------------------------------------
// Rate limiters
// ---------------------------------------------------------------------------

// storeRateLimiter is a fixed-window counter.
//
// A fixed window is chosen over a sliding one deliberately: it needs one key and
// one atomic increment, which is what makes it correct over any of the backing
// stores. Its known weakness is a burst of up to 2×limit across a window
// boundary, which is acceptable for the abuse-prevention and backpressure jobs
// it is used for here. A deployment that needs a strict sliding window should
// register one through the driver SPI over a backend that supports it natively.
type storeRateLimiter struct {
	store  kv.Store
	prefix string
}

type rateWindow struct {
	Count    int       `json:"count"`
	StartsAt time.Time `json:"starts_at"`
}

func openMemoryRateLimiter(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("ratelimit.memory", spec.Config, "max_entries"); err != nil {
		return nil, nil, err
	}
	maxEntries, err := configInt(spec.Config, "max_entries", 100000)
	if err != nil {
		return nil, nil, err
	}
	store := kv.NewMemoryStore(kv.WithMaxEntries(maxEntries), kv.WithGCInterval(time.Minute))
	return &storeRateLimiter{store: store, prefix: "rate:"}, store, nil
}

func openStoreRateLimiter(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("ratelimit.store", spec.Config, "cache", "prefix"); err != nil {
		return nil, nil, err
	}
	store, err := requireMutableStore(spec, "cache")
	if err != nil {
		return nil, nil, err
	}
	return &storeRateLimiter{store: store, prefix: configString(spec.Config, "prefix", "rate:")}, nil, nil
}

// Allow implements spi.RateLimiter.
func (l *storeRateLimiter) Allow(_ context.Context, key string, limit int, window time.Duration) (bool, int, time.Time, error) {
	if limit <= 0 || window <= 0 {
		return true, 0, time.Time{}, nil
	}
	var (
		allowed   bool
		remaining int
		resetAt   time.Time
	)
	err := l.store.Mutate(l.prefix+key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		now := time.Now().UTC()
		state := rateWindow{StartsAt: now}
		if exists && len(current) > 0 {
			if err := json.Unmarshal(current, &state); err != nil || now.Sub(state.StartsAt) >= window {
				state = rateWindow{StartsAt: now}
			}
		}
		resetAt = state.StartsAt.Add(window)
		if state.Count >= limit {
			allowed, remaining = false, 0
			// Nothing to write: a rejected call must not extend the window, or a
			// client hammering the endpoint would never be let back in.
			return nil, 0, false, nil
		}
		state.Count++
		allowed = true
		remaining = limit - state.Count
		encoded, err := json.Marshal(state)
		if err != nil {
			return nil, 0, false, err
		}
		return encoded, resetAt.Sub(now), true, nil
	})
	return allowed, remaining, resetAt, err
}

// ---------------------------------------------------------------------------
// Circuit breakers
// ---------------------------------------------------------------------------

// storeCircuitBreaker counts consecutive failures per key and opens the circuit
// once the threshold is reached. After the reset window it admits exactly one
// probe (half-open); that probe's outcome either closes the circuit or re-opens
// it for another window.
type storeCircuitBreaker struct {
	store     kv.Store
	prefix    string
	threshold int
	reset     time.Duration
}

type breakerState struct {
	Failures int       `json:"failures"`
	OpenedAt time.Time `json:"opened_at"`
	Probing  bool      `json:"probing"`
}

func openStoreCircuitBreaker(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("circuit_breaker.store", spec.Config,
		"cache", "prefix", "failure_threshold", "reset_after"); err != nil {
		return nil, nil, err
	}
	store, err := requireMutableStore(spec, "cache")
	if err != nil {
		return nil, nil, err
	}
	threshold, err := configInt(spec.Config, "failure_threshold", 5)
	if err != nil {
		return nil, nil, err
	}
	reset, err := configDuration(spec.Config, "reset_after", 30*time.Second)
	if err != nil {
		return nil, nil, err
	}
	return &storeCircuitBreaker{
		store:     store,
		prefix:    configString(spec.Config, "prefix", "cb:"),
		threshold: threshold,
		reset:     reset,
	}, nil, nil
}

// Allow implements spi.CircuitBreaker.
func (b *storeCircuitBreaker) Allow(_ context.Context, key string) (bool, error) {
	allowed := true
	err := b.store.Mutate(b.prefix+key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if !exists || len(current) == 0 {
			return nil, 0, false, nil
		}
		var state breakerState
		if err := json.Unmarshal(current, &state); err != nil {
			return nil, 0, false, nil
		}
		if state.Failures < b.threshold {
			return nil, 0, false, nil
		}
		if time.Since(state.OpenedAt) < b.reset {
			allowed = false
			return nil, 0, false, nil
		}
		if state.Probing {
			// A probe is already in flight. Refusing the rest keeps a recovering
			// dependency from being hit by the whole backlog at once.
			allowed = false
			return nil, 0, false, nil
		}
		state.Probing = true
		encoded, err := json.Marshal(state)
		if err != nil {
			return nil, 0, false, err
		}
		return encoded, b.reset * 2, true, nil
	})
	return allowed, err
}

// Record implements spi.CircuitBreaker.
func (b *storeCircuitBreaker) Record(_ context.Context, key string, success bool) error {
	return b.store.Mutate(b.prefix+key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if success {
			if !exists {
				return nil, 0, false, nil
			}
			return nil, time.Nanosecond, true, nil
		}
		var state breakerState
		if exists && len(current) > 0 {
			_ = json.Unmarshal(current, &state)
		}
		state.Failures++
		state.Probing = false
		if state.Failures >= b.threshold {
			state.OpenedAt = time.Now().UTC()
		}
		encoded, err := json.Marshal(state)
		if err != nil {
			return nil, 0, false, err
		}
		return encoded, b.reset * 4, true, nil
	})
}

// ---------------------------------------------------------------------------
// Shared resolution
// ---------------------------------------------------------------------------

// requireMutableStore resolves a named cache resource and checks that it offers
// the atomic read-modify-write these primitives need.
//
// The check is the point. A cache that only does Get/Set cannot implement a
// correct lock or limiter, and a deployment finds that out here, at load time,
// rather than from a double-spend in production.
func requireMutableStore(spec ResourceSpec, key string) (kv.Store, error) {
	name, err := requiredString(spec.Config, key)
	if err != nil {
		return nil, fmt.Errorf("%s %q: %w", spec.Kind, spec.Name, err)
	}
	resolved, ok := spec.resolved[name]
	if !ok {
		return nil, fmt.Errorf("%s %q: config.%s names unknown resource %q", spec.Kind, spec.Name, key, name)
	}
	store, ok := resolved.(kv.Store)
	if !ok {
		return nil, fmt.Errorf("%s %q: resource %q cannot back a %s — it does not provide atomic read-modify-write. Use cache.memory, cache.file or cache.sql",
			spec.Kind, spec.Name, name, spec.Kind)
	}
	return store, nil
}

// Interface assertions. They cost nothing at run time and turn a contract drift
// into a compile error in this package rather than a type assertion failure in
// somebody's deployment.
var (
	_ spi.Locker         = (*storeLock)(nil)
	_ spi.RateLimiter    = (*storeRateLimiter)(nil)
	_ spi.CircuitBreaker = (*storeCircuitBreaker)(nil)
	_ spi.Cache          = (*sqlCache)(nil)
	_ spi.CacheContext   = (*sqlCache)(nil)
	_ spi.CachePrefix    = (*sqlCache)(nil)
	_ kv.Store           = (*sqlCache)(nil)
)
