package source

import (
	"context"
	"hash/maphash"
	"time"
	"unsafe"

	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
)

// Registration describes a capability for the graph compiler.
// Duplicated type alias to avoid import cycle with capability package.
// The actual wiring happens at the call site via RegistrationAdapter.
type Registration struct {
	Name        string
	Requires    []fact.AnyKey
	Provides    []fact.AnyKey
	Kind        graph.NodeKind
	Speculation graph.SpeculationClass
	Cacheable   bool
	Idempotent  bool
	Source      *Spec
	Run         func(nc *execution.NodeContext) error
}

// SourceOption configures source capability behaviour.
type SourceOption func(*sourceConfig)

type sourceConfig struct {
	cache     Cache
	coalescer *Coalescer
	observers []func(Metrics)
	keyFunc   func(nc *execution.NodeContext) string // cache/coalesce key extractor
}

// WithCache sets the L1/L2 cache for the source capability.
func WithCache(c Cache) SourceOption {
	return func(sc *sourceConfig) { sc.cache = c }
}

// WithCoalescer enables in-flight request deduplication.
func WithCoalescer(c *Coalescer) SourceOption {
	return func(sc *sourceConfig) { sc.coalescer = c }
}

// WithMetricsObserver adds a metrics callback for every source operation.
func WithMetricsObserver(fn func(Metrics)) SourceOption {
	return func(sc *sourceConfig) { sc.observers = append(sc.observers, fn) }
}

// WithKeyFunc sets the cache/coalesce key extractor.
// The function should return a string that uniquely identifies the operation + params.
func WithKeyFunc(fn func(nc *execution.NodeContext) string) SourceOption {
	return func(sc *sourceConfig) { sc.keyFunc = fn }
}

// hashSeed is a per-process hash seed for deterministic-within-process hashing.
var hashSeed = maphash.MakeSeed()

func hashString(s string) uint64 {
	var h maphash.Hash
	h.SetSeed(hashSeed)
	h.WriteString(s)
	return h.Sum64()
}

func hashBytes(b []byte) uint64 {
	var h maphash.Hash
	h.SetSeed(hashSeed)
	h.Write(b)
	return h.Sum64()
}

// NewFetchCapability creates a ReadNode capability for a source that fetches
// a single item per call. Automatically integrates caching, coalescing,
// and observability based on the Spec and options.
//
// The fetchFn receives the NodeContext and returns the result. The caller
// is responsible for reading input parameters from facts/invocation.
func NewFetchCapability(
	name string,
	spec Spec,
	outputKey fact.AnyKey,
	fetchFn func(nc *execution.NodeContext) (any, error),
	opts ...SourceOption,
) Registration {
	cfg := &sourceConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	reg := Registration{
		Name:     name,
		Kind:     graph.ReadNode,
		Provides: []fact.AnyKey{outputKey},
		Source:   &spec,
	}

	reg.Run = func(nc *execution.NodeContext) error {
		start := time.Now()
		metrics := Metrics{
			SourceName: spec.Name,
			SourceKind: spec.Kind,
			Operation:  "fetch",
		}

		// Build cache key if cacheable
		var cacheKey CacheKey
		if spec.Cacheable && cfg.cache != nil {
			cacheKey = buildCacheKey(nc, spec, cfg)
			if val, hit, err := cfg.cache.Get(cacheKey); err == nil && hit {
				metrics.CacheHit = true
				metrics.CacheLevel = cacheLevel(spec.CacheScope)
				metrics.TotalTime = time.Since(start)
				emitMetrics(cfg, metrics)
				if val != nil {
					publishAny(nc, outputKey, val)
				}
				return nil
			}
		}

		// Coalesce if enabled
		var val any
		var fetchErr error

		if spec.Coalescible && cfg.coalescer != nil {
			coalesceKey := coalesceKeyFor(nc, spec, cfg)
			metrics.Coalesced = true
			result, err := cfg.coalescer.Do(coalesceKey, func() (any, error) {
				return fetchFn(nc)
			})
			val, fetchErr = result, err
		} else {
			val, fetchErr = fetchFn(nc)
		}

		metrics.ExecTime = time.Since(start)
		metrics.TotalTime = time.Since(start)

		if fetchErr != nil {
			metrics.Error = true
			emitMetrics(cfg, metrics)
			return fetchErr
		}

		// Populate cache
		if spec.Cacheable && cfg.cache != nil && val != nil {
			_ = cfg.cache.Set(cacheKey, val, spec.CacheTTL)
		}

		emitMetrics(cfg, metrics)
		publishAny(nc, outputKey, val)
		return nil
	}

	return reg
}

// NewBatchCapability creates a ReadNode capability where the underlying source
// supports batch fetching natively.
//
// The batchFn receives a list of keys and returns results for all of them.
// REF automatically batches multiple concurrent requests (from parallel nodes
// or repeated access patterns) into a single batchFn call via the DataLoader.
//
// The loaderKeyFact is used to read the lookup key(s) from the fact store.
// The outputKey is where the result is published.
func NewBatchCapability[K comparable, V any](
	name string,
	spec Spec,
	outputKey fact.AnyKey,
	batchFn BatchFunc[K, V],
	keyExtractor func(nc *execution.NodeContext) K,
	loaderCfg LoaderConfig,
	opts ...SourceOption,
) Registration {
	cfg := &sourceConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	// The DataLoader is shared across all invocations of this capability
	// within a single request. For request-scoping, the caller should create
	// a new loader per invocation via a fact.
	loader := NewLoader[K, V](batchFn, loaderCfg)

	reg := Registration{
		Name:     name,
		Kind:     graph.ReadNode,
		Provides: []fact.AnyKey{outputKey},
		Source:   &spec,
	}

	reg.Run = func(nc *execution.NodeContext) error {
		start := time.Now()
		metrics := Metrics{
			SourceName: spec.Name,
			SourceKind: spec.Kind,
			Operation:  "batch_fetch",
			Batched:    true,
		}

		key := keyExtractor(nc)

		// Check cache first
		if spec.Cacheable && cfg.cache != nil {
			cacheKey := buildCacheKeyForParam(spec, cfg, nc, key)
			if val, hit, err := cfg.cache.Get(cacheKey); err == nil && hit {
				metrics.CacheHit = true
				metrics.CacheLevel = cacheLevel(spec.CacheScope)
				metrics.TotalTime = time.Since(start)
				emitMetrics(cfg, metrics)
				if val != nil {
					publishAny(nc, outputKey, val)
				}
				return nil
			}
		}

		val, err := loader.Load(context.Background(), key)

		metrics.ExecTime = time.Since(start)
		metrics.TotalTime = time.Since(start)

		if err != nil {
			metrics.Error = true
			emitMetrics(cfg, metrics)
			return err
		}

		// Populate cache
		if spec.Cacheable && cfg.cache != nil {
			cacheKey := buildCacheKeyForParam(spec, cfg, nc, key)
			_ = cfg.cache.Set(cacheKey, val, spec.CacheTTL)
		}

		emitMetrics(cfg, metrics)
		publishAny(nc, outputKey, val)
		return nil
	}

	return reg
}

func buildCacheKey(nc *execution.NodeContext, spec Spec, cfg *sourceConfig) CacheKey {
	var tenantID, principalID string
	inv := nc.Invocation()
	if inv != nil {
		// Extract tenant/principal from invocation metadata if available
		if inv.Principal.SessionID != "" {
			principalID = inv.Principal.SessionID
		}
	}

	queryHash := hashString(spec.Name)
	var paramHash uint64
	if cfg.keyFunc != nil {
		paramHash = hashString(cfg.keyFunc(nc))
	} else if inv != nil {
		paramHash = hashBytes(inv.Input.RawBytes())
	}

	return CacheKey{
		SourceName:  spec.Name,
		TenantID:    tenantID,
		PrincipalID: principalID,
		QueryHash:   queryHash,
		ParamHash:   paramHash,
	}
}

func buildCacheKeyForParam[K comparable](spec Spec, cfg *sourceConfig, nc *execution.NodeContext, key K) CacheKey {
	ck := buildCacheKey(nc, spec, cfg)
	// Hash the key parameter
	size := unsafe.Sizeof(key)
	if size <= 8 {
		// Small key: use direct bit pattern
		ck.ParamHash = *(*uint64)(unsafe.Pointer(&key))
	} else {
		// Fallback: hash the string representation
		var h maphash.Hash
		h.SetSeed(hashSeed)
		ptr := (*[64]byte)(unsafe.Pointer(&key))
		h.Write(ptr[:size])
		ck.ParamHash = h.Sum64()
	}
	return ck
}

func coalesceKeyFor(nc *execution.NodeContext, spec Spec, cfg *sourceConfig) string {
	if cfg.keyFunc != nil {
		return spec.Name + ":" + cfg.keyFunc(nc)
	}
	inv := nc.Invocation()
	if inv != nil {
		return spec.Name + ":" + string(inv.Input.RawBytes())
	}
	return spec.Name
}

func cacheLevel(scope CacheScope) string {
	switch scope {
	case ScopeInvocation:
		return "L0"
	case ScopeProcess:
		return "L1"
	case ScopeDistributed:
		return "L2"
	default:
		return ""
	}
}

func emitMetrics(cfg *sourceConfig, m Metrics) {
	for _, fn := range cfg.observers {
		fn(m)
	}
}

func publishAny(nc *execution.NodeContext, key fact.AnyKey, val any) {
	if nc == nil || nc.Facts() == nil {
		return
	}
	slot, ok := nc.SlotOf(key.DefID)
	if !ok {
		return
	}
	fact.Put(nc.Facts(), slot, val)
}
