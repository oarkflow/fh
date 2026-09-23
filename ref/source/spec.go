// Package source provides REF's universal data-source optimisation layer.
//
// Any ReadNode capability can opt into source-level optimisations (batching,
// caching, coalescing, observability) by attaching a [Spec] to its registration.
// The subsystem is source-agnostic: databases, REST/gRPC APIs, object stores,
// caches, queues, and search engines are all treated uniformly.
package source

import "time"

// SourceKind classifies the external data source type.
type SourceKind uint8

const (
	KindDatabase SourceKind = iota // SQL, document DB
	KindAPI                        // REST, gRPC, GraphQL
	KindCache                      // Redis, Memcached
	KindQueue                      // Kafka, SQS, NATS
	KindStorage                    // S3, GCS, local FS
	KindSearch                     // Elasticsearch, Solr
	KindCustom                     // user-defined
)

func (k SourceKind) String() string {
	switch k {
	case KindDatabase:
		return "database"
	case KindAPI:
		return "api"
	case KindCache:
		return "cache"
	case KindQueue:
		return "queue"
	case KindStorage:
		return "storage"
	case KindSearch:
		return "search"
	case KindCustom:
		return "custom"
	default:
		return "unknown"
	}
}

// ConsistencyLevel describes the freshness guarantee a read requires.
type ConsistencyLevel uint8

const (
	Strong   ConsistencyLevel = iota // always go to primary/source
	Session                          // respect read-after-write within session
	Eventual                         // stale reads acceptable
)

func (c ConsistencyLevel) String() string {
	switch c {
	case Strong:
		return "strong"
	case Session:
		return "session"
	case Eventual:
		return "eventual"
	default:
		return "unknown"
	}
}

// Cardinality describes the expected result set size.
type Cardinality uint8

const (
	One      Cardinality = iota // exactly one result
	MaybeOne                    // zero or one
	Many                        // zero to N
)

func (c Cardinality) String() string {
	switch c {
	case One:
		return "one"
	case MaybeOne:
		return "maybe_one"
	case Many:
		return "many"
	default:
		return "unknown"
	}
}

// CacheScope determines where cached results live.
type CacheScope uint8

const (
	ScopeNone        CacheScope = iota // no caching
	ScopeInvocation                    // L0: dies with the request (fact store)
	ScopeProcess                       // L1: in-process, shared across requests
	ScopeDistributed                   // L2: distributed cache (Redis etc.)
)

func (s CacheScope) String() string {
	switch s {
	case ScopeNone:
		return "none"
	case ScopeInvocation:
		return "invocation"
	case ScopeProcess:
		return "process"
	case ScopeDistributed:
		return "distributed"
	default:
		return "unknown"
	}
}

// Spec declares how a ReadNode interacts with its data source.
// Optional — capabilities without a Spec work exactly as they do today.
type Spec struct {
	// Kind classifies the source type for routing and metrics.
	Kind SourceKind

	// Name is the logical source identifier (e.g. "users-db", "payment-api").
	// Used as a key for metrics aggregation, connection routing, and cache scoping.
	Name string

	// Optimisation hints
	ReadOnly    bool // source operation does not mutate state
	Cacheable   bool // result can be cached
	Batchable   bool // multiple keys can be fetched in one call
	Coalescible bool // identical in-flight requests can share one execution
	Idempotent  bool // safe to retry on failure

	// Consistency determines freshness requirements for reads.
	Consistency ConsistencyLevel

	// Cardinality describes the expected result set size.
	Cardinality Cardinality

	// CacheTTL is how long a cached result stays valid.
	CacheTTL time.Duration

	// CacheScope determines where cached results live.
	CacheScope CacheScope

	// CostWeight is a relative cost (1–1000) for budget accounting.
	// Higher values consume more budget tokens per operation.
	// Zero means default cost of 1.
	CostWeight uint32
}
