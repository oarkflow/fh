package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
	"github.com/oarkflow/fh/ref"
	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/observer"
)

// Deps is everything the application's capabilities, intents and effects need.
//
// It is assembled once, at startup, and passed by pointer. Nothing here is created
// lazily on first use: a dependency that cannot be reached is a startup failure, so
// the first request never discovers a broken database or an unroutable notifier.
type Deps struct {
	Config   Config
	Log      *slog.Logger
	Store    *Store
	Cache    kv.Store
	Queue    *fh.DurableQueue
	Effects  *SQLEffectStore
	Outbox   *OutboxDeliverer
	Notifier Notifier
	Tokens   *TokenSigner
	Metrics  *Metrics
	Audit    *AuditObserver

	// Observer is the single tiered observer the engine is given: it fans out to
	// Metrics and Audit off the scheduler's goroutine.
	Observer   *observer.CompositeObserver
	dispatcher *observer.AsyncDispatcher

	db      *sql.DB
	closers []io.Closer
}

// OpenDeps opens every real resource: the database (and its schema), the cache, the
// durable queue, the notifier, the effect store, the outbox deliverer.
//
// On any failure it closes whatever it already opened, in reverse order, and returns
// the error. A half-open set of connections outliving a failed startup is how a
// process ends up holding a pool it will never use.
func OpenDeps(ctx context.Context, cfg Config, log *slog.Logger) (*Deps, error) {
	deps := &Deps{Config: cfg, Log: log, Metrics: NewMetrics()}

	fail := func(err error) (*Deps, error) {
		_ = deps.Close()
		return nil, err
	}

	db, err := sql.Open(cfg.DatabaseDriver, cfg.DatabaseURL)
	if err != nil {
		return fail(fmt.Errorf("ref-app: open database: %w", err))
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(30 * time.Minute)
	deps.db = db
	deps.closers = append(deps.closers, db)

	// Ping before anything else: every capability below assumes the database is
	// reachable, and finding out here costs one round trip.
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		return fail(fmt.Errorf("ref-app: the database is not reachable: %w", err))
	}
	if err := migrate(ctx, db); err != nil {
		return fail(err)
	}
	deps.Store = NewStore(db)

	cache, err := kv.NewFileStore(cfg.CacheDir, kv.WithFileGCInterval(time.Minute))
	if err != nil {
		return fail(fmt.Errorf("ref-app: open cache: %w", err))
	}
	deps.Cache = cache
	deps.closers = append(deps.closers, cache)

	queue, err := fh.OpenDurableQueue(fh.DurableQueueConfig{
		Dir:          cfg.QueueDir,
		Workers:      2,
		MaxAttempts:  5,
		PollInterval: 100 * time.Millisecond,
		Backoff:      time.Second,
	})
	if err != nil {
		return fail(fmt.Errorf("ref-app: open queue: %w", err))
	}
	deps.Queue = queue
	deps.closers = append(deps.closers, queue)

	notifier, err := NewNotifier(cfg, log)
	if err != nil {
		return fail(err)
	}
	deps.Notifier = notifier
	deps.Outbox = NewOutboxDeliverer(db, notifier, deps.Metrics, log, cfg)

	deps.Effects = NewSQLEffectStore(db, queue, log)
	deps.closers = append(deps.closers, deps.Effects)

	tokens, err := NewTokenSigner(cfg.JWTSecret, cfg.JWTIssuer, cfg.TokenTTL)
	if err != nil {
		return fail(err)
	}
	deps.Tokens = tokens

	deps.Audit = NewAuditObserver(db, log)
	deps.closers = append(deps.closers, deps.Audit)

	// Observers reach the engine through one dispatcher, wrapped in tiers.
	// WithObserver on its own calls an observer *synchronously* on the scheduler's
	// path — fine for a counter, not fine for an audit insert — so the tiering is
	// what keeps observation off the request's critical path.
	deps.dispatcher = observer.NewAsyncDispatcher(4096)
	deps.Observer = observer.NewCompositeObserver(deps.dispatcher,
		// Both are Async rather than Lossy. Async is the right tier for the audit
		// trail regardless; for the metrics it is also the only tier that works,
		// because CompositeObserver forwards ExecutionFinished to its critical and
		// async observers only — a Lossy observer would receive node events and
		// never see an execution finish, so every latency histogram would stay
		// empty. Async still cannot block the scheduler: the dispatcher's send is
		// non-blocking and drops under saturation, which DroppedObservations reports.
		observer.TieredObserver{Observer: deps.Metrics, Tier: observer.Async},
		observer.TieredObserver{Observer: deps.Audit, Tier: observer.Async},
	)
	deps.closers = append(deps.closers, closerFunc(func() error {
		// Close the dispatcher before the audit observer so queued events reach it.
		deps.dispatcher.Close()
		return nil
	}))

	return deps, nil
}

// DB exposes the pool for health checks.
func (d *Deps) DB() *sql.DB { return d.db }

// Close releases everything in reverse order of opening. The effect store goes
// before the database on purpose: it must roll its transactions back while the pool
// is still usable.
func (d *Deps) Close() error {
	var firstErr error
	for i := len(d.closers) - 1; i >= 0; i-- {
		if err := d.closers[i].Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	d.closers = nil
	return firstErr
}

// BuildEngine registers every capability and intent and compiles the plans.
//
// Compilation is where the architecture's promises are checked: a fact with no
// producer, a cycle, or an intent whose policy could never run is an error here,
// before the process serves anything. Nothing in this function performs I/O, which
// is what lets a test compile the engine against stub dependencies.
func BuildEngine(deps *Deps) (*ref.Engine, error) {
	return registerAll(ref.NewEngine(deps.engineOptions()...), deps)
}

// BuildEngineWithApp is the same, attached to an fh application so the app's own
// REF integration is used. It exists because EnableREF returns the engine fh will
// serve, and mounting two engines would be a confusing lie.
func BuildEngineWithApp(app *fh.App, deps *Deps) (*ref.Engine, error) {
	return registerAll(app.EnableREF(deps.engineOptions()...), deps)
}

// engineOptions is the engine's whole configuration: where effects commit, and who
// watches.
func (d *Deps) engineOptions() []ref.Option {
	var options []ref.Option
	if d.Effects != nil {
		// The effect store is what turns a plan into a transaction. Without it REF
		// would still commit effects, but nothing would be atomic. It is optional
		// only so a test can compile the plans without a database.
		options = append(options, ref.WithEffectStore(d.Effects))
	}
	if d.Observer != nil {
		options = append(options, ref.WithObserver(d.Observer))
	}
	return options
}

func registerAll(engine *ref.Engine, deps *Deps) (*ref.Engine, error) {
	for _, registration := range []struct {
		what string
		reg  capability.Registration
	}{
		{"app.auth", NewAuthCapability(deps)},
		{"app.tenant", NewTenantCapability(deps)},
		{"app.tenant.public", NewPublicTenantCapability(deps)},
		{"app.ratelimit.address", NewAddressRateLimitCapability(deps)},
		{"app.ratelimit.principal", NewPrincipalRateLimitCapability(deps)},
		{"app.policy.orders", NewAuthorizationCapability(deps)},
		{"app.catalog", NewCatalogCapability(deps)},
	} {
		if err := ref.RegisterCapability(engine, registration.reg); err != nil {
			return nil, fmt.Errorf("ref-app: register capability %s: %w", registration.what, err)
		}
	}

	if err := ref.Register(engine, RegisterIntent{deps: deps}); err != nil {
		return nil, fmt.Errorf("ref-app: register auth.register: %w", err)
	}
	if err := ref.Register(engine, LoginIntent{deps: deps}); err != nil {
		return nil, fmt.Errorf("ref-app: register auth.login: %w", err)
	}
	if err := ref.Register(engine, CatalogIntent{}); err != nil {
		return nil, fmt.Errorf("ref-app: register catalog.list: %w", err)
	}
	if err := ref.Register(engine, CreateOrderIntent{deps: deps}); err != nil {
		return nil, fmt.Errorf("ref-app: register order.create: %w", err)
	}
	if err := ref.Register(engine, GetOrderIntent{deps: deps}); err != nil {
		return nil, fmt.Errorf("ref-app: register order.get: %w", err)
	}
	if err := ref.Register(engine, ListOrdersIntent{deps: deps}); err != nil {
		return nil, fmt.Errorf("ref-app: register order.list: %w", err)
	}
	if err := ref.Register(engine, CancelOrderIntent{deps: deps}); err != nil {
		return nil, fmt.Errorf("ref-app: register order.cancel: %w", err)
	}

	if err := engine.Compile(); err != nil {
		return nil, fmt.Errorf("ref-app: compile: %w", err)
	}
	return engine, nil
}

// closerFunc adapts a function to io.Closer, so ordered shutdown stays one list.
type closerFunc func() error

func (f closerFunc) Close() error { return f() }

// DroppedObservations reports events the dispatcher shed under load. /healthz serves
// it: a number nobody can see is not a warning.
func (d *Deps) DroppedObservations() uint64 { return d.dispatcher.DroppedCount() }
