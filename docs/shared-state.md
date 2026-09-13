# Shared State

fh exposes interface-based shared state through `kv.Provider` and `kv.Store`.
The provider is the configuration and lifecycle boundary; each feature receives
an isolated store namespace. This avoids unrelated features interpreting or
evicting each other's values while still allowing one Redis, PostgreSQL, file,
or in-memory state system to serve the application.

## Contracts

```go
type Provider interface {
    Store(context.Context, string) (kv.Store, error)
    Close() error
}

type Store interface {
    Get(key string) ([]byte, bool, error)
    Set(key string, value []byte, ttl time.Duration) error
    Delete(key string) error
    Len() (int, error)
    Mutate(key string, fn func([]byte, bool) ([]byte, time.Duration, bool, error)) error
    Close() error
}
```

`Provider.Store` must return the same logical store for repeated calls with the
same namespace. Different namespaces must have independent keys, TTLs,
capacity accounting and `Len` results. All operations must be concurrency-safe.

`Mutate` is the critical distributed-state operation. A remote implementation
must execute the read, callback decision and conditional write atomically
across every process using the backend. A process-local mutex is insufficient.
Redis adapters can use a Lua script or an optimistic transaction;
PostgreSQL adapters can use a transaction with row locking or an atomic
upsert/update design.

## Application-owned provider

This complete example shares one provider across sessions, rate limiting and
replay protection. Each feature has a distinct namespace.

```go
package main

import (
    "log"
    "time"

    "github.com/oarkflow/fh"
    "github.com/oarkflow/fh/mw/ratelimiter"
    "github.com/oarkflow/fh/mw/replay"
    "github.com/oarkflow/fh/mw/session"
    "github.com/oarkflow/fh/pkg/storage/kv"
)

func main() {
    state := kv.NewMemoryProvider(
        kv.WithMaxEntries(100_000),
        kv.WithGCInterval(time.Minute),
    )
    app := fh.New(
        fh.WithSecureByDefault(true),
        fh.WithSharedState(state),
    )

    sessions := session.NewSessionManager(
        app.MustStateStore("sessions/default"),
        session.SessionSecret([]byte("replace-with-at-least-32-random-bytes")),
    )
    app.Use(session.New(sessions))
    app.Use(ratelimiter.New(ratelimiter.Config{
        Store:  app.MustStateStore("ratelimit/public-api"),
        Max:    100,
        Window: time.Minute,
    }))
    app.Use(replay.New(replay.Config{
        Store:      app.MustStateStore("replay/public-api"),
        TTL:        5 * time.Minute,
        MaxEntries: 100_000,
    }))

    app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
    log.Fatal(app.ListenWithGracefulShutdown(":8080"))
}
```

`WithSharedState` transfers lifecycle ownership to the application. Shutdown
hooks run first, then fh closes the provider and all of its namespace stores.
Do not close an individual store returned by the provider. Use `StateStore`
instead of `MustStateStore` in libraries or initialization paths that return
errors.

## Built-in providers

### In-memory

```go
state := kv.NewMemoryProvider(
    kv.WithShardCount(64),
    kv.WithMaxEntries(100_000), // applied independently to every namespace
    kv.WithGCInterval(time.Minute),
)
```

This is fast and concurrency-safe but process-local and non-durable. In a
multi-replica deployment, every replica has different state.

### Durable files

```go
state, err := kv.NewFileProvider("/var/lib/my-service/state",
    kv.WithMaxEntrySize(8<<20),
    kv.WithFileGCInterval(time.Minute),
)
if err != nil {
    return err
}
```

The provider creates a hashed directory per namespace with restrictive file
permissions. It survives restarts on one host, but it is not a distributed
store and should not be placed on a filesystem that lacks the required atomic
rename and locking semantics.

## Recommended namespaces

Use stable, explicit names in the form `feature/instance`:

| Feature | Example namespace |
|---|---|
| Sessions | `sessions/default` |
| Fixed/sliding rate limit | `ratelimit/public-api` |
| Nonce or replay markers | `replay/webhooks` |
| API-key registry | `apikey/partners` |
| Response cache | `cache/catalog` |
| IP reputation | `ipreputation/edge` |
| Cluster membership and leases | `cluster/control-plane` |
| Secure transport devices | `securetransport/devices` |
| Secure transport sessions | `securetransport/sessions` |

Never reuse one namespace for features with different value formats. Use
different instance suffixes when two configurations of the same middleware
need different retention or capacity policies.

## Cluster and secure-transport wiring

Components that already accept `kv.Store` can consume a provider namespace
directly:

```go
coordinator, err := cluster.New(cluster.Config{
    Store: app.MustStateStore("cluster/control-plane"),
    Node:  cluster.Node{ID: nodeID, Address: advertiseAddress},
    TTL:   15 * time.Second,
})
```

Secure transport should use separate namespaces for device records, sessions
and replay markers:

```go
cfg.DeviceStore = app.MustStateStore("securetransport/devices")
cfg.SessionStore = app.MustStateStore("securetransport/sessions")
cfg.ReplayStore = app.MustStateStore("securetransport/replay")
```

## Reliability state

Queues, request journals, idempotency records and outbox/inbox stores have
domain-specific atomic operations that cannot safely be reduced to generic
key/value calls. fh therefore uses interface segregation rather than forcing
them through `kv.Store`:

- `RequestJournalStore`
- `IdempotencyRepository`
- `QueueStorage`
- `OutboxStore`
- `InboxStore`

A Redis or PostgreSQL integration may implement both `kv.Provider` and these
domain contracts, sharing its connection pool internally. Queue `Claim`,
idempotency `Begin`, inbox admission and lease mutation must remain atomic
across replicas.

## Adapter checklist

Before treating an external provider as production-ready, verify:

- namespace isolation, including independent `Len` and capacity behavior;
- linearizable or transactionally atomic `Mutate` semantics;
- server-side TTL expiration with documented clock assumptions;
- bounded key/value sizes and connection pools;
- cancellation and deadlines during namespace acquisition;
- TLS, authentication, least-privilege credentials and secret rotation;
- retry behavior that does not duplicate non-idempotent state transitions;
- health/readiness signals and observable latency/error metrics;
- provider shutdown while requests are draining;
- contract, race, fault-injection and multi-process tests.

