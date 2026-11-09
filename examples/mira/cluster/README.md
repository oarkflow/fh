# Cluster ownership example

The cluster package contains rendezvous owner selection, fenced leases,
PostgreSQL lease/outbox adapters, and an mTLS forwarding plane. It does not
transparently migrate live UDP session keys or guarantee failover by itself.

## PostgreSQL setup

The application supplies its own `database/sql` driver and connection pool.
Apply [`cluster/sql/schema.sql`](../../../cluster/sql/schema.sql) using the
deployment's migration process, then construct a `cluster.PostgreSQLStore` with
the application's `*sql.DB`:

```go
store := cluster.PostgreSQLStore{DB: db}
lease, err := store.Acquire(ctx, identity, nodeID, 15*time.Second)
if err != nil {
    return err
}
defer store.Release(context.Background(), lease)
```

Check `lease.Fence` in the same database transaction as every business effect.
The lease is authoritative; rendezvous hashing is only a routing hint. An
in-memory store is for tests, not multi-process ownership.

Run `go test ./cluster -count=1` for local lease and forwarding tests. The
repository's three-node test uses simulated in-memory state; it is not evidence
of PostgreSQL failover under partitions. Validate the production driver,
network partitions, stale-owner fencing, backup, and restore procedures in a
controlled staging environment.
