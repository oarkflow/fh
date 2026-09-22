package platform

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// Cache providers.
//
// cache.memory and cache.file wrap fh's own kv stores, which already satisfy
// spi.Cache and give us atomic read-modify-write through Mutate — the primitive
// the in-process lock and rate limiter are built on.
//
// cache.sql exists because a multi-replica deployment that has a database but
// no Redis still needs a shared cache, and "just add Redis" is a real
// operational cost to impose on somebody who has not yet needed one. It is a
// plain table with a TTL column, swept lazily on read and periodically in the
// background, which is enough for the cache-aside and idempotency patterns that
// motivate a shared cache in the first place.

func registerCacheResources(r *Registry) {
	mustResource(r, "cache.memory", ResourceFactoryFunc(func(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
		if err := rejectUnknownConfig("cache.memory", spec.Config, "max_entries", "max_entry_bytes", "gc_interval"); err != nil {
			return nil, nil, err
		}
		maxEntries, err := configInt(spec.Config, "max_entries", 10000)
		if err != nil {
			return nil, nil, err
		}
		gc, err := configDuration(spec.Config, "gc_interval", time.Minute)
		if err != nil {
			return nil, nil, err
		}
		store := kv.NewMemoryStore(kv.WithMaxEntries(maxEntries), kv.WithGCInterval(gc))
		return store, store, nil
	}), ResourceKindInfo{
		Family:   "cache",
		Summary:  "In-process cache. Fast and free, but private to one replica — never use it for anything two replicas must agree on.",
		Provides: []string{"Cache", "Locker", "RateLimiter", "CircuitBreaker"},
		Config: []ConfigField{
			{Name: "max_entries", Type: "int", Default: "10000", Summary: "Eviction bound"},
			{Name: "gc_interval", Type: "duration", Default: "1m", Summary: "Background expiry sweep"},
		},
	})

	mustResource(r, "cache.file", ResourceFactoryFunc(func(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
		if err := rejectUnknownConfig("cache.file", spec.Config, "dir", "gc_interval"); err != nil {
			return nil, nil, err
		}
		dir, err := requiredString(spec.Config, "dir")
		if err != nil {
			return nil, nil, err
		}
		gc, err := configDuration(spec.Config, "gc_interval", time.Minute)
		if err != nil {
			return nil, nil, err
		}
		store, err := kv.NewFileStore(dir, kv.WithFileGCInterval(gc))
		return store, store, err
	}), ResourceKindInfo{
		Family:   "cache",
		Summary:  "On-disk cache surviving a restart. Shared only between processes on the same filesystem.",
		Provides: []string{"Cache", "Locker", "RateLimiter", "CircuitBreaker"},
		Config: []ConfigField{
			{Name: "dir", Type: "string", Required: true},
			{Name: "gc_interval", Type: "duration", Default: "1m"},
		},
	})

	mustResource(r, "cache.sql", ResourceFactoryFunc(openSQLCache), ResourceKindInfo{
		Family:   "cache",
		Summary:  "Shared cache in a SQL table. Correct across replicas without adding a second datastore.",
		Provides: []string{"Cache", "CachePrefix", "Locker", "RateLimiter"},
		Config: []ConfigField{
			{Name: "database", Type: "resource", Required: true, Summary: "A database.sql resource to use"},
			{Name: "table", Type: "string", Default: "platform_cache"},
			{Name: "migrate", Type: "bool", Default: "true", Summary: "Create the table if it is missing"},
			{Name: "gc_interval", Type: "duration", Default: "5m", Summary: "Background delete of expired rows"},
		},
	})
}

// sqlCache is a TTL cache over one SQL table.
//
// Expiry is enforced on read (a row past its expiry reads as absent, whatever
// the sweeper has got round to) as well as swept in the background. Doing both
// matters: relying on the sweeper alone would serve stale values between
// sweeps, and relying on read-time filtering alone would grow the table
// without bound.
type sqlCache struct {
	db     *Database
	table  string
	cancel context.CancelFunc
	done   chan struct{}
}

// query rewrites placeholders for the handle's dialect. Every statement in this
// file is written PostgreSQL-style and rebound once here.
func (c *sqlCache) query(statement string) string {
	return rebind(c.db.Dialect, statement)
}

func openSQLCache(ctx context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("cache.sql", spec.Config, "database", "table", "migrate", "gc_interval"); err != nil {
		return nil, nil, err
	}
	db, err := requireSQLHandle(spec, "database")
	if err != nil {
		return nil, nil, err
	}
	table, err := safeIdentifier(configString(spec.Config, "table", "platform_cache"))
	if err != nil {
		return nil, nil, fmt.Errorf("cache.sql %q: %w", spec.Name, err)
	}
	gc, err := configDuration(spec.Config, "gc_interval", 5*time.Minute)
	if err != nil {
		return nil, nil, err
	}
	cache := &sqlCache{db: db, table: table, done: make(chan struct{})}
	if configBool(spec.Config, "migrate", true) {
		if err := cache.migrate(ctx); err != nil {
			return nil, nil, fmt.Errorf("cache.sql %q: %w", spec.Name, err)
		}
	}
	sweepCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	cache.cancel = cancel
	go cache.sweep(sweepCtx, gc)
	return cache, cache, nil
}

func (c *sqlCache) migrate(ctx context.Context) error {
	dialect := c.db.Dialect
	statements := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			cache_key   %s NOT NULL PRIMARY KEY,
			value       %s,
			expires_at  %s NULL
		)`, c.table, textType(dialect), blobType(dialect), timestampType(dialect)),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_expiry_idx ON %s (expires_at)`, c.table, c.table),
	}
	for _, statement := range statements {
		if _, err := c.db.ExecContext(ctx, statement); err != nil {
			// MySQL before 8.0.29 rejects CREATE INDEX IF NOT EXISTS. A
			// pre-existing index is the expected state on every restart after
			// the first, so an index failure is only fatal when the index is
			// genuinely absent — which the following insert path would surface
			// as a missing-table error instead.
			if strings.Contains(statement, "CREATE INDEX") {
				continue
			}
			return err
		}
	}
	return nil
}

func (c *sqlCache) sweep(ctx context.Context, every time.Duration) {
	defer close(c.done)
	if every <= 0 {
		return
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	statement := c.query(fmt.Sprintf("DELETE FROM %s WHERE expires_at IS NOT NULL AND expires_at <= $1", c.table))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// A failed sweep is not worth surfacing: read-time expiry already
			// keeps the cache correct, and the next tick will try again.
			_, _ = c.db.ExecContext(ctx, statement, time.Now().UTC())
		}
	}
}

// Get implements spi.Cache.
func (c *sqlCache) Get(key string) ([]byte, bool, error) {
	return c.GetContext(context.Background(), key)
}

// GetContext implements spi.CacheContext.
func (c *sqlCache) GetContext(ctx context.Context, key string) ([]byte, bool, error) {
	var (
		value   []byte
		expires sql.NullTime
	)
	statement := c.query(fmt.Sprintf("SELECT value, expires_at FROM %s WHERE cache_key = $1", c.table))
	switch err := c.db.QueryRowContext(ctx, statement, key).Scan(&value, &expires); {
	case err == sql.ErrNoRows:
		return nil, false, nil
	case err != nil:
		return nil, false, err
	}
	if expires.Valid && !expires.Time.After(time.Now().UTC()) {
		return nil, false, nil
	}
	return value, true, nil
}

// Set implements spi.Cache.
func (c *sqlCache) Set(key string, value []byte, ttl time.Duration) error {
	return c.SetContext(context.Background(), key, value, ttl)
}

// SetContext implements spi.CacheContext.
func (c *sqlCache) SetContext(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	var expires any
	if ttl > 0 {
		expires = time.Now().UTC().Add(ttl)
	}
	statement := fmt.Sprintf("INSERT INTO %s (cache_key, value, expires_at) VALUES ($1, $2, $3)", c.table)
	if c.db.Dialect == "mysql" {
		statement += upsertClause(c.db.Dialect, "cache_key", "value = VALUES(value)", "expires_at = VALUES(expires_at)")
	} else {
		statement += upsertClause(c.db.Dialect, "cache_key", "value = EXCLUDED.value", "expires_at = EXCLUDED.expires_at")
	}
	_, err := c.db.ExecContext(ctx, c.query(statement), key, value, expires)
	return err
}

// Delete implements spi.Cache.
func (c *sqlCache) Delete(key string) error {
	return c.DeleteContext(context.Background(), key)
}

// DeleteContext implements spi.CacheContext.
func (c *sqlCache) DeleteContext(ctx context.Context, key string) error {
	_, err := c.db.ExecContext(ctx, c.query(fmt.Sprintf("DELETE FROM %s WHERE cache_key = $1", c.table)), key)
	return err
}

// DeletePrefix implements spi.CachePrefix. The LIKE pattern is escaped so a key
// prefix containing % or _ invalidates only what it names.
func (c *sqlCache) DeletePrefix(prefix string) (int, error) {
	pattern := escapeLike(prefix) + "%"
	statement := fmt.Sprintf(`DELETE FROM %s WHERE cache_key LIKE $1 ESCAPE '\'`, c.table)
	result, err := c.db.ExecContext(context.Background(), statement, pattern)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	return int(affected), err
}

// Close stops the background sweeper. It does not close the database, which is
// owned by the database resource this cache borrowed.
func (c *sqlCache) Close() error {
	if c.cancel != nil {
		c.cancel()
		<-c.done
	}
	return nil
}

func escapeLike(text string) string {
	text = strings.ReplaceAll(text, `\`, `\\`)
	text = strings.ReplaceAll(text, "%", `\%`)
	return strings.ReplaceAll(text, "_", `\_`)
}

// Len implements kv.Store. Expired-but-unswept rows are excluded so the count
// reflects what a reader would actually find.
func (c *sqlCache) Len() (int, error) {
	statement := c.query(fmt.Sprintf(
		"SELECT COUNT(*) FROM %s WHERE expires_at IS NULL OR expires_at > $1", c.table))
	var count int
	err := c.db.QueryRowContext(context.Background(), statement, time.Now().UTC()).Scan(&count)
	return count, err
}

// Mutate implements kv.Store's atomic read-modify-write.
//
// The whole operation runs in one transaction with the row locked, which is what
// makes this cache usable as a shared rate limiter and lock table rather than
// only as a cache: two replicas incrementing the same counter cannot lose an
// update. SQLite has no row locks but also no concurrent writers, so its
// transaction is already serialised.
func (c *sqlCache) Mutate(key string, fn func(current []byte, exists bool) (next []byte, ttl time.Duration, ok bool, err error)) error {
	ctx := context.Background()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	selectStatement := fmt.Sprintf("SELECT value, expires_at FROM %s WHERE cache_key = $1", c.table)
	if lock := skipLocked(c.db.Dialect); lock != "" {
		// FOR UPDATE without SKIP LOCKED: this caller must wait for the current
		// holder rather than skip the row, because it needs this specific key.
		selectStatement += " FOR UPDATE"
	}
	var (
		current []byte
		expires sql.NullTime
		exists  bool
	)
	switch err := tx.QueryRowContext(ctx, c.query(selectStatement), key).Scan(&current, &expires); {
	case err == sql.ErrNoRows:
	case err != nil:
		return err
	default:
		exists = !expires.Valid || expires.Time.After(time.Now().UTC())
		if !exists {
			current = nil
		}
	}

	next, ttl, ok, err := fn(current, exists)
	if err != nil {
		return err
	}
	if !ok {
		return tx.Commit()
	}
	var expiry any
	if ttl > 0 {
		expiry = time.Now().UTC().Add(ttl)
	}
	insert := fmt.Sprintf("INSERT INTO %s (cache_key, value, expires_at) VALUES ($1, $2, $3)", c.table)
	if c.db.Dialect == "mysql" {
		insert += upsertClause(c.db.Dialect, "cache_key", "value = VALUES(value)", "expires_at = VALUES(expires_at)")
	} else {
		insert += upsertClause(c.db.Dialect, "cache_key", "value = EXCLUDED.value", "expires_at = EXCLUDED.expires_at")
	}
	if _, err := tx.ExecContext(ctx, c.query(insert), key, next, expiry); err != nil {
		return err
	}
	return tx.Commit()
}
