package platform

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

// The SQL database provider, and the portability helpers every SQL-backed
// provider in this package shares.
//
// One deliberate constraint runs through all of it: this package never imports a
// driver. `driver "pgx"` works because the application imported
// github.com/jackc/pgx/v5/stdlib; `driver "sqlite"` works because it imported a
// SQLite driver. That keeps fh's dependency tree honest and lets one platform
// binary talk to whatever the deployment actually runs.

// Database is the handle a database.sql resource exposes. It carries the driver
// name alongside the pool because portable DDL and placeholder rewriting need to
// know which dialect they are talking to — and discovering that by trial and
// error at request time is not an option.
type Database struct {
	*sql.DB
	// Driver is the registered driver name from config.
	Driver string
	// Dialect is the normalised family: "postgres", "mysql" or "sqlite".
	Dialect string
	// ReadOnly is a separate pool for read replicas, nil when unconfigured.
	ReadOnly *sql.DB
	// allowlist, when non-empty, is the set of statement prefixes this resource
	// permits. It is a defence in depth for a deployment that lets less-trusted
	// authors write BCL: the connection itself refuses anything unlisted.
	allowlist []string
}

// Reader returns the pool a read-only query should use, falling back to the
// primary when no replica is configured.
func (d *Database) Reader() *sql.DB {
	if d.ReadOnly != nil {
		return d.ReadOnly
	}
	return d.DB
}

// CheckStatement enforces the configured statement allowlist.
func (d *Database) CheckStatement(statement string) error {
	if len(d.allowlist) == 0 {
		return nil
	}
	trimmed := strings.ToUpper(strings.TrimSpace(statement))
	for _, allowed := range d.allowlist {
		if strings.HasPrefix(trimmed, allowed) {
			return nil
		}
	}
	return fmt.Errorf("statement is not permitted by this database resource's allowed_statements")
}

// Close releases the read replica pool; the primary is closed by the resource
// closer that owns it.
func (d *Database) Close() error {
	if d.ReadOnly != nil {
		return d.ReadOnly.Close()
	}
	return nil
}

func registerDatabaseResources(r *Registry) {
	mustResource(r, "database.sql", ResourceFactoryFunc(openDatabase), ResourceKindInfo{
		Family:   "database",
		Summary:  "SQL database over database/sql. Works with any driver the application imports — PostgreSQL, MySQL, SQLite.",
		Provides: []string{"Database"},
		Config: []ConfigField{
			{Name: "driver", Type: "string", Required: true, Summary: `Registered driver name, e.g. "pgx", "mysql", "sqlite"`},
			{Name: "dsn", Type: "string", Required: true, Summary: "Connection string. Keep credentials in the environment."},
			{Name: "read_replica_dsn", Type: "string", Summary: "Optional replica DSN used for read-kind nodes"},
			{Name: "max_open_connections", Type: "int"},
			{Name: "max_idle_connections", Type: "int"},
			{Name: "connection_max_lifetime", Type: "duration"},
			{Name: "connection_max_idle_time", Type: "duration"},
			{Name: "ping", Type: "bool", Summary: "Verify connectivity before the app starts serving"},
			{Name: "migrations", Type: "[]string", Summary: "Statements run in order at startup"},
			{Name: "allowed_statements", Type: "[]string", Summary: "Permitted statement prefixes, e.g. [SELECT INSERT UPDATE]"},
		},
	})
}

func openDatabase(ctx context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("database.sql", spec.Config,
		"driver", "dsn", "read_replica_dsn", "max_open_connections", "max_idle_connections",
		"connection_max_lifetime", "connection_max_idle_time", "ping", "migrations", "allowed_statements",
	); err != nil {
		return nil, nil, err
	}
	driver, err := requiredString(spec.Config, "driver")
	if err != nil {
		return nil, nil, err
	}
	dsn, err := requiredString(spec.Config, "dsn")
	if err != nil {
		return nil, nil, err
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, nil, err
	}
	handle := &Database{DB: db, Driver: driver, Dialect: dialectOf(driver)}
	if err := applyPoolSettings(db, spec.Config); err != nil {
		db.Close()
		return nil, nil, err
	}
	if replicaDSN := configString(spec.Config, "read_replica_dsn", ""); replicaDSN != "" {
		replica, err := sql.Open(driver, replicaDSN)
		if err != nil {
			db.Close()
			return nil, nil, fmt.Errorf("read replica: %w", err)
		}
		if err := applyPoolSettings(replica, spec.Config); err != nil {
			db.Close()
			replica.Close()
			return nil, nil, err
		}
		handle.ReadOnly = replica
	}
	for _, allowed := range configStrings(spec.Config, "allowed_statements") {
		handle.allowlist = append(handle.allowlist, strings.ToUpper(strings.TrimSpace(allowed)))
	}

	if configBool(spec.Config, "ping", false) {
		// Ping before migrations so an unreachable database reports as
		// unreachable rather than as a failed migration.
		if err := db.PingContext(ctx); err != nil {
			handle.closeAll()
			return nil, nil, err
		}
		if handle.ReadOnly != nil {
			if err := handle.ReadOnly.PingContext(ctx); err != nil {
				handle.closeAll()
				return nil, nil, fmt.Errorf("read replica: %w", err)
			}
		}
	}
	for i, statement := range configStrings(spec.Config, "migrations") {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			handle.closeAll()
			return nil, nil, fmt.Errorf("migration[%d]: %w", i, err)
		}
	}
	return handle, closerFunc(handle.closeAll), nil
}

func (d *Database) closeAll() error {
	var firstErr error
	if d.ReadOnly != nil {
		if err := d.ReadOnly.Close(); err != nil {
			firstErr = err
		}
	}
	if err := d.DB.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func applyPoolSettings(db *sql.DB, config map[string]any) error {
	if n, err := configInt(config, "max_open_connections", 0); err != nil {
		return err
	} else if n > 0 {
		db.SetMaxOpenConns(n)
	}
	if n, err := configInt(config, "max_idle_connections", 0); err != nil {
		return err
	} else if n > 0 {
		db.SetMaxIdleConns(n)
	}
	lifetime, err := configDuration(config, "connection_max_lifetime", 0)
	if err != nil {
		return err
	}
	db.SetConnMaxLifetime(lifetime)
	idle, err := configDuration(config, "connection_max_idle_time", 0)
	if err != nil {
		return err
	}
	db.SetConnMaxIdleTime(idle)
	return nil
}

// dialectOf normalises a driver name into a SQL family. Driver names vary
// ("pgx", "postgres", "pq"; "sqlite", "sqlite3", "libsql"), and the DDL and
// placeholder differences we care about are per-family, not per-driver.
func dialectOf(driver string) string {
	name := strings.ToLower(driver)
	switch {
	case strings.Contains(name, "pgx"), strings.Contains(name, "postgres"), name == "pq", strings.Contains(name, "cockroach"):
		return "postgres"
	case strings.Contains(name, "mysql"), strings.Contains(name, "maria"):
		return "mysql"
	case strings.Contains(name, "sqlite"), strings.Contains(name, "libsql"), strings.Contains(name, "duckdb"):
		return "sqlite"
	default:
		// An unknown driver is treated as PostgreSQL-shaped, which is the
		// dialect this package's own DDL is written in. A driver that disagrees
		// will fail loudly on its first migration rather than corrupting data.
		return "postgres"
	}
}

// ---------------------------------------------------------------------------
// Portability helpers
// ---------------------------------------------------------------------------

// requireSQLHandle is requireSQLDependency for providers that need the dialect
// and the read replica too.
func requireSQLHandle(spec ResourceSpec, key string) (*Database, error) {
	name, err := requiredString(spec.Config, key)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", spec.Kind, err)
	}
	resolved, ok := spec.resolved[name]
	if !ok {
		return nil, fmt.Errorf("%s %q: config.%s names unknown resource %q", spec.Kind, spec.Name, key, name)
	}
	handle, ok := resolved.(*Database)
	if !ok {
		return nil, fmt.Errorf("%s %q: resource %q is not a database.sql resource", spec.Kind, spec.Name, name)
	}
	return handle, nil
}

var identifierPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// safeIdentifier validates a configured table or column name.
//
// Table names come from configuration and are interpolated into DDL and DML,
// which cannot be parameterised. Rather than trusting or quoting per dialect,
// this restricts them to a plain identifier — which is every legitimate table
// name and no injection vector.
func safeIdentifier(name string) (string, error) {
	name = strings.TrimSpace(name)
	if !identifierPattern.MatchString(name) {
		return "", fmt.Errorf("%q is not a valid table or column name (letters, digits and underscore only)", name)
	}
	if len(name) > 63 {
		return "", fmt.Errorf("%q is too long for a table name", name)
	}
	return name, nil
}

// safeIdentifiers validates a list of identifiers, used for CRUD column sets
// and ORDER BY clauses built from configuration.
func safeIdentifiers(names []string) ([]string, error) {
	out := make([]string, 0, len(names))
	for _, name := range names {
		safe, err := safeIdentifier(name)
		if err != nil {
			return nil, err
		}
		out = append(out, safe)
	}
	return out, nil
}

// rebind converts $1-style placeholders into the dialect's own form. database/sql
// does not do this, and writing every internal statement three times would be
// both tedious and a place for the three versions to drift.
func rebind(dialect, statement string) string {
	if dialect != "mysql" {
		return statement
	}
	var out strings.Builder
	out.Grow(len(statement))
	for i := 0; i < len(statement); i++ {
		if statement[i] != '$' {
			out.WriteByte(statement[i])
			continue
		}
		j := i + 1
		for j < len(statement) && statement[j] >= '0' && statement[j] <= '9' {
			j++
		}
		if j == i+1 {
			out.WriteByte('$')
			continue
		}
		out.WriteByte('?')
		i = j - 1
	}
	return out.String()
}

// upsertClause returns the dialect's "insert or replace on this key" suffix.
func upsertClause(dialect, key string, assignments ...string) string {
	switch dialect {
	case "mysql":
		return " ON DUPLICATE KEY UPDATE " + strings.Join(assignments, ", ")
	default: // postgres and sqlite share the ON CONFLICT syntax
		return fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s", key, strings.Join(assignments, ", "))
	}
}

// skipLocked returns the row-locking suffix that lets many replicas claim
// different rows from one queue table without blocking each other.
//
// SQLite has no SKIP LOCKED and no real concurrent writers, so it returns
// nothing: a single-writer database does not need the hint, and claiming there
// is serialised by the database itself.
func skipLocked(dialect string) string {
	switch dialect {
	case "postgres":
		return " FOR UPDATE SKIP LOCKED"
	case "mysql":
		return " FOR UPDATE SKIP LOCKED"
	default:
		return ""
	}
}

// timestampType is the column type for an instant, per dialect.
func timestampType(dialect string) string {
	switch dialect {
	case "postgres":
		return "TIMESTAMPTZ"
	case "mysql":
		return "DATETIME(6)"
	default:
		return "TIMESTAMP"
	}
}

// blobType is the column type for opaque bytes, per dialect.
func blobType(dialect string) string {
	switch dialect {
	case "postgres":
		return "BYTEA"
	case "mysql":
		return "LONGBLOB"
	default:
		return "BLOB"
	}
}

// textType is the column type for unbounded text, per dialect.
func textType(dialect string) string {
	switch dialect {
	case "mysql":
		// MySQL cannot index a LONGTEXT column without a prefix length, so keys
		// that must be indexed use a bounded VARCHAR instead.
		return "VARCHAR(512)"
	default:
		return "TEXT"
	}
}

// closerFunc adapts a function to io.Closer.
type closerFunc func() error

// Close implements io.Closer.
func (f closerFunc) Close() error { return f() }

// nowUTC is the platform's single clock read for persisted timestamps. Every
// stored instant is UTC: a mixed-zone timestamp column is impossible to compare
// correctly later, and by then the data is already written.
func nowUTC() time.Time { return time.Now().UTC() }
