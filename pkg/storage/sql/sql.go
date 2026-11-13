package sqlstore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectMySQL    Dialect = "mysql"
	DialectSQLite   Dialect = "sqlite"
)

type Config struct {
	DB      *sql.DB
	Dialect Dialect
	// Prefix is sanitized to ASCII letters, digits and underscores and must
	// remain non-empty and at most 47 bytes after sanitization.
	Prefix         string
	IdempotencyTTL time.Duration
	// QueueLeaseTTL sets the SQL queue claim lease. Non-positive values use 30s.
	QueueLeaseTTL time.Duration
}
type Store struct {
	Journal     *JournalStore
	Idempotency *IdempotencyStore
	Queue       *QueueStorage
	c           *core
}
type core struct {
	db               *sql.DB
	dialect          Dialect
	prefix           string
	ttl              time.Duration
	queueLeaseTTL    time.Duration
	queueSchemaReady atomic.Bool
}

type JournalStore struct{ c *core }
type IdempotencyStore struct{ c *core }
type QueueStorage struct {
	c         *core
	keyLeases sync.Map // job ID -> queueKeyLeaseOwner for leases owned by this process
}

type queueKeyLeaseOwner struct {
	key   string
	fence uint64
}

const queueFenceLimit = uint64(1<<63 - 2)

// ErrQueueNotProcessing is returned when a worker tries to change a job that
// is no longer in the processing state. ErrQueueLeaseLost identifies a stale
// or expired fenced claim.
var ErrQueueNotProcessing = errors.New("fh sqlstore: queue job is not processing")
var ErrQueueLeaseLost = fmt.Errorf("%w: lease lost or expired", ErrQueueNotProcessing)

// ErrIdempotencyNotProcessing is returned when a response cannot be committed
// because its matching idempotency record is absent or no longer in flight.
var ErrIdempotencyNotProcessing = errors.New("fh sqlstore: idempotency record is not processing")

func Open(cfg Config) (*Store, error) {
	if cfg.DB == nil {
		return nil, errors.New("fh sqlstore: nil db")
	}
	if cfg.Dialect == "" {
		cfg.Dialect = DialectPostgres
	}
	switch cfg.Dialect {
	case DialectPostgres, DialectMySQL, DialectSQLite:
	default:
		return nil, fmt.Errorf("fh sqlstore: unsupported SQL dialect %q", cfg.Dialect)
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "fh_"
	}
	prefix := sanitizePrefix(cfg.Prefix)
	if prefix == "" {
		return nil, errors.New("fh sqlstore: prefix must contain an ASCII letter, digit, or underscore")
	}
	if len(prefix) > 47 {
		return nil, errors.New("fh sqlstore: sanitized prefix must be at most 47 bytes")
	}
	if cfg.IdempotencyTTL <= 0 {
		cfg.IdempotencyTTL = 24 * time.Hour
	}
	if cfg.QueueLeaseTTL <= 0 {
		cfg.QueueLeaseTTL = 30 * time.Second
	}
	if cfg.QueueLeaseTTL < time.Millisecond {
		return nil, errors.New("fh sqlstore: queue lease TTL must be at least 1ms")
	}
	c := &core{db: cfg.DB, dialect: cfg.Dialect, prefix: prefix, ttl: cfg.IdempotencyTTL, queueLeaseTTL: cfg.QueueLeaseTTL}
	return &Store{Journal: &JournalStore{c}, Idempotency: &IdempotencyStore{c}, Queue: &QueueStorage{c: c}, c: c}, nil
}

func sanitizePrefix(p string) string {
	var b strings.Builder
	for _, r := range p {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func (c *core) queueTxOptions() *sql.TxOptions {
	if c.dialect == DialectPostgres || c.dialect == DialectMySQL {
		return &sql.TxOptions{Isolation: sql.LevelReadCommitted}
	}
	return nil
}

func generateJobID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("fh sqlstore: generate queue job id: %w", err)
	}
	return fmt.Sprintf("sqljob_%d_%x", time.Now().UnixNano(), b), nil
}

func (s *Store) Migrate(ctx context.Context) error {
	s.c.queueSchemaReady.Store(false)
	for _, q := range s.Schema() {
		if _, err := s.c.db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	if s.c.dialect == DialectMySQL {
		table := s.c.prefix + "queue_jobs"
		index := fmt.Sprintf("idx_%squeue_claim", strings.TrimSuffix(s.c.prefix, "_"))
		if err := s.c.migrateIndex(ctx, table, index, "state, visible_at, priority"); err != nil {
			return err
		}
		if err := s.c.requireInnoDBQueueTables(ctx); err != nil {
			return err
		}
		if err := s.c.migrateQueueKeyLeaseKey(ctx); err != nil {
			return err
		}
		if err := s.c.migrateQueueKeyLeaseIndex(ctx); err != nil {
			return err
		}
	}
	if err := s.c.migrateQueueLeaseColumns(ctx); err != nil {
		return err
	}
	s.c.queueSchemaReady.Store(true)
	return nil
}
func (s *Store) Schema() []string { return s.c.schema() }

// MySQL queue claims require transactional row locks in both tables. Refuse
// to advertise multi-replica safety for an existing non-transactional queue.
func (c *core) requireInnoDBQueueTables(ctx context.Context) error {
	for _, suffix := range []string{"queue_jobs", "queue_fences", "queue_key_leases"} {
		table := c.prefix + suffix
		var engine string
		err := c.db.QueryRowContext(ctx, `SELECT engine FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?`, table).Scan(&engine)
		if err != nil {
			return fmt.Errorf("fh sqlstore: inspect storage engine for %s: %w", table, err)
		}
		if !strings.EqualFold(engine, "InnoDB") {
			return fmt.Errorf("fh sqlstore: MySQL queue table %s uses %q; multi-replica queue storage requires InnoDB", table, engine)
		}
	}
	return nil
}

func (c *core) migrateQueueKeyLeaseKey(ctx context.Context) error {
	table := c.prefix + "queue_key_leases"
	readType := func() (string, error) {
		var dataType string
		err := c.db.QueryRowContext(ctx, `SELECT data_type FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND column_name='concurrency_key'`, table).Scan(&dataType)
		return dataType, err
	}
	dataType, err := readType()
	if err != nil {
		return fmt.Errorf("fh sqlstore: inspect %s.concurrency_key: %w", table, err)
	}
	if strings.EqualFold(dataType, "varbinary") {
		return nil
	}
	if _, err := c.db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s MODIFY concurrency_key VARBINARY(720) NOT NULL`, table)); err != nil {
		// Another replica may have completed this idempotent schema change after
		// our metadata read.
		if updatedType, checkErr := readType(); checkErr == nil && strings.EqualFold(updatedType, "varbinary") {
			return nil
		}
		return fmt.Errorf("fh sqlstore: make %s.concurrency_key byte-exact: %w", table, err)
	}
	return nil
}

func (c *core) migrateQueueKeyLeaseIndex(ctx context.Context) error {
	table := c.prefix + "queue_key_leases"
	name := fmt.Sprintf("idx_%sqkle", strings.TrimSuffix(c.prefix, "_"))
	return c.migrateIndex(ctx, table, name, "lease_expires_at")
}

func (c *core) migrateQueueLeaseColumns(ctx context.Context) error {
	table := c.prefix + "queue_jobs"
	columns := []struct {
		name string
		typ  string
	}{
		{name: "lease_fence", typ: "BIGINT NOT NULL DEFAULT 0"},
		{name: "lease_expires_at", typ: "TIMESTAMP(6) NULL"},
	}
	for _, col := range columns {
		exists, err := c.columnExists(ctx, table, col.name)
		if err != nil {
			return fmt.Errorf("fh sqlstore: inspect %s.%s: %w", table, col.name, err)
		}
		if exists {
			continue
		}
		q := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, table, col.name, col.typ)
		if _, err := c.db.ExecContext(ctx, q); err != nil {
			// Another replica may have completed the same idempotent migration
			// between the metadata read and ALTER TABLE.
			if exists, checkErr := c.columnExists(ctx, table, col.name); checkErr == nil && exists {
				continue
			}
			return fmt.Errorf("fh sqlstore: add %s.%s: %w", table, col.name, err)
		}
	}
	return c.migrateQueueLeaseIndex(ctx, table)
}

func (c *core) migrateQueueLeaseIndex(ctx context.Context, table string) error {
	name := fmt.Sprintf("idx_%squeue_lease", strings.TrimSuffix(c.prefix, "_"))
	return c.migrateIndex(ctx, table, name, "state, lease_expires_at")
}

func (c *core) migrateIndex(ctx context.Context, table, name, columns string) error {
	var exists bool
	switch c.dialect {
	case DialectPostgres:
		err := c.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname=current_schema() AND tablename=$1 AND indexname=$2)`, table, name).Scan(&exists)
		if err != nil {
			return err
		}
	case DialectMySQL:
		var count int
		err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name=? AND index_name=?`, table, name).Scan(&count)
		if err != nil {
			return err
		}
		exists = count != 0
	case DialectSQLite:
		var count int
		err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name=? AND name=?`, table, name).Scan(&count)
		if err != nil {
			return err
		}
		exists = count != 0
	default:
		return fmt.Errorf("unsupported SQL dialect %q", c.dialect)
	}
	if exists {
		return nil
	}
	if _, err := c.db.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX %s ON %s (%s)`, name, table, columns)); err != nil {
		// Index creation is check-then-create on MySQL. Treat a duplicate
		// created by another migrator as success, but preserve other failures.
		if c.indexExists(ctx, table, name) {
			return nil
		}
		return fmt.Errorf("fh sqlstore: create queue index %s: %w", name, err)
	}
	return nil
}

func (c *core) indexExists(ctx context.Context, table, name string) bool {
	var exists bool
	switch c.dialect {
	case DialectPostgres:
		return c.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname=current_schema() AND tablename=$1 AND indexname=$2)`, table, name).Scan(&exists) == nil && exists
	case DialectMySQL:
		var count int
		return c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name=? AND index_name=?`, table, name).Scan(&count) == nil && count != 0
	case DialectSQLite:
		var count int
		return c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name=? AND name=?`, table, name).Scan(&count) == nil && count != 0
	default:
		return false
	}
}

func (c *core) columnExists(ctx context.Context, table, column string) (bool, error) {
	switch c.dialect {
	case DialectPostgres:
		var exists bool
		err := c.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 AND column_name=$2)`, table, column).Scan(&exists)
		return exists, err
	case DialectMySQL:
		var count int
		err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND column_name=?`, table, column).Scan(&count)
		return count > 0, err
	case DialectSQLite:
		rows, err := c.db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, table))
		if err != nil {
			return false, err
		}
		defer rows.Close()
		for rows.Next() {
			var cid int
			var name, typ string
			var notNull, primaryKey int
			var defaultValue any
			if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
				return false, err
			}
			if name == column {
				return true, nil
			}
		}
		return false, rows.Err()
	default:
		return false, fmt.Errorf("unsupported SQL dialect %q", c.dialect)
	}
}

func (c *core) currentTimeExpression() string {
	switch c.dialect {
	case DialectPostgres:
		// CURRENT_TIMESTAMP is fixed at transaction start in PostgreSQL. Use
		// the wall clock so lock waits do not consume a lease or let an update
		// succeed based on a timestamp captured before the lease expired. The
		// lease column is TIMESTAMP WITHOUT TIME ZONE for compatibility with
		// existing schemas, so normalize the database wall clock to UTC before
		// storing or comparing it. Otherwise sessions with different TimeZone
		// settings could interpret the same lease differently.
		return "timezone('UTC', clock_timestamp())"
	case DialectMySQL:
		return "CURRENT_TIMESTAMP(6)"
	case DialectSQLite:
		return `strftime('%Y-%m-%d %H:%M:%f','now')`
	default:
		return "CURRENT_TIMESTAMP"
	}
}

func (c *core) queueLeaseExpression(index int) string {
	switch c.dialect {
	case DialectPostgres:
		return fmt.Sprintf(`%s + (%s * INTERVAL '1 microsecond')`, c.currentTimeExpression(), c.p(index))
	case DialectMySQL:
		return fmt.Sprintf(`DATE_ADD(%s, INTERVAL %s MICROSECOND)`, c.currentTimeExpression(), c.p(index))
	case DialectSQLite:
		return fmt.Sprintf(`strftime('%%Y-%%m-%%d %%H:%%M:%%f','now', %s)`, c.p(index))
	default:
		return fmt.Sprintf(`%s + (%s * INTERVAL '1 microsecond')`, c.currentTimeExpression(), c.p(index))
	}
}

func (c *core) queueLeaseArgument() any {
	return c.queueDurationArgument(c.queueLeaseTTL)
}

func (c *core) queueDurationArgument(duration time.Duration) any {
	if c.dialect == DialectSQLite {
		millis := duration / time.Millisecond
		if duration%time.Millisecond != 0 {
			millis++
		}
		return fmt.Sprintf("+%d.%03d seconds", millis/1000, millis%1000)
	}
	micros := duration / time.Microsecond
	if duration%time.Microsecond != 0 {
		micros++
	}
	return int64(micros)
}

func (c *core) schema() []string {
	p := c.prefix
	if c.dialect == DialectMySQL {
		return []string{
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %srequest_journal (id BIGINT AUTO_INCREMENT PRIMARY KEY, request_id VARCHAR(160) NOT NULL, event VARCHAR(80) NOT NULL, method VARCHAR(16), path VARCHAR(1024), status INT, body_hash VARCHAR(128), remote_ip VARCHAR(80), created_at TIMESTAMP NOT NULL)`, p),
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %sidempotency (idem_key VARCHAR(160) PRIMARY KEY, request_hash VARCHAR(160) NOT NULL, method VARCHAR(16), path VARCHAR(1024), state VARCHAR(32) NOT NULL, status_code INT, content_type VARCHAR(255), headers JSON, response LONGBLOB, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL, expires_at TIMESTAMP NOT NULL)`, p),
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %squeue_jobs (id VARCHAR(180) PRIMARY KEY, type VARCHAR(180) NOT NULL, state VARCHAR(32) NOT NULL, payload LONGBLOB, headers JSON, attempts INT NOT NULL, max_attempts INT NOT NULL, visible_at TIMESTAMP NOT NULL, run_at TIMESTAMP NULL, priority INT NOT NULL, concurrency_key VARCHAR(180), last_error TEXT, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL, lease_fence BIGINT NOT NULL DEFAULT 0, lease_expires_at TIMESTAMP(6) NULL) ENGINE=InnoDB`, p),
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %squeue_fences (job_id VARCHAR(180) PRIMARY KEY, fence BIGINT NOT NULL) ENGINE=InnoDB`, p),
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %squeue_key_leases (concurrency_key VARBINARY(720) PRIMARY KEY, job_id VARCHAR(180) NOT NULL, lease_fence BIGINT NOT NULL, lease_expires_at TIMESTAMP(6) NOT NULL) ENGINE=InnoDB`, p),
		}
	}
	blob := "BYTEA"
	auto := "BIGSERIAL PRIMARY KEY"
	jsonType := "JSONB"
	if c.dialect == DialectSQLite {
		blob = "BLOB"
		auto = "INTEGER PRIMARY KEY AUTOINCREMENT"
		jsonType = "TEXT"
	}
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %srequest_journal (id %s, request_id TEXT NOT NULL, event TEXT NOT NULL, method TEXT, path TEXT, status INTEGER, body_hash TEXT, remote_ip TEXT, created_at TIMESTAMP NOT NULL)`, p, auto),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %sidempotency (idem_key TEXT PRIMARY KEY, request_hash TEXT NOT NULL, method TEXT, path TEXT, state TEXT NOT NULL, status_code INTEGER, content_type TEXT, headers %s, response %s, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL, expires_at TIMESTAMP NOT NULL)`, p, jsonType, blob),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %squeue_jobs (id TEXT PRIMARY KEY, type TEXT NOT NULL, state TEXT NOT NULL, payload %s, headers %s, attempts INTEGER NOT NULL, max_attempts INTEGER NOT NULL, visible_at TIMESTAMP NOT NULL, run_at TIMESTAMP NULL, priority INTEGER NOT NULL, concurrency_key TEXT, last_error TEXT, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL, lease_fence BIGINT NOT NULL DEFAULT 0, lease_expires_at TIMESTAMP(6) NULL)`, p, blob, jsonType),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %squeue_fences (job_id TEXT PRIMARY KEY, fence BIGINT NOT NULL)`, p),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %squeue_key_leases (concurrency_key TEXT PRIMARY KEY, job_id TEXT NOT NULL, lease_fence BIGINT NOT NULL, lease_expires_at TIMESTAMP(6) NOT NULL)`, p),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%squeue_claim ON %squeue_jobs (state, visible_at, priority)`, strings.TrimSuffix(p, "_"), p),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%sqkle ON %squeue_key_leases (lease_expires_at)`, strings.TrimSuffix(p, "_"), p),
	}
}
func (c *core) p(n int) string {
	if c.dialect == DialectPostgres {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}
func (c *core) ph(n int) string {
	parts := make([]string, n)
	for i := 1; i <= n; i++ {
		parts[i-1] = c.p(i)
	}
	return strings.Join(parts, ",")
}

func (s *JournalStore) Append(e fh.RequestJournalEntry) error {
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	c := s.c
	q := fmt.Sprintf(`INSERT INTO %srequest_journal (request_id,event,method,path,status,body_hash,remote_ip,created_at) VALUES (%s)`, c.prefix, c.ph(8))
	_, err := c.db.Exec(q, e.RequestID, e.Event, e.Method, e.Path, e.Status, e.BodyHash, e.RemoteIP, e.Time)
	return err
}
func (s *JournalStore) Close() error { return nil }

func (s *IdempotencyStore) Begin(key, reqHash, method, path string) (fh.IdempotencyDecision, *fh.IdempotencyRecord, error) {
	// A concurrent Begin for the same key can win the unique-key race after
	// this transaction's initial read. Retry the complete transaction so the
	// loser observes the committed processing/completed record instead of
	// exposing a backend-specific duplicate-key or serialization error.
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		decision, rec, err := s.beginOnce(key, reqHash, method, path)
		if err == nil {
			return decision, rec, nil
		}
		lastErr = err
	}
	return 0, nil, lastErr
}

func (s *IdempotencyStore) beginOnce(key, reqHash, method, path string) (fh.IdempotencyDecision, *fh.IdempotencyRecord, error) {
	c := s.c
	now := time.Now().UTC()
	tx, err := c.db.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback()
	rec, err := s.get(tx, key)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, nil, err
	}
	if rec != nil && rec.ExpiresAt.Before(now) {
		_, _ = tx.Exec(fmt.Sprintf(`DELETE FROM %sidempotency WHERE idem_key=%s`, c.prefix, c.p(1)), key)
		rec = nil
	}
	if rec != nil {
		if rec.RequestHash != reqHash {
			if err := tx.Commit(); err != nil {
				return 0, nil, err
			}
			return fh.IdempotencyConflict, rec, nil
		}
		if rec.State == "completed" {
			if err := tx.Commit(); err != nil {
				return 0, nil, err
			}
			return fh.IdempotencyReplay, rec, nil
		}
		if err := tx.Commit(); err != nil {
			return 0, nil, err
		}
		return fh.IdempotencyProcessing, rec, nil
	}
	rec = &fh.IdempotencyRecord{Key: key, RequestHash: reqHash, Method: method, Path: path, State: "processing", CreatedAt: now, UpdatedAt: now, ExpiresAt: now.Add(c.ttl)}
	q := fmt.Sprintf(`INSERT INTO %sidempotency (idem_key,request_hash,method,path,state,status_code,content_type,headers,response,created_at,updated_at,expires_at) VALUES (%s)`, c.prefix, c.ph(12))
	if _, err := tx.Exec(q, rec.Key, rec.RequestHash, rec.Method, rec.Path, rec.State, rec.StatusCode, rec.ContentType, nil, nil, rec.CreatedAt, rec.UpdatedAt, rec.ExpiresAt); err != nil {
		return 0, nil, err
	}
	return fh.IdempotencyNew, rec, tx.Commit()
}
func (s *IdempotencyStore) Complete(key, reqHash string, status int, contentType string, headers map[string][]string, response []byte) error {
	c := s.c
	h, err := json.Marshal(headers)
	if err != nil {
		return fmt.Errorf("fh sqlstore: marshal headers: %w", err)
	}
	q := fmt.Sprintf(`UPDATE %sidempotency SET state=%s,status_code=%s,content_type=%s,headers=%s,response=%s,updated_at=%s WHERE idem_key=%s AND request_hash=%s AND state=%s`, c.prefix, c.p(1), c.p(2), c.p(3), c.p(4), c.p(5), c.p(6), c.p(7), c.p(8), c.p(9))
	res, err := c.db.Exec(q, "completed", status, contentType, string(h), response, time.Now().UTC(), key, reqHash, "processing")
	if err != nil {
		return err
	}
	updated, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("fh sqlstore: rows affected: %w", err)
	}
	if updated != 1 {
		return ErrIdempotencyNotProcessing
	}
	return nil
}
func (s *IdempotencyStore) Close() error { return nil }

func (s *IdempotencyStore) PurgeExpired(ctx context.Context, now time.Time) (int, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	c := s.c
	q := fmt.Sprintf(`DELETE FROM %sidempotency WHERE expires_at<=%s`, c.prefix, c.p(1))
	res, err := c.db.ExecContext(ctx, q, now)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("fh sqlstore: rows affected: %w", err)
	}
	return int(n), nil
}
func (s *IdempotencyStore) get(tx *sql.Tx, key string) (*fh.IdempotencyRecord, error) {
	c := s.c
	q := fmt.Sprintf(`SELECT idem_key,request_hash,method,path,state,status_code,content_type,headers,response,created_at,updated_at,expires_at FROM %sidempotency WHERE idem_key=%s`, c.prefix, c.p(1))
	var rec fh.IdempotencyRecord
	var headers sql.NullString
	var resp []byte
	err := tx.QueryRow(q, key).Scan(&rec.Key, &rec.RequestHash, &rec.Method, &rec.Path, &rec.State, &rec.StatusCode, &rec.ContentType, &headers, &resp, &rec.CreatedAt, &rec.UpdatedAt, &rec.ExpiresAt)
	if err != nil {
		return nil, err
	}
	if headers.Valid && headers.String != "" {
		if err := json.Unmarshal([]byte(headers.String), &rec.Headers); err != nil {
			return nil, fmt.Errorf("fh sqlstore: unmarshal headers: %w", err)
		}
	}
	rec.Response = append([]byte(nil), resp...)
	return &rec, nil
}

func (s *QueueStorage) Enqueue(ctx context.Context, job *fh.QueueJob) error {
	if job == nil {
		return errors.New("fh sqlstore: nil job")
	}
	c := s.c
	now := time.Now().UTC()
	createdAtDefault := job.CreatedAt.IsZero()
	visibleAtDefault := job.VisibleAt.IsZero()
	if job.ID == "" {
		id, err := generateJobID()
		if err != nil {
			return err
		}
		job.ID = id
	}
	if createdAtDefault {
		job.CreatedAt = now
	} else {
		job.CreatedAt = job.CreatedAt.UTC()
	}
	job.UpdatedAt = now
	if !job.RunAt.IsZero() {
		job.RunAt = job.RunAt.UTC()
	}
	visibleAtUseDBClock := visibleAtDefault && job.RunAt.IsZero()
	if visibleAtDefault {
		if job.RunAt.IsZero() {
			job.VisibleAt = now
		} else {
			// Preserve delayed scheduling for callers that use QueueStorage
			// directly instead of DurableQueue.EnqueueJob.
			job.VisibleAt = job.RunAt
		}
	} else {
		job.VisibleAt = job.VisibleAt.UTC()
	}
	if job.MaxAttempts <= 0 {
		job.MaxAttempts = 5
	}
	h, err := json.Marshal(job.Headers)
	if err != nil {
		return fmt.Errorf("fh sqlstore: marshal job headers: %w", err)
	}
	fields := []any{job.ID, job.Type, "pending", []byte(job.Payload), string(h), job.Attempts, job.MaxAttempts, job.VisibleAt, nullableTime(job.RunAt), job.Priority, job.ConcurrencyKey, job.LastError, job.CreatedAt, job.UpdatedAt}
	values := make([]string, len(fields))
	args := make([]any, 0, len(fields))
	for i, value := range fields {
		// Default scheduling and bookkeeping timestamps must share the database
		// clock used by Claim. Otherwise producer/worker clock skew can make a
		// fresh job run early or remain invisible across every replica.
		if (i == 7 && visibleAtUseDBClock) || (i == 12 && createdAtDefault) || i == 13 {
			values[i] = c.currentTimeExpression()
			continue
		}
		args = append(args, value)
		values[i] = c.p(len(args))
	}
	q := fmt.Sprintf(`INSERT INTO %squeue_jobs (id,type,state,payload,headers,attempts,max_attempts,visible_at,run_at,priority,concurrency_key,last_error,created_at,updated_at) VALUES (%s)`, c.prefix, strings.Join(values, ","))
	_, err = c.db.ExecContext(ctx, q, args...)
	return err
}

// Claim uses the database clock for due-job and lease-expiry checks. The now
// argument remains for QueueStorage compatibility; using a worker-local clock
// here could let a fast-clock replica claim delayed work early.
func (s *QueueStorage) Claim(ctx context.Context, _ time.Time) (*fh.QueueJob, error) {
	c := s.c
	if c.dialect == DialectPostgres {
		return s.claimPostgres(ctx)
	}
	tx, err := c.db.BeginTx(ctx, c.queueTxOptions())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Read only the candidate key until the conditional state transition wins.
	// Expired processing rows are eligible here as well as pending rows, so a
	// crashed owner can be reclaimed by a live replica without waiting for a
	// process restart or a separate recovery loop. The PostgreSQL path below
	// handles its lock, fence increment and state transition in one statement.
	dbNow := c.currentTimeExpression()
	lockClause := ""
	if c.dialect == DialectMySQL {
		// Hold the InnoDB row lock until the fence and lease transition commit.
		// Without this, replicas can all increment the fence for the same
		// candidate before one conditional update wins.
		lockClause = " FOR UPDATE"
	}
	sel := fmt.Sprintf(`SELECT id FROM %squeue_jobs WHERE (state=%s AND visible_at<=%s AND (run_at IS NULL OR run_at<=%s)) OR (state=%s AND (lease_expires_at IS NULL OR lease_expires_at<=%s)) ORDER BY priority DESC, visible_at ASC LIMIT 1%s`, c.prefix, c.p(1), dbNow, dbNow, c.p(2), dbNow, lockClause)
	var id string
	err = tx.QueryRowContext(ctx, sel, "pending", "processing").Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fh.ErrQueueEmpty
	}
	if err != nil {
		return nil, err
	}
	fence, err := c.nextQueueFence(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	leaseExpiry := c.queueLeaseExpression(3)
	upd := fmt.Sprintf(`UPDATE %squeue_jobs SET state=%s,updated_at=%s,lease_fence=%s,lease_expires_at=%s WHERE id=%s AND ((state=%s AND visible_at<=%s AND (run_at IS NULL OR run_at<=%s)) OR (state=%s AND (lease_expires_at IS NULL OR lease_expires_at<=%s)))`, c.prefix, c.p(1), dbNow, c.p(2), leaseExpiry, c.p(4), c.p(5), dbNow, dbNow, c.p(6), dbNow)
	res, err := tx.ExecContext(ctx, upd, "processing", fence, c.queueLeaseArgument(), id, "pending", "processing")
	if err != nil {
		return nil, err
	}
	if n, err := res.RowsAffected(); err != nil {
		return nil, fmt.Errorf("fh sqlstore: rows affected: %w", err)
	} else if n == 0 {
		return nil, fh.ErrQueueEmpty
	}
	full := fmt.Sprintf(`SELECT id,type,payload,headers,attempts,max_attempts,visible_at,run_at,priority,concurrency_key,last_error,created_at,updated_at,lease_fence,lease_expires_at FROM %squeue_jobs WHERE id=%s AND state=%s`, c.prefix, c.p(1), c.p(2))
	j, err := scanJob(tx.QueryRowContext(ctx, full, id, "processing"))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return j, nil
}

// claimPostgres selects, fences and transitions a job in one database
// statement. Besides reducing network round trips, the locked candidate row
// keeps concurrent replicas from contending on the same job.
func (s *QueueStorage) claimPostgres(ctx context.Context) (*fh.QueueJob, error) {
	c := s.c
	dbNow := c.currentTimeExpression()
	leaseExpiry := c.queueLeaseExpression(1)
	q := fmt.Sprintf(`WITH candidate AS (
		SELECT id FROM %squeue_jobs
		WHERE (state='pending' AND visible_at<=%s AND (run_at IS NULL OR run_at<=%s))
		   OR (state='processing' AND (lease_expires_at IS NULL OR lease_expires_at<=%s))
		ORDER BY priority DESC, visible_at ASC
		LIMIT 1 FOR UPDATE SKIP LOCKED
	), next_fence AS (
		INSERT INTO %squeue_fences (job_id,fence)
		SELECT id,1 FROM candidate
		ON CONFLICT (job_id) DO UPDATE
		SET fence=CASE WHEN %squeue_fences.fence<9223372036854775806 THEN %squeue_fences.fence+1 ELSE %squeue_fences.fence END
		RETURNING job_id,fence
	)
	UPDATE %squeue_jobs AS q
	SET state='processing',updated_at=%s,lease_fence=f.fence,lease_expires_at=%s
	FROM candidate AS c JOIN next_fence AS f ON f.job_id=c.id
	WHERE q.id=c.id AND f.fence<%d
	RETURNING q.id,q.type,q.payload,q.headers,q.attempts,q.max_attempts,q.visible_at,q.run_at,q.priority,q.concurrency_key,q.last_error,q.created_at,q.updated_at,q.lease_fence,q.lease_expires_at`,
		c.prefix, dbNow, dbNow, dbNow,
		c.prefix, c.prefix, c.prefix, c.prefix,
		c.prefix, dbNow, leaseExpiry, queueFenceLimit)
	job, err := scanJob(c.db.QueryRowContext(ctx, q, c.queueLeaseArgument()))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fh.ErrQueueEmpty
	}
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (c *core) nextQueueFence(ctx context.Context, tx *sql.Tx, id string) (uint64, error) {
	table := c.prefix + "queue_fences"
	var upsert string
	switch c.dialect {
	case DialectPostgres:
		upsert = fmt.Sprintf(`INSERT INTO %s (job_id,fence) VALUES (%s,1) ON CONFLICT (job_id) DO UPDATE SET fence=CASE WHEN %s.fence<9223372036854775806 THEN %s.fence+1 ELSE %s.fence END`, table, c.p(1), table, table, table)
	case DialectMySQL:
		upsert = fmt.Sprintf(`INSERT INTO %s (job_id,fence) VALUES (?,1) ON DUPLICATE KEY UPDATE fence=CASE WHEN fence<9223372036854775806 THEN fence+1 ELSE fence END`, table)
	case DialectSQLite:
		upsert = fmt.Sprintf(`INSERT INTO %s (job_id,fence) VALUES (?,1) ON CONFLICT(job_id) DO UPDATE SET fence=CASE WHEN fence<9223372036854775806 THEN fence+1 ELSE fence END`, table)
	default:
		return 0, fmt.Errorf("unsupported SQL dialect %q", c.dialect)
	}
	if _, err := tx.ExecContext(ctx, upsert, id); err != nil {
		return 0, err
	}
	var fence uint64
	err := tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT fence FROM %s WHERE job_id=%s`, table, c.p(1)), id).Scan(&fence)
	if err != nil {
		return 0, err
	}
	if fence == 0 || fence >= queueFenceLimit {
		return 0, errors.New("fh sqlstore: queue fence counter exhausted")
	}
	return fence, nil
}
func (s *QueueStorage) Complete(ctx context.Context, job *fh.QueueJob) error {
	return s.set(ctx, job, "done", nil, 0)
}
func (s *QueueStorage) Fail(ctx context.Context, job *fh.QueueJob, cause error) error {
	return s.set(ctx, job, "failed", cause, 0)
}
func (s *QueueStorage) Retry(ctx context.Context, job *fh.QueueJob, cause error, backoff time.Duration) error {
	if job == nil {
		return nil
	}
	job.Attempts++
	if cause != nil {
		job.LastError = cause.Error()
	}
	if job.MaxAttempts <= 0 {
		job.MaxAttempts = 5
	}
	if job.Attempts >= job.MaxAttempts {
		return s.Fail(ctx, job, cause)
	}
	return s.set(ctx, job, "pending", cause, backoff*time.Duration(job.Attempts))
}

// Defer returns a claimed job to pending without consuming a handler attempt.
// It is used when another worker currently owns the job's concurrency key.
func (s *QueueStorage) Defer(ctx context.Context, job *fh.QueueJob, delay time.Duration) error {
	if job == nil {
		return nil
	}
	if delay < 0 {
		delay = 0
	}
	return s.set(ctx, job, "pending", nil, delay)
}

func (s *QueueStorage) Recover(ctx context.Context) error {
	c := s.c
	dbNow := c.currentTimeExpression()
	tx, err := c.db.BeginTx(ctx, c.queueTxOptions())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	q := fmt.Sprintf(`UPDATE %squeue_jobs SET state=%s,lease_expires_at=NULL,updated_at=%s WHERE state=%s AND (lease_expires_at IS NULL OR lease_expires_at<=%s)`, c.prefix, c.p(1), dbNow, c.p(2), dbNow)
	if _, err := tx.ExecContext(ctx, q, "pending", "processing"); err != nil {
		return err
	}
	// Successful job transitions delete their key lease transactionally. Rows
	// left by crashed workers can be discarded after expiry; active owners are
	// protected by the database clock and their renewal transaction.
	q = fmt.Sprintf(`DELETE FROM %squeue_key_leases WHERE lease_expires_at<=%s`, c.prefix, dbNow)
	if _, err := tx.ExecContext(ctx, q); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *QueueStorage) LeaseTTL() time.Duration { return s.c.queueLeaseTTL }

func (s *QueueStorage) SupportsMultiReplica() bool {
	return s.c.queueSchemaReady.Load() && (s.c.dialect == DialectPostgres || s.c.dialect == DialectMySQL)
}

func (s *QueueStorage) SupportsMultiReplicaKeyConcurrency() bool {
	return s.SupportsMultiReplica()
}

func (s *QueueStorage) AcquireConcurrencyLease(ctx context.Context, job *fh.QueueJob) (bool, error) {
	if job == nil || job.ID == "" || job.ConcurrencyKey == "" || job.LeaseFence == 0 {
		return false, errors.New("fh sqlstore: invalid concurrency lease claim")
	}
	c := s.c
	// Make the post-upsert SELECT observe a conflicting lease committed while
	// the insert waited, even if the server defaults to repeatable read.
	tx, err := c.db.BeginTx(ctx, c.queueTxOptions())
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	jobLock := ""
	if c.dialect == DialectPostgres || c.dialect == DialectMySQL {
		jobLock = " FOR UPDATE"
	}
	var claimID string
	claimQuery := fmt.Sprintf(`SELECT id FROM %squeue_jobs WHERE id=%s AND state=%s AND lease_fence=%s AND lease_expires_at>%s%s`, c.prefix, c.p(1), c.p(2), c.p(3), c.currentTimeExpression(), jobLock)
	if err := tx.QueryRowContext(ctx, claimQuery, job.ID, "processing", job.LeaseFence).Scan(&claimID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, ErrQueueLeaseLost
		}
		return false, err
	}

	leaseExpiry := c.queueLeaseExpression(4)
	table := c.prefix + "queue_key_leases"
	var insert string
	switch c.dialect {
	case DialectPostgres:
		insert = fmt.Sprintf(`INSERT INTO %s (concurrency_key,job_id,lease_fence,lease_expires_at) VALUES (%s,%s,%s,%s) ON CONFLICT (concurrency_key) DO NOTHING`, table, c.p(1), c.p(2), c.p(3), leaseExpiry)
	case DialectMySQL:
		insert = fmt.Sprintf(`INSERT INTO %s (concurrency_key,job_id,lease_fence,lease_expires_at) VALUES (?,?,?,%s) ON DUPLICATE KEY UPDATE concurrency_key=VALUES(concurrency_key)`, table, leaseExpiry)
	case DialectSQLite:
		insert = fmt.Sprintf(`INSERT OR IGNORE INTO %s (concurrency_key,job_id,lease_fence,lease_expires_at) VALUES (?,?,?,%s)`, table, leaseExpiry)
	default:
		return false, fmt.Errorf("unsupported SQL dialect %q", c.dialect)
	}
	if _, err := tx.ExecContext(ctx, insert, job.ConcurrencyKey, job.ID, job.LeaseFence, c.queueLeaseArgument()); err != nil {
		return false, err
	}
	lock := ""
	if c.dialect == DialectPostgres || c.dialect == DialectMySQL {
		lock = " FOR UPDATE"
	}
	selectLease := fmt.Sprintf(`SELECT job_id,lease_fence,(lease_expires_at<=%s) FROM %s WHERE concurrency_key=%s%s`, c.currentTimeExpression(), table, c.p(1), lock)
	var owner string
	var fence uint64
	var expired bool
	if err := tx.QueryRowContext(ctx, selectLease, job.ConcurrencyKey).Scan(&owner, &fence, &expired); err != nil {
		return false, err
	}
	if !expired && (owner != job.ID || fence != job.LeaseFence) {
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return false, nil
	}
	// Key-row acquisition can wait behind another replica. Renew the job claim
	// only after that wait and only if the old fence is still live, so a slow
	// acquisition cannot start a handler with an already-expired claim.
	renewJob := fmt.Sprintf(`UPDATE %squeue_jobs SET lease_expires_at=%s WHERE id=%s AND state=%s AND lease_fence=%s AND lease_expires_at>%s`, c.prefix, c.queueLeaseExpression(1), c.p(2), c.p(3), c.p(4), c.currentTimeExpression())
	res, err := tx.ExecContext(ctx, renewJob, c.queueLeaseArgument(), job.ID, "processing", job.LeaseFence)
	if err != nil {
		return false, err
	}
	updated, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("fh sqlstore: rows affected: %w", err)
	}
	if updated != 1 {
		return false, ErrQueueLeaseLost
	}
	update := fmt.Sprintf(`UPDATE %s SET job_id=%s,lease_fence=%s,lease_expires_at=%s WHERE concurrency_key=%s`, table, c.p(2), c.p(3), c.queueLeaseExpression(1), c.p(4))
	if _, err := tx.ExecContext(ctx, update, c.queueLeaseArgument(), job.ID, job.LeaseFence, job.ConcurrencyKey); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	s.keyLeases.Store(job.ID, queueKeyLeaseOwner{key: job.ConcurrencyKey, fence: job.LeaseFence})
	return true, nil
}

func (s *QueueStorage) ownsConcurrencyLease(job *fh.QueueJob) bool {
	if job == nil {
		return false
	}
	value, ok := s.keyLeases.Load(job.ID)
	if !ok {
		return false
	}
	owner, ok := value.(queueKeyLeaseOwner)
	return ok && owner.key == job.ConcurrencyKey && owner.fence == job.LeaseFence
}

func (s *QueueStorage) forgetConcurrencyLease(job *fh.QueueJob) {
	if !s.ownsConcurrencyLease(job) {
		return
	}
	value, ok := s.keyLeases.Load(job.ID)
	if ok {
		s.keyLeases.CompareAndDelete(job.ID, value)
	}
}

func (s *QueueStorage) ReleaseConcurrencyLease(ctx context.Context, job *fh.QueueJob) error {
	if job == nil || job.ID == "" || job.ConcurrencyKey == "" || job.LeaseFence == 0 {
		return nil
	}
	// set removes the row in the same transaction as a successful job
	// transition and clears this local owner record. Avoid a second round trip.
	if !s.ownsConcurrencyLease(job) {
		return nil
	}
	c := s.c
	q := fmt.Sprintf(`DELETE FROM %squeue_key_leases WHERE concurrency_key=%s AND job_id=%s AND lease_fence=%s`, c.prefix, c.p(1), c.p(2), c.p(3))
	_, err := c.db.ExecContext(ctx, q, job.ConcurrencyKey, job.ID, job.LeaseFence)
	if err == nil {
		s.forgetConcurrencyLease(job)
	}
	return err
}

func (s *QueueStorage) RenewLease(ctx context.Context, job *fh.QueueJob) error {
	if job == nil || job.ID == "" || job.LeaseFence == 0 {
		return ErrQueueLeaseLost
	}
	c := s.c
	if s.ownsConcurrencyLease(job) {
		committed := false
		defer func() {
			if !committed {
				// DurableQueue stops renewal after any renewal error. Drop only the
				// process-local tracking record; the database lease remains fenced
				// and unavailable until its expiry.
				s.forgetConcurrencyLease(job)
			}
		}()
		tx, err := c.db.BeginTx(ctx, c.queueTxOptions())
		if err != nil {
			return err
		}
		defer tx.Rollback()
		leaseExpiry := c.queueLeaseExpression(1)
		q := fmt.Sprintf(`UPDATE %squeue_jobs SET lease_expires_at=%s WHERE id=%s AND state=%s AND lease_fence=%s AND lease_expires_at>%s`, c.prefix, leaseExpiry, c.p(2), c.p(3), c.p(4), c.currentTimeExpression())
		res, err := tx.ExecContext(ctx, q, c.queueLeaseArgument(), job.ID, "processing", job.LeaseFence)
		if err != nil {
			return err
		}
		updated, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("fh sqlstore: rows affected: %w", err)
		}
		if updated != 1 {
			return ErrQueueLeaseLost
		}
		q = fmt.Sprintf(`UPDATE %squeue_key_leases SET lease_expires_at=%s WHERE concurrency_key=%s AND job_id=%s AND lease_fence=%s AND lease_expires_at>%s`, c.prefix, c.queueLeaseExpression(1), c.p(2), c.p(3), c.p(4), c.currentTimeExpression())
		res, err = tx.ExecContext(ctx, q, c.queueLeaseArgument(), job.ConcurrencyKey, job.ID, job.LeaseFence)
		if err != nil {
			return err
		}
		updated, err = res.RowsAffected()
		if err != nil {
			return fmt.Errorf("fh sqlstore: rows affected: %w", err)
		}
		if updated != 1 {
			return ErrQueueLeaseLost
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
		return nil
	}
	leaseExpiry := c.queueLeaseExpression(1)
	q := fmt.Sprintf(`UPDATE %squeue_jobs SET lease_expires_at=%s WHERE id=%s AND state=%s AND lease_fence=%s AND lease_expires_at>%s`, c.prefix, leaseExpiry, c.p(2), c.p(3), c.p(4), c.currentTimeExpression())
	res, err := c.db.ExecContext(ctx, q, c.queueLeaseArgument(), job.ID, "processing", job.LeaseFence)
	if err != nil {
		return err
	}
	updated, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("fh sqlstore: rows affected: %w", err)
	}
	if updated != 1 {
		return ErrQueueLeaseLost
	}
	return nil
}
func (s *QueueStorage) Stats(ctx context.Context) (fh.QueueStats, error) {
	c := s.c
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf(`SELECT state, COUNT(*) FROM %squeue_jobs GROUP BY state`, c.prefix))
	if err != nil {
		return fh.QueueStats{}, err
	}
	defer rows.Close()
	var st fh.QueueStats
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return st, err
		}
		switch state {
		case "pending":
			st.Pending = n
		case "processing":
			st.Processing = n
		case "done":
			st.Done = n
		case "failed":
			st.Failed = n
		}
	}
	return st, rows.Err()
}
func (s *QueueStorage) Close() error { return nil }
func (s *QueueStorage) set(ctx context.Context, job *fh.QueueJob, state string, cause error, delay time.Duration) error {
	if job == nil {
		return nil
	}
	if delay < 0 {
		delay = 0
	}
	c := s.c
	now := time.Now().UTC()
	if cause != nil {
		job.LastError = cause.Error()
	}
	job.UpdatedAt = now
	if delay > 0 {
		job.VisibleAt = now.Add(delay)
	}
	h, err := json.Marshal(job.Headers)
	if err != nil {
		return fmt.Errorf("fh sqlstore: marshal job headers: %w", err)
	}
	if job.LeaseFence == 0 {
		return ErrQueueLeaseLost
	}
	visibleAt := c.p(5)
	args := []any{state, string(h), job.Attempts, job.MaxAttempts}
	if delay > 0 || state == "pending" {
		// Derive retry visibility from the same database clock used for lease
		// ownership; worker clock skew must not shorten a retry backoff. This
		// also makes zero-delay retries immediately eligible instead of reusing
		// the job's previous visibility timestamp.
		visibleAt = c.queueLeaseExpression(5)
		args = append(args, c.queueDurationArgument(delay))
	} else {
		args = append(args, job.VisibleAt)
	}
	args = append(args, nullableTime(job.RunAt), job.Priority, job.ConcurrencyKey, job.LastError, job.ID, "processing", job.LeaseFence)
	q := fmt.Sprintf(`UPDATE %squeue_jobs SET state=%s,headers=%s,attempts=%s,max_attempts=%s,visible_at=%s,run_at=%s,priority=%s,concurrency_key=%s,last_error=%s,updated_at=%s,lease_expires_at=NULL WHERE id=%s AND state=%s AND lease_fence=%s AND lease_expires_at>%s`, c.prefix, c.p(1), c.p(2), c.p(3), c.p(4), visibleAt, c.p(6), c.p(7), c.p(8), c.p(9), c.currentTimeExpression(), c.p(10), c.p(11), c.p(12), c.currentTimeExpression())
	ownsKeyLease := s.ownsConcurrencyLease(job)
	transitioned := false
	if ownsKeyLease {
		defer func() {
			if !transitioned {
				s.forgetConcurrencyLease(job)
			}
		}()
	}
	var tx *sql.Tx
	var execer interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	} = c.db
	if s.ownsConcurrencyLease(job) {
		tx, err = c.db.BeginTx(ctx, c.queueTxOptions())
		if err != nil {
			return err
		}
		defer tx.Rollback()
		execer = tx
	}
	res, err := execer.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}
	updated, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("fh sqlstore: rows affected: %w", err)
	}
	if updated != 1 {
		return ErrQueueLeaseLost
	}
	if tx != nil {
		release := fmt.Sprintf(`DELETE FROM %squeue_key_leases WHERE concurrency_key=%s AND job_id=%s AND lease_fence=%s`, c.prefix, c.p(1), c.p(2), c.p(3))
		if _, err := tx.ExecContext(ctx, release, job.ConcurrencyKey, job.ID, job.LeaseFence); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		transitioned = true
		s.forgetConcurrencyLease(job)
	} else {
		transitioned = true
	}
	return nil
}

type scanner interface{ Scan(...any) error }

func scanJob(row scanner) (*fh.QueueJob, error) {
	var j fh.QueueJob
	var headers sql.NullString
	var payload []byte
	var runAt sql.NullTime
	var leaseExpiresAt sql.NullTime
	err := row.Scan(&j.ID, &j.Type, &payload, &headers, &j.Attempts, &j.MaxAttempts, &j.VisibleAt, &runAt, &j.Priority, &j.ConcurrencyKey, &j.LastError, &j.CreatedAt, &j.UpdatedAt, &j.LeaseFence, &leaseExpiresAt)
	if err != nil {
		return nil, err
	}
	j.Payload = append([]byte(nil), payload...)
	if runAt.Valid {
		j.RunAt = runAt.Time
	}
	if leaseExpiresAt.Valid {
		j.LeaseExpiresAt = leaseExpiresAt.Time
	}
	if headers.Valid && headers.String != "" {
		if err := json.Unmarshal([]byte(headers.String), &j.Headers); err != nil {
			return nil, fmt.Errorf("fh sqlstore: unmarshal queue job headers: %w", err)
		}
	}
	return &j, nil
}
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func (s *QueueStorage) ListJobs(ctx context.Context, state string, limit int) ([]fh.QueueJobSnapshot, error) {
	if state != "" && state != "pending" && state != "processing" && state != "done" && state != "failed" {
		return nil, fmt.Errorf("fh sqlstore: invalid queue state %q", state)
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	c := s.c
	base := fmt.Sprintf(`SELECT id,type,state,payload,headers,attempts,max_attempts,visible_at,run_at,priority,concurrency_key,last_error,created_at,updated_at FROM %squeue_jobs`, c.prefix)
	var rows *sql.Rows
	var err error
	if state == "" {
		q := base + ` ORDER BY updated_at DESC LIMIT ` + c.p(1)
		rows, err = c.db.QueryContext(ctx, q, limit)
	} else {
		q := base + ` WHERE state=` + c.p(1) + ` ORDER BY updated_at DESC LIMIT ` + c.p(2)
		rows, err = c.db.QueryContext(ctx, q, state, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]fh.QueueJobSnapshot, 0, limit)
	for rows.Next() {
		j, st, err := scanJobWithState(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, queueSnapshot(st, j))
	}
	return out, rows.Err()
}

func (s *QueueStorage) RequeueFailed(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("fh sqlstore: missing job id")
	}
	c := s.c
	dbNow := c.currentTimeExpression()
	q := fmt.Sprintf(`UPDATE %squeue_jobs SET state=%s,visible_at=%s,updated_at=%s,last_error=%s WHERE id=%s AND state=%s`, c.prefix, c.p(1), dbNow, dbNow, c.p(2), c.p(3), c.p(4))
	res, err := c.db.ExecContext(ctx, q, "pending", "", id, "failed")
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("fh sqlstore: rows affected: %w", err)
	} else if n == 0 {
		return fmt.Errorf("fh sqlstore: failed job %q not found", id)
	}
	return nil
}

func (s *QueueStorage) DiscardFailed(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("fh sqlstore: missing job id")
	}
	c := s.c
	q := fmt.Sprintf(`DELETE FROM %squeue_jobs WHERE id=%s AND state=%s`, c.prefix, c.p(1), c.p(2))
	res, err := c.db.ExecContext(ctx, q, id, "failed")
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("fh sqlstore: rows affected: %w", err)
	} else if n == 0 {
		return fmt.Errorf("fh sqlstore: failed job %q not found", id)
	}
	return nil
}

func (s *QueueStorage) PurgeJobs(ctx context.Context, state string, before time.Time, limit int) (int, error) {
	if state == "" {
		state = "done"
	}
	if state != "done" && state != "failed" {
		return 0, fmt.Errorf("fh sqlstore: only done or failed jobs can be purged, got %q", state)
	}
	if before.IsZero() {
		before = time.Now().UTC()
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	c := s.c
	q := fmt.Sprintf(`DELETE FROM %squeue_jobs WHERE state=%s AND updated_at<=%s`, c.prefix, c.p(1), c.p(2))
	if c.dialect == DialectPostgres {
		q = fmt.Sprintf(`DELETE FROM %squeue_jobs WHERE id IN (SELECT id FROM %squeue_jobs WHERE state=%s AND updated_at<=%s LIMIT %s)`, c.prefix, c.prefix, c.p(1), c.p(2), c.p(3))
	} else {
		q += ` LIMIT ` + c.p(3)
	}
	res, err := c.db.ExecContext(ctx, q, state, before, limit)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("fh sqlstore: rows affected: %w", err)
	}
	return int(n), nil
}

func scanJobWithState(row scanner) (*fh.QueueJob, string, error) {
	var j fh.QueueJob
	var state string
	var headers sql.NullString
	var payload []byte
	var runAt sql.NullTime
	err := row.Scan(&j.ID, &j.Type, &state, &payload, &headers, &j.Attempts, &j.MaxAttempts, &j.VisibleAt, &runAt, &j.Priority, &j.ConcurrencyKey, &j.LastError, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		return nil, "", err
	}
	j.Payload = append([]byte(nil), payload...)
	if runAt.Valid {
		j.RunAt = runAt.Time
	}
	if headers.Valid && headers.String != "" {
		if err := json.Unmarshal([]byte(headers.String), &j.Headers); err != nil {
			return nil, "", fmt.Errorf("fh sqlstore: unmarshal headers: %w", err)
		}
	}
	return &j, state, nil
}

func queueSnapshot(state string, job *fh.QueueJob) fh.QueueJobSnapshot {
	preview := string(job.Payload)
	if len(preview) > 512 {
		preview = preview[:512] + "..."
	}
	headers := map[string]string(nil)
	if len(job.Headers) > 0 {
		headers = make(map[string]string, len(job.Headers))
		for k, v := range job.Headers {
			headers[k] = v
		}
	}
	return fh.SanitizeQueueJobSnapshot(fh.QueueJobSnapshot{ID: job.ID, Type: job.Type, State: state, Headers: headers, Attempts: job.Attempts, MaxAttempts: job.MaxAttempts, VisibleAt: job.VisibleAt, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt, LastError: job.LastError, Priority: job.Priority, RunAt: job.RunAt, ConcurrencyKey: job.ConcurrencyKey, PayloadBytes: len(job.Payload), PayloadPreview: preview})
}
