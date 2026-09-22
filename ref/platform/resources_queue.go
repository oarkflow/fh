package platform

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

// Queue providers.
//
// Both wrap fh's DurableQueue, so retries, attempt counting, visibility timeouts
// and the worker pool are the framework's. They differ in storage:
//
//	queue.file — fh's own file-backed storage. Durable, but one filesystem.
//	queue.sql  — a broker table claimed with FOR UPDATE SKIP LOCKED, so any
//	             number of replicas share one queue and each job is delivered to
//	             exactly one of them.
//
// queue.sql is the one that makes the process engine multi-replica. A durable
// process that parks on a timer needs somebody to wake it, and "whichever
// replica happens to hold the file" is not an answer once there is more than
// one.

func registerQueueResources(r *Registry) {
	mustResource(r, "queue.file", ResourceFactoryFunc(func(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
		if err := rejectUnknownConfig("queue.file", spec.Config,
			"dir", "workers", "max_attempts", "poll_interval", "backoff", "concurrency_limit_by_key"); err != nil {
			return nil, nil, err
		}
		dir, err := requiredString(spec.Config, "dir")
		if err != nil {
			return nil, nil, err
		}
		cfg, err := queueConfig(spec)
		if err != nil {
			return nil, nil, err
		}
		cfg.Dir = dir
		queue, err := fh.OpenDurableQueue(cfg)
		return queue, queue, err
	}), ResourceKindInfo{
		Family:   "queue",
		Summary:  "File-backed durable queue. Survives a restart; visible only to processes sharing the filesystem.",
		Provides: []string{"JobQueue", "QueueDelay", "QueueConsume"},
		Config: []ConfigField{
			{Name: "dir", Type: "string", Required: true},
			{Name: "workers", Type: "int", Default: "1", Summary: "Concurrent handlers in this replica"},
			{Name: "max_attempts", Type: "int", Default: "5"},
			{Name: "poll_interval", Type: "duration", Default: "250ms"},
			{Name: "backoff", Type: "duration", Default: "1s"},
			{Name: "concurrency_limit_by_key", Type: "bool", Summary: "Serialise jobs sharing a concurrency key"},
		},
	})

	mustResource(r, "queue.sql", ResourceFactoryFunc(openSQLQueue), ResourceKindInfo{
		Family:   "queue",
		Summary:  "SQL broker table claimed with row-level leases. One queue shared correctly by every replica.",
		Provides: []string{"JobQueue", "QueueDelay", "QueueConsume"},
		Config: []ConfigField{
			{Name: "database", Type: "resource", Required: true},
			{Name: "table", Type: "string", Default: "platform_jobs"},
			{Name: "migrate", Type: "bool", Default: "true"},
			{Name: "workers", Type: "int", Default: "2"},
			{Name: "max_attempts", Type: "int", Default: "5"},
			{Name: "poll_interval", Type: "duration", Default: "500ms"},
			{Name: "backoff", Type: "duration", Default: "1s"},
			{Name: "visibility_timeout", Type: "duration", Default: "5m", Summary: "How long a claimed job stays invisible before another replica may retry it"},
			{Name: "retention", Type: "duration", Default: "168h", Summary: "How long completed rows are kept"},
		},
	})
}

func queueConfig(spec ResourceSpec) (fh.DurableQueueConfig, error) {
	workers, err := configInt(spec.Config, "workers", 1)
	if err != nil {
		return fh.DurableQueueConfig{}, err
	}
	attempts, err := configInt(spec.Config, "max_attempts", 5)
	if err != nil {
		return fh.DurableQueueConfig{}, err
	}
	poll, err := configDuration(spec.Config, "poll_interval", 250*time.Millisecond)
	if err != nil {
		return fh.DurableQueueConfig{}, err
	}
	backoff, err := configDuration(spec.Config, "backoff", time.Second)
	if err != nil {
		return fh.DurableQueueConfig{}, err
	}
	return fh.DurableQueueConfig{
		Workers:               workers,
		MaxAttempts:           attempts,
		PollInterval:          poll,
		Backoff:               backoff,
		ConcurrencyLimitByKey: configBool(spec.Config, "concurrency_limit_by_key", false),
	}, nil
}

func openSQLQueue(ctx context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("queue.sql", spec.Config,
		"database", "table", "migrate", "workers", "max_attempts", "poll_interval", "backoff",
		"visibility_timeout", "retention", "concurrency_limit_by_key"); err != nil {
		return nil, nil, err
	}
	db, err := requireSQLHandle(spec, "database")
	if err != nil {
		return nil, nil, err
	}
	table, err := safeIdentifier(configString(spec.Config, "table", "platform_jobs"))
	if err != nil {
		return nil, nil, fmt.Errorf("queue.sql %q: %w", spec.Name, err)
	}
	visibility, err := configDuration(spec.Config, "visibility_timeout", 5*time.Minute)
	if err != nil {
		return nil, nil, err
	}
	retention, err := configDuration(spec.Config, "retention", 7*24*time.Hour)
	if err != nil {
		return nil, nil, err
	}
	cfg, err := queueConfig(spec)
	if err != nil {
		return nil, nil, err
	}
	if cfg.Workers <= 1 {
		cfg.Workers = 2
	}
	if cfg.PollInterval < 500*time.Millisecond {
		// A SQL broker polled every 250ms by every replica is a surprising amount
		// of database load for an idle system. Half a second is still responsive
		// and an order of magnitude cheaper at rest.
		cfg.PollInterval = 500 * time.Millisecond
	}

	storage := &sqlQueueStorage{db: db, table: table, visibility: visibility, retention: retention}
	if configBool(spec.Config, "migrate", true) {
		if err := storage.migrate(ctx); err != nil {
			return nil, nil, fmt.Errorf("queue.sql %q: %w", spec.Name, err)
		}
	}
	queue := fh.NewDurableQueue(cfg, storage)
	if err := queue.Recover(); err != nil {
		return nil, nil, fmt.Errorf("queue.sql %q: recover: %w", spec.Name, err)
	}
	return queue, queue, nil
}

// sqlQueueStorage implements fh.QueueStorage over one table.
//
// The claim is the whole design. A worker takes the oldest visible row with
// FOR UPDATE SKIP LOCKED and immediately pushes its visible_at forward by the
// visibility timeout, inside the same transaction. Two consequences follow, and
// both matter: no two replicas can claim the same row, and a replica that dies
// mid-job does not strand it — the row simply becomes visible again once the
// lease lapses, and another replica picks it up.
type sqlQueueStorage struct {
	db         *Database
	table      string
	visibility time.Duration
	retention  time.Duration
}

func (s *sqlQueueStorage) query(statement string) string { return rebind(s.db.Dialect, statement) }

func (s *sqlQueueStorage) migrate(ctx context.Context) error {
	dialect := s.db.Dialect
	statements := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id               %s NOT NULL PRIMARY KEY,
			job_type         %s NOT NULL,
			payload          %s,
			headers          %s,
			state            %s NOT NULL,
			attempts         INTEGER NOT NULL DEFAULT 0,
			max_attempts     INTEGER NOT NULL DEFAULT 5,
			priority         INTEGER NOT NULL DEFAULT 0,
			concurrency_key  %s NULL,
			visible_at       %s NOT NULL,
			created_at       %s NOT NULL,
			updated_at       %s NOT NULL,
			last_error       TEXT NULL
		)`, s.table,
			textType(dialect), textType(dialect), blobType(dialect), blobType(dialect),
			textType(dialect), textType(dialect),
			timestampType(dialect), timestampType(dialect), timestampType(dialect)),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_claim_idx ON %s (state, visible_at, priority)`, s.table, s.table),
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			if strings.Contains(statement, "CREATE INDEX") {
				continue
			}
			return err
		}
	}
	return nil
}

// Enqueue implements fh.QueueStorage.
func (s *sqlQueueStorage) Enqueue(ctx context.Context, job *fh.QueueJob) error {
	headers, err := json.Marshal(job.Headers)
	if err != nil {
		return err
	}
	visible := job.VisibleAt
	if !job.RunAt.IsZero() && job.RunAt.After(visible) {
		visible = job.RunAt
	}
	if visible.IsZero() {
		visible = nowUTC()
	}
	created := job.CreatedAt
	if created.IsZero() {
		created = nowUTC()
	}
	statement := fmt.Sprintf(`INSERT INTO %s
		(id, job_type, payload, headers, state, attempts, max_attempts, priority, concurrency_key, visible_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,'pending',$5,$6,$7,$8,$9,$10,$11)`, s.table)
	_, err = s.db.ExecContext(ctx, s.query(statement),
		job.ID, job.Type, []byte(job.Payload), headers,
		job.Attempts, job.MaxAttempts, job.Priority, nullString(job.ConcurrencyKey),
		visible.UTC(), created.UTC(), nowUTC())
	return err
}

// Claim implements fh.QueueStorage. The returned job is leased, not removed: a
// crash before Complete leaves it to reappear rather than vanish.
func (s *sqlQueueStorage) Claim(ctx context.Context, now time.Time) (*fh.QueueJob, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	selectStatement := fmt.Sprintf(`SELECT id, job_type, payload, headers, attempts, max_attempts, priority, concurrency_key, created_at
		FROM %s WHERE state IN ('pending','processing') AND visible_at <= $1
		ORDER BY priority DESC, visible_at ASC LIMIT 1%s`, s.table, skipLocked(s.db.Dialect))

	var (
		job            fh.QueueJob
		payload        []byte
		headers        []byte
		concurrencyKey sql.NullString
		createdAt      time.Time
	)
	switch err := tx.QueryRowContext(ctx, s.query(selectStatement), now.UTC()).Scan(
		&job.ID, &job.Type, &payload, &headers, &job.Attempts, &job.MaxAttempts,
		&job.Priority, &concurrencyKey, &createdAt,
	); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, err
	}

	lease := now.UTC().Add(s.visibility)
	update := fmt.Sprintf(`UPDATE %s SET state='processing', visible_at=$1, attempts=attempts+1, updated_at=$2 WHERE id=$3`, s.table)
	if _, err := tx.ExecContext(ctx, s.query(update), lease, nowUTC(), job.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	job.Payload = json.RawMessage(payload)
	job.Attempts++
	job.CreatedAt = createdAt
	job.VisibleAt = lease
	job.ConcurrencyKey = concurrencyKey.String
	if len(headers) > 0 {
		_ = json.Unmarshal(headers, &job.Headers)
	}
	return &job, nil
}

// Complete implements fh.QueueStorage.
func (s *sqlQueueStorage) Complete(ctx context.Context, job *fh.QueueJob) error {
	if s.retention <= 0 {
		_, err := s.db.ExecContext(ctx, s.query(fmt.Sprintf("DELETE FROM %s WHERE id=$1", s.table)), job.ID)
		return err
	}
	statement := fmt.Sprintf("UPDATE %s SET state='done', updated_at=$1, last_error=NULL WHERE id=$2", s.table)
	if _, err := s.db.ExecContext(ctx, s.query(statement), nowUTC(), job.ID); err != nil {
		return err
	}
	// Retention is enforced opportunistically on completion rather than by a
	// second background goroutine: completions are exactly when the table grows,
	// and one bounded delete per completion keeps it flat without another timer.
	cutoff := nowUTC().Add(-s.retention)
	purge := fmt.Sprintf("DELETE FROM %s WHERE state='done' AND updated_at < $1", s.table)
	_, _ = s.db.ExecContext(ctx, s.query(purge), cutoff)
	return nil
}

// Retry implements fh.QueueStorage.
func (s *sqlQueueStorage) Retry(ctx context.Context, job *fh.QueueJob, cause error, delay time.Duration) error {
	statement := fmt.Sprintf("UPDATE %s SET state='pending', visible_at=$1, updated_at=$2, last_error=$3 WHERE id=$4", s.table)
	_, err := s.db.ExecContext(ctx, s.query(statement), nowUTC().Add(delay), nowUTC(), errorText(cause), job.ID)
	return err
}

// Fail implements fh.QueueStorage. A failed job is kept, not deleted: the row is
// the dead-letter record, and an operator needs it to decide whether to retry.
func (s *sqlQueueStorage) Fail(ctx context.Context, job *fh.QueueJob, cause error) error {
	statement := fmt.Sprintf("UPDATE %s SET state='failed', updated_at=$1, last_error=$2 WHERE id=$3", s.table)
	_, err := s.db.ExecContext(ctx, s.query(statement), nowUTC(), errorText(cause), job.ID)
	return err
}

// Recover implements fh.QueueStorage. It releases leases this deployment left
// behind, which is what makes a restart mid-job safe.
func (s *sqlQueueStorage) Recover(ctx context.Context) error {
	statement := fmt.Sprintf(`UPDATE %s SET state='pending', updated_at=$1
		WHERE state='processing' AND visible_at <= $2`, s.table)
	_, err := s.db.ExecContext(ctx, s.query(statement), nowUTC(), nowUTC())
	return err
}

// Stats implements fh.QueueStorage.
func (s *sqlQueueStorage) Stats(ctx context.Context) (fh.QueueStats, error) {
	statement := fmt.Sprintf("SELECT state, COUNT(*) FROM %s GROUP BY state", s.table)
	rows, err := s.db.QueryContext(ctx, s.query(statement))
	if err != nil {
		return fh.QueueStats{}, err
	}
	defer rows.Close()
	var stats fh.QueueStats
	for rows.Next() {
		var (
			state string
			count int
		)
		if err := rows.Scan(&state, &count); err != nil {
			return stats, err
		}
		switch state {
		case "pending":
			stats.Pending = count
		case "processing":
			stats.Processing = count
		case "done":
			stats.Done = count
		case "failed":
			stats.Failed = count
		}
	}
	return stats, rows.Err()
}

// Close implements fh.QueueStorage. The database belongs to the resource this
// storage borrowed it from, so there is nothing of ours to close.
func (s *sqlQueueStorage) Close() error { return nil }

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func errorText(err error) any {
	if err == nil {
		return nil
	}
	text := err.Error()
	if len(text) > 2000 {
		text = text[:2000]
	}
	return text
}
