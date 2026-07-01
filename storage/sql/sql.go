package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	DB             *sql.DB
	Dialect        Dialect
	Prefix         string
	IdempotencyTTL time.Duration
}
type Store struct {
	Journal     *JournalStore
	Idempotency *IdempotencyStore
	Queue       *QueueStorage
	c           *core
}
type core struct {
	db      *sql.DB
	dialect Dialect
	prefix  string
	ttl     time.Duration
}

type JournalStore struct{ c *core }
type IdempotencyStore struct{ c *core }
type QueueStorage struct{ c *core }

func Open(cfg Config) (*Store, error) {
	if cfg.DB == nil {
		return nil, errors.New("fh sqlstore: nil db")
	}
	if cfg.Dialect == "" {
		cfg.Dialect = DialectPostgres
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "fh_"
	}
	if cfg.IdempotencyTTL <= 0 {
		cfg.IdempotencyTTL = 24 * time.Hour
	}
	c := &core{db: cfg.DB, dialect: cfg.Dialect, prefix: cfg.Prefix, ttl: cfg.IdempotencyTTL}
	return &Store{Journal: &JournalStore{c}, Idempotency: &IdempotencyStore{c}, Queue: &QueueStorage{c}, c: c}, nil
}
func (s *Store) Migrate(ctx context.Context) error {
	for _, q := range s.Schema() {
		if _, err := s.c.db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) Schema() []string { return s.c.schema() }

func (c *core) schema() []string {
	p := c.prefix
	if c.dialect == DialectMySQL {
		return []string{
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %srequest_journal (id BIGINT AUTO_INCREMENT PRIMARY KEY, request_id VARCHAR(160) NOT NULL, event VARCHAR(80) NOT NULL, method VARCHAR(16), path VARCHAR(1024), status INT, body_hash VARCHAR(128), remote_ip VARCHAR(80), created_at TIMESTAMP NOT NULL)`, p),
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %sidempotency (idem_key VARCHAR(160) PRIMARY KEY, request_hash VARCHAR(160) NOT NULL, method VARCHAR(16), path VARCHAR(1024), state VARCHAR(32) NOT NULL, status_code INT, content_type VARCHAR(255), headers JSON, response LONGBLOB, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL, expires_at TIMESTAMP NOT NULL)`, p),
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %squeue_jobs (id VARCHAR(180) PRIMARY KEY, type VARCHAR(180) NOT NULL, state VARCHAR(32) NOT NULL, payload LONGBLOB, headers JSON, attempts INT NOT NULL, max_attempts INT NOT NULL, visible_at TIMESTAMP NOT NULL, run_at TIMESTAMP NULL, priority INT NOT NULL, concurrency_key VARCHAR(180), last_error TEXT, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL)`, p),
			fmt.Sprintf(`CREATE INDEX idx_%squeue_claim ON %squeue_jobs (state, visible_at, priority)`, strings.TrimSuffix(p, "_"), p),
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
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %squeue_jobs (id TEXT PRIMARY KEY, type TEXT NOT NULL, state TEXT NOT NULL, payload %s, headers %s, attempts INTEGER NOT NULL, max_attempts INTEGER NOT NULL, visible_at TIMESTAMP NOT NULL, run_at TIMESTAMP NULL, priority INTEGER NOT NULL, concurrency_key TEXT, last_error TEXT, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL)`, p, blob, jsonType),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%squeue_claim ON %squeue_jobs (state, visible_at, priority)`, strings.TrimSuffix(p, "_"), p),
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
			_ = tx.Commit()
			return fh.IdempotencyConflict, rec, nil
		}
		if rec.State == "completed" {
			_ = tx.Commit()
			return fh.IdempotencyReplay, rec, nil
		}
		_ = tx.Commit()
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
	h, _ := json.Marshal(headers)
	q := fmt.Sprintf(`UPDATE %sidempotency SET state=%s,status_code=%s,content_type=%s,headers=%s,response=%s,updated_at=%s WHERE idem_key=%s AND request_hash=%s`, c.prefix, c.p(1), c.p(2), c.p(3), c.p(4), c.p(5), c.p(6), c.p(7), c.p(8))
	_, err := c.db.Exec(q, "completed", status, contentType, string(h), response, time.Now().UTC(), key, reqHash)
	return err
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
	n, _ := res.RowsAffected()
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
		_ = json.Unmarshal([]byte(headers.String), &rec.Headers)
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
	if job.ID == "" {
		job.ID = fmt.Sprintf("sqljob_%d", now.UnixNano())
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	job.UpdatedAt = now
	if job.VisibleAt.IsZero() {
		job.VisibleAt = now
	}
	if job.MaxAttempts <= 0 {
		job.MaxAttempts = 5
	}
	h, _ := json.Marshal(job.Headers)
	q := fmt.Sprintf(`INSERT INTO %squeue_jobs (id,type,state,payload,headers,attempts,max_attempts,visible_at,run_at,priority,concurrency_key,last_error,created_at,updated_at) VALUES (%s)`, c.prefix, c.ph(14))
	_, err := c.db.ExecContext(ctx, q, job.ID, job.Type, "pending", []byte(job.Payload), string(h), job.Attempts, job.MaxAttempts, job.VisibleAt, nullableTime(job.RunAt), job.Priority, job.ConcurrencyKey, job.LastError, job.CreatedAt, job.UpdatedAt)
	return err
}
func (s *QueueStorage) Claim(ctx context.Context, now time.Time) (*fh.QueueJob, error) {
	c := s.c
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	sel := fmt.Sprintf(`SELECT id,type,payload,headers,attempts,max_attempts,visible_at,run_at,priority,concurrency_key,last_error,created_at,updated_at FROM %squeue_jobs WHERE state=%s AND visible_at<=%s ORDER BY priority DESC, visible_at ASC LIMIT 1`, c.prefix, c.p(1), c.p(2))
	j, err := scanJob(tx.QueryRowContext(ctx, sel, "pending", now))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fh.ErrQueueEmpty
	}
	if err != nil {
		return nil, err
	}
	upd := fmt.Sprintf(`UPDATE %squeue_jobs SET state=%s,updated_at=%s WHERE id=%s AND state=%s`, c.prefix, c.p(1), c.p(2), c.p(3), c.p(4))
	res, err := tx.ExecContext(ctx, upd, "processing", now, j.ID, "pending")
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, fh.ErrQueueEmpty
	}
	return j, tx.Commit()
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
func (s *QueueStorage) Recover(ctx context.Context) error {
	c := s.c
	_, err := c.db.ExecContext(ctx, fmt.Sprintf(`UPDATE %squeue_jobs SET state=%s WHERE state=%s`, c.prefix, c.p(1), c.p(2)), "pending", "processing")
	return err
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
	c := s.c
	now := time.Now().UTC()
	if cause != nil {
		job.LastError = cause.Error()
	}
	job.UpdatedAt = now
	if delay > 0 {
		job.VisibleAt = now.Add(delay)
	}
	h, _ := json.Marshal(job.Headers)
	q := fmt.Sprintf(`UPDATE %squeue_jobs SET state=%s,headers=%s,attempts=%s,max_attempts=%s,visible_at=%s,run_at=%s,priority=%s,concurrency_key=%s,last_error=%s,updated_at=%s WHERE id=%s`, c.prefix, c.p(1), c.p(2), c.p(3), c.p(4), c.p(5), c.p(6), c.p(7), c.p(8), c.p(9), c.p(10), c.p(11))
	_, err := c.db.ExecContext(ctx, q, state, string(h), job.Attempts, job.MaxAttempts, job.VisibleAt, nullableTime(job.RunAt), job.Priority, job.ConcurrencyKey, job.LastError, job.UpdatedAt, job.ID)
	return err
}

type scanner interface{ Scan(...any) error }

func scanJob(row scanner) (*fh.QueueJob, error) {
	var j fh.QueueJob
	var headers sql.NullString
	var payload []byte
	var runAt sql.NullTime
	err := row.Scan(&j.ID, &j.Type, &payload, &headers, &j.Attempts, &j.MaxAttempts, &j.VisibleAt, &runAt, &j.Priority, &j.ConcurrencyKey, &j.LastError, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		return nil, err
	}
	j.Payload = append([]byte(nil), payload...)
	if runAt.Valid {
		j.RunAt = runAt.Time
	}
	if headers.Valid && headers.String != "" {
		_ = json.Unmarshal([]byte(headers.String), &j.Headers)
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
		return nil, errors.New("fh sqlstore: invalid queue state")
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
	now := time.Now().UTC()
	q := fmt.Sprintf(`UPDATE %squeue_jobs SET state=%s,visible_at=%s,updated_at=%s,last_error=%s WHERE id=%s AND state=%s`, c.prefix, c.p(1), c.p(2), c.p(3), c.p(4), c.p(5), c.p(6))
	res, err := c.db.ExecContext(ctx, q, "pending", now, now, "", id, "failed")
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return errors.New("fh sqlstore: failed job not found")
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
	if n, _ := res.RowsAffected(); n == 0 {
		return errors.New("fh sqlstore: failed job not found")
	}
	return nil
}

func (s *QueueStorage) PurgeJobs(ctx context.Context, state string, before time.Time, limit int) (int, error) {
	if state == "" {
		state = "done"
	}
	if state != "done" && state != "failed" {
		return 0, errors.New("fh sqlstore: only done or failed jobs can be purged")
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
	n, _ := res.RowsAffected()
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
		_ = json.Unmarshal([]byte(headers.String), &j.Headers)
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
	return fh.QueueJobSnapshot{ID: job.ID, Type: job.Type, State: state, Headers: headers, Attempts: job.Attempts, MaxAttempts: job.MaxAttempts, VisibleAt: job.VisibleAt, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt, LastError: job.LastError, Priority: job.Priority, RunAt: job.RunAt, ConcurrencyKey: job.ConcurrencyKey, PayloadBytes: len(job.Payload), PayloadPreview: preview}
}
