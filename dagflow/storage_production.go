package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	_ "github.com/lib/pq"
)

type Storage interface {
	ExtendedStore
	Migrate(context.Context) error
	Health(context.Context) error
	Close() error
}

type NodeDedupRecord struct {
	DedupKey   string    `json:"dedup_key"`
	TaskID     string    `json:"task_id"`
	WorkflowID string    `json:"workflow_id"`
	NodeID     string    `json:"node_id"`
	InputHash  string    `json:"input_hash"`
	Status     string    `json:"status"`
	Result     any       `json:"result,omitempty"`
	Error      string    `json:"error,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type NodeDedupStore interface {
	GetNodeDedup(key string) (*NodeDedupRecord, error)
	PutNodeDedup(rec NodeDedupRecord) error
}

type PostgresStorage struct{ db *sql.DB }

func NewPostgresStorage(dsn string) (*PostgresStorage, error) {
	if dsn == "" {
		return nil, errors.New("postgres dsn is required")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(30 * time.Minute)
	s := &PostgresStorage{db: db}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Health(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.Migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *PostgresStorage) Close() error                     { return s.db.Close() }
func (s *PostgresStorage) Health(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *PostgresStorage) Migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS dagflow_tasks (
 id text PRIMARY KEY,
 workflow_id text NOT NULL,
 status text NOT NULL,
 tenant_id text NOT NULL DEFAULT '',
 user_id text NOT NULL DEFAULT '',
 idempotency_key text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL,
 updated_at timestamptz NOT NULL,
 data jsonb NOT NULL
)`,
		`CREATE INDEX IF NOT EXISTS idx_dagflow_tasks_workflow_status ON dagflow_tasks(workflow_id,status,updated_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_dagflow_tasks_tenant ON dagflow_tasks(tenant_id,updated_at DESC)`,
		`CREATE TABLE IF NOT EXISTS dagflow_chains (
 id text PRIMARY KEY,
 chain_id text NOT NULL,
 status text NOT NULL,
 created_at timestamptz NOT NULL,
 updated_at timestamptz NOT NULL,
 data jsonb NOT NULL
)`,
		`CREATE INDEX IF NOT EXISTS idx_dagflow_chains_chain_status ON dagflow_chains(chain_id,status,updated_at DESC)`,
		`CREATE TABLE IF NOT EXISTS dagflow_idempotency (
 key text PRIMARY KEY,
 workflow_id text NOT NULL,
 input_hash text NOT NULL,
 task_id text NOT NULL,
 created_at timestamptz NOT NULL,
 data jsonb NOT NULL
)`,
		`CREATE TABLE IF NOT EXISTS dagflow_dlq (
 id text PRIMARY KEY,
 task_id text NOT NULL,
 workflow_id text NOT NULL,
 node_id text NOT NULL,
 created_at timestamptz NOT NULL,
 data jsonb NOT NULL
)`,
		`CREATE INDEX IF NOT EXISTS idx_dagflow_dlq_created ON dagflow_dlq(created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS dagflow_outbox (
 id text PRIMARY KEY,
 topic text NOT NULL,
 status text NOT NULL,
 attempts integer NOT NULL DEFAULT 0,
 next_run_at timestamptz NOT NULL,
 created_at timestamptz NOT NULL,
 updated_at timestamptz NOT NULL,
 data jsonb NOT NULL
)`,
		`CREATE INDEX IF NOT EXISTS idx_dagflow_outbox_claim ON dagflow_outbox(status,next_run_at,created_at)`,
		`CREATE TABLE IF NOT EXISTS dagflow_leases (
 id text PRIMARY KEY,
 task_id text NOT NULL,
 node_id text NOT NULL,
 job_id text NOT NULL,
 worker_id text NOT NULL,
 expires_at timestamptz NOT NULL,
 beat_at timestamptz NOT NULL,
 data jsonb NOT NULL
)`,
		`CREATE INDEX IF NOT EXISTS idx_dagflow_leases_expires ON dagflow_leases(expires_at)`,
		`CREATE TABLE IF NOT EXISTS dagflow_snapshots (
 workflow_id text NOT NULL,
 version text NOT NULL,
 hash text NOT NULL,
 created_at timestamptz NOT NULL,
 data jsonb NOT NULL,
 PRIMARY KEY(workflow_id,version,hash)
)`,
		`CREATE TABLE IF NOT EXISTS dagflow_jobs (
 id text PRIMARY KEY,
 workflow_id text NOT NULL,
 status text NOT NULL,
 attempts integer NOT NULL DEFAULT 0,
 visible_at timestamptz NOT NULL,
 locked_by text NOT NULL DEFAULT '',
 lease_until timestamptz,
 result_data jsonb,
 error text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL,
 updated_at timestamptz NOT NULL,
 data jsonb NOT NULL
)`,
		`CREATE INDEX IF NOT EXISTS idx_dagflow_jobs_claim ON dagflow_jobs(workflow_id,status,visible_at,created_at)`,
		`CREATE TABLE IF NOT EXISTS dagflow_node_dedup (
 dedup_key text PRIMARY KEY,
 task_id text NOT NULL,
 workflow_id text NOT NULL,
 node_id text NOT NULL,
 input_hash text NOT NULL,
 status text NOT NULL,
 created_at timestamptz NOT NULL,
 updated_at timestamptz NOT NULL,
 data jsonb NOT NULL
)`,
	}
	for _, st := range stmts {
		if _, err := s.db.ExecContext(ctx, st); err != nil {
			return err
		}
	}
	return nil
}

func jsonBytes(v any) []byte {
	b, _ := json.Marshal(v)
	if b == nil {
		return []byte("null")
	}
	return b
}
func scanJSON[T any](b []byte, out *T) error { return json.Unmarshal(b, out) }

func (s *PostgresStorage) Create(t *Task) error { return s.Save(t) }
func (s *PostgresStorage) Save(t *Task) error {
	if t == nil {
		return errors.New("nil task")
	}
	now := time.Now()
	if t.CreatedAt.IsZero() {
		t.CreatedAt = now
	}
	t.UpdatedAt = now
	_, err := s.db.Exec(`INSERT INTO dagflow_tasks(id,workflow_id,status,tenant_id,user_id,idempotency_key,created_at,updated_at,data) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT(id) DO UPDATE SET workflow_id=EXCLUDED.workflow_id,status=EXCLUDED.status,tenant_id=EXCLUDED.tenant_id,user_id=EXCLUDED.user_id,idempotency_key=EXCLUDED.idempotency_key,updated_at=EXCLUDED.updated_at,data=EXCLUDED.data`,
		t.ID, t.WorkflowID, string(t.Status), t.TenantID, t.UserID, t.IdempotencyKey, t.CreatedAt, t.UpdatedAt, jsonBytes(t))
	return err
}
func (s *PostgresStorage) Get(id string) (*Task, error) {
	var b []byte
	if err := s.db.QueryRow(`SELECT data FROM dagflow_tasks WHERE id=$1`, id).Scan(&b); err != nil {
		return nil, err
	}
	var t Task
	if err := scanJSON(b, &t); err != nil {
		return nil, err
	}
	return &t, nil
}
func (s *PostgresStorage) List() []*Task {
	rows, err := s.db.Query(`SELECT data FROM dagflow_tasks ORDER BY updated_at DESC LIMIT 500`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []*Task{}
	for rows.Next() {
		var b []byte
		if rows.Scan(&b) == nil {
			var t Task
			if json.Unmarshal(b, &t) == nil {
				out = append(out, &t)
			}
		}
	}
	return out
}
func (s *PostgresStorage) GetIdempotency(key string) (*IdempotencyRecord, error) {
	var b []byte
	if err := s.db.QueryRow(`SELECT data FROM dagflow_idempotency WHERE key=$1`, key).Scan(&b); err != nil {
		return nil, err
	}
	var r IdempotencyRecord
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return &r, nil
}
func (s *PostgresStorage) PutIdempotency(rec IdempotencyRecord) error {
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now()
	}
	_, err := s.db.Exec(`INSERT INTO dagflow_idempotency(key,workflow_id,input_hash,task_id,created_at,data) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(key) DO UPDATE SET data=EXCLUDED.data`, rec.Key, rec.WorkflowID, rec.InputHash, rec.TaskID, rec.CreatedAt, jsonBytes(rec))
	return err
}
func (s *PostgresStorage) AddDLQ(item DLQItem) error {
	if item.ID == "" {
		item.ID = newID("dlq")
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now()
	}
	_, err := s.db.Exec(`INSERT INTO dagflow_dlq(id,task_id,workflow_id,node_id,created_at,data) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO UPDATE SET data=EXCLUDED.data`, item.ID, item.TaskID, item.WorkflowID, item.NodeID, item.CreatedAt, jsonBytes(item))
	return err
}
func (s *PostgresStorage) ListDLQ() []DLQItem {
	rows, err := s.db.Query(`SELECT data FROM dagflow_dlq ORDER BY created_at DESC LIMIT 500`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []DLQItem
	for rows.Next() {
		var b []byte
		if rows.Scan(&b) == nil {
			var it DLQItem
			if json.Unmarshal(b, &it) == nil {
				out = append(out, it)
			}
		}
	}
	return out
}
func (s *PostgresStorage) DeleteDLQ(id string) error {
	_, err := s.db.Exec(`DELETE FROM dagflow_dlq WHERE id=$1`, id)
	return err
}

func (s *PostgresStorage) SaveOutbox(ev OutboxEvent) error {
	now := time.Now()
	if ev.ID == "" {
		ev.ID = newID("outbox")
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = now
	}
	if ev.NextRunAt.IsZero() {
		ev.NextRunAt = now
	}
	ev.UpdatedAt = now
	if ev.Status == "" {
		ev.Status = "pending"
	}
	_, err := s.db.Exec(`INSERT INTO dagflow_outbox(id,topic,status,attempts,next_run_at,created_at,updated_at,data) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(id) DO UPDATE SET status=EXCLUDED.status,attempts=EXCLUDED.attempts,next_run_at=EXCLUDED.next_run_at,updated_at=EXCLUDED.updated_at,data=EXCLUDED.data`, ev.ID, ev.Topic, ev.Status, ev.Attempts, ev.NextRunAt, ev.CreatedAt, ev.UpdatedAt, jsonBytes(ev))
	return err
}
func (s *PostgresStorage) UpdateOutbox(ev OutboxEvent) error { return s.SaveOutbox(ev) }
func (s *PostgresStorage) ListOutbox() []OutboxEvent {
	rows, err := s.db.Query(`SELECT data FROM dagflow_outbox ORDER BY created_at DESC LIMIT 500`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []OutboxEvent
	for rows.Next() {
		var b []byte
		if rows.Scan(&b) == nil {
			var ev OutboxEvent
			if json.Unmarshal(b, &ev) == nil {
				out = append(out, ev)
			}
		}
	}
	return out
}
func (s *PostgresStorage) CreateLease(l WorkerLease) error {
	if l.ID == "" {
		l.ID = newID("lease")
	}
	if l.BeatAt.IsZero() {
		l.BeatAt = time.Now()
	}
	_, err := s.db.Exec(`INSERT INTO dagflow_leases(id,task_id,node_id,job_id,worker_id,expires_at,beat_at,data) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(id) DO UPDATE SET expires_at=EXCLUDED.expires_at,beat_at=EXCLUDED.beat_at,data=EXCLUDED.data`, l.ID, l.TaskID, l.NodeID, l.JobID, l.WorkerID, l.ExpiresAt, l.BeatAt, jsonBytes(l))
	return err
}
func (s *PostgresStorage) HeartbeatLease(id string, extend time.Duration) error {
	now := time.Now()
	_, err := s.db.Exec(`UPDATE dagflow_leases SET beat_at=$2, expires_at=$3 WHERE id=$1`, id, now, now.Add(extend))
	return err
}
func (s *PostgresStorage) ExpireLeases(now time.Time) []WorkerLease {
	rows, err := s.db.Query(`DELETE FROM dagflow_leases WHERE expires_at < $1 RETURNING data`, now)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []WorkerLease
	for rows.Next() {
		var b []byte
		if rows.Scan(&b) == nil {
			var l WorkerLease
			if json.Unmarshal(b, &l) == nil {
				out = append(out, l)
			}
		}
	}
	return out
}
func (s *PostgresStorage) ListLeases() []WorkerLease {
	rows, err := s.db.Query(`SELECT data FROM dagflow_leases ORDER BY expires_at ASC LIMIT 500`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []WorkerLease
	for rows.Next() {
		var b []byte
		if rows.Scan(&b) == nil {
			var l WorkerLease
			if json.Unmarshal(b, &l) == nil {
				out = append(out, l)
			}
		}
	}
	return out
}
func (s *PostgresStorage) DeleteLease(id string) error {
	_, err := s.db.Exec(`DELETE FROM dagflow_leases WHERE id=$1`, id)
	return err
}
func (s *PostgresStorage) SaveSnapshot(sn WorkflowSnapshot) error {
	if sn.CreatedAt.IsZero() {
		sn.CreatedAt = time.Now()
	}
	_, err := s.db.Exec(`INSERT INTO dagflow_snapshots(workflow_id,version,hash,created_at,data) VALUES($1,$2,$3,$4,$5) ON CONFLICT(workflow_id,version,hash) DO UPDATE SET data=EXCLUDED.data`, sn.WorkflowID, sn.Version, sn.Hash, sn.CreatedAt, jsonBytes(sn))
	return err
}
func (s *PostgresStorage) ListSnapshots(id string) []WorkflowSnapshot {
	rows, err := s.db.Query(`SELECT data FROM dagflow_snapshots WHERE workflow_id=$1 ORDER BY created_at DESC`, id)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []WorkflowSnapshot
	for rows.Next() {
		var b []byte
		if rows.Scan(&b) == nil {
			var sn WorkflowSnapshot
			if json.Unmarshal(b, &sn) == nil {
				out = append(out, sn)
			}
		}
	}
	return out
}

func (s *PostgresStorage) GetNodeDedup(key string) (*NodeDedupRecord, error) {
	var b []byte
	err := s.db.QueryRow(`SELECT data FROM dagflow_node_dedup WHERE dedup_key=$1`, key).Scan(&b)
	if err != nil {
		return nil, err
	}
	var r NodeDedupRecord
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return &r, nil
}
func (s *PostgresStorage) PutNodeDedup(rec NodeDedupRecord) error {
	now := time.Now()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = now
	}
	rec.UpdatedAt = now
	_, err := s.db.Exec(`INSERT INTO dagflow_node_dedup(dedup_key,task_id,workflow_id,node_id,input_hash,status,created_at,updated_at,data) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(dedup_key) DO UPDATE SET status=EXCLUDED.status,updated_at=EXCLUDED.updated_at,data=EXCLUDED.data`, rec.DedupKey, rec.TaskID, rec.WorkflowID, rec.NodeID, rec.InputHash, rec.Status, rec.CreatedAt, rec.UpdatedAt, jsonBytes(rec))
	return err
}

// ChainStore adapter, kept separate because Go does not support overloaded Create/Save methods.
type PostgresChainStore struct{ pg *PostgresStorage }

func (s *PostgresStorage) ChainStore() ChainStore     { return PostgresChainStore{pg: s} }
func (c PostgresChainStore) Create(r *ChainRun) error { return c.Save(r) }
func (c PostgresChainStore) Save(r *ChainRun) error {
	if r == nil {
		return errors.New("nil chain run")
	}
	now := time.Now()
	if r.CreatedAt.IsZero() {
		r.CreatedAt = now
	}
	r.UpdatedAt = now
	_, err := c.pg.db.Exec(`INSERT INTO dagflow_chains(id,chain_id,status,created_at,updated_at,data) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO UPDATE SET status=EXCLUDED.status,updated_at=EXCLUDED.updated_at,data=EXCLUDED.data`, r.ID, r.ChainID, string(r.Status), r.CreatedAt, r.UpdatedAt, jsonBytes(r))
	return err
}
func (c PostgresChainStore) Get(id string) (*ChainRun, error) {
	var b []byte
	if err := c.pg.db.QueryRow(`SELECT data FROM dagflow_chains WHERE id=$1`, id).Scan(&b); err != nil {
		return nil, err
	}
	var r ChainRun
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return &r, nil
}
func (c PostgresChainStore) List() []*ChainRun {
	rows, err := c.pg.db.Query(`SELECT data FROM dagflow_chains ORDER BY updated_at DESC LIMIT 500`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []*ChainRun
	for rows.Next() {
		var b []byte
		if rows.Scan(&b) == nil {
			var r ChainRun
			if json.Unmarshal(b, &r) == nil {
				out = append(out, &r)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	return out
}

func (s *PostgresStorage) EnqueueJob(ctx context.Context, job Job) error {
	now := time.Now()
	if job.ID == "" {
		job.ID = newID("job")
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO dagflow_jobs(id,workflow_id,status,attempts,visible_at,created_at,updated_at,data) VALUES($1,$2,'queued',0,$3,$4,$5,$6) ON CONFLICT(id) DO UPDATE SET data=EXCLUDED.data`, job.ID, job.WorkflowID, now, job.CreatedAt, now, jsonBytes(job))
	return err
}
func (s *PostgresStorage) ClaimJob(ctx context.Context, workflowID, workerID string, lease time.Duration) (Job, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback()
	var b []byte
	var id string
	err = tx.QueryRowContext(ctx, `SELECT id,data FROM dagflow_jobs WHERE workflow_id=$1 AND status IN('queued','retry') AND visible_at<=now() ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED`, workflowID).Scan(&id, &b)
	if err != nil {
		return Job{}, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE dagflow_jobs SET status='running',locked_by=$2,lease_until=$3,attempts=attempts+1,updated_at=now() WHERE id=$1`, id, workerID, time.Now().Add(lease))
	if err != nil {
		return Job{}, err
	}
	if err = tx.Commit(); err != nil {
		return Job{}, err
	}
	var j Job
	if err = json.Unmarshal(b, &j); err != nil {
		return Job{}, err
	}
	return j, nil
}
func (s *PostgresStorage) AckJob(ctx context.Context, jobID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE dagflow_jobs SET status='acked',updated_at=now() WHERE id=$1`, jobID)
	return err
}
func (s *PostgresStorage) NackJob(ctx context.Context, jobID string, reason error, delay time.Duration, maxAttempts int) error {
	var attempts int
	_ = s.db.QueryRowContext(ctx, `SELECT attempts FROM dagflow_jobs WHERE id=$1`, jobID).Scan(&attempts)
	status := "retry"
	if maxAttempts > 0 && attempts >= maxAttempts {
		status = "dead"
	}
	_, err := s.db.ExecContext(ctx, `UPDATE dagflow_jobs SET status=$2,error=$3,visible_at=$4,updated_at=now() WHERE id=$1`, jobID, status, fmt.Sprint(reason), time.Now().Add(delay))
	return err
}
func (s *PostgresStorage) CompleteJob(ctx context.Context, r JobResult) error {
	_, err := s.db.ExecContext(ctx, `UPDATE dagflow_jobs SET status='completed',result_data=$2,error=$3,updated_at=now() WHERE id=$1`, r.JobID, jsonBytes(r), r.Error)
	return err
}
func (s *PostgresStorage) WaitJobResult(ctx context.Context, jobID string) (JobResult, error) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		var b []byte
		var status, errText string
		err := s.db.QueryRowContext(ctx, `SELECT COALESCE(result_data,'null'::jsonb),status,error FROM dagflow_jobs WHERE id=$1`, jobID).Scan(&b, &status, &errText)
		if err == nil && status == "completed" {
			var r JobResult
			_ = json.Unmarshal(b, &r)
			return r, nil
		}
		if err == nil && status == "dead" {
			return JobResult{}, errors.New(errText)
		}
		select {
		case <-t.C:
		case <-ctx.Done():
			return JobResult{}, ctx.Err()
		}
	}
}
func (s *PostgresStorage) RecoverExpiredJobs(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `UPDATE dagflow_jobs SET status='retry',visible_at=now(),locked_by='',lease_until=NULL,updated_at=now() WHERE status='running' AND lease_until < now()`)
	return err
}
