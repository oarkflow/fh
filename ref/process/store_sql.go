package process

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// The SQL store: the one that is correct across replicas.
//
// Three mechanisms carry that correctness, and they are worth naming because the
// rest of the file is mechanical:
//
//   - Optimistic revisions. Every run write is `WHERE revision = $n`, and a zero
//     row count is a conflict, not a success. This is what makes two replicas
//     advancing the same run safe even when a lease has lapsed and both believe
//     they hold it.
//   - Atomic lease acquisition. One INSERT … ON CONFLICT that only takes the row
//     when the existing lease has expired. No read-then-write window.
//   - Atomic timer claiming. A timer is deleted as it is read, inside one
//     transaction with SKIP LOCKED, so a due timer fires exactly once across the
//     deployment.
//
// Everything is written in PostgreSQL-shaped SQL and rebound per dialect, so one
// implementation serves PostgreSQL, MySQL and SQLite rather than three that drift.

// SQLStoreConfig configures a SQL-backed store.
type SQLStoreConfig struct {
	// DB is the pool to use. The store never closes it: it belongs to whoever
	// opened it.
	DB *sql.DB
	// Dialect is "postgres", "mysql" or "sqlite".
	Dialect string
	// TablePrefix namespaces the tables, so two applications can share a database.
	TablePrefix string
	// Retention removes terminal runs older than this on each purge. Zero keeps
	// them forever.
	Retention time.Duration
}

type sqlStore struct {
	db      *sql.DB
	dialect string

	runs     string
	steps    string
	timers   string
	subs     string
	joins    string
	leases   string
	tasks    string
	events   string
	sequence string
}

var sqlIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// NewSQLStore builds a SQL-backed store. Table names are derived from the prefix
// and validated, because they are interpolated into every statement and cannot be
// parameterised.
func NewSQLStore(cfg SQLStoreConfig) (Store, error) {
	if cfg.DB == nil {
		return nil, errors.New("ref/process: a SQL store needs a database pool")
	}
	prefix := cfg.TablePrefix
	if prefix == "" {
		prefix = "process"
	}
	if !sqlIdentifier.MatchString(prefix) {
		return nil, fmt.Errorf("ref/process: %q is not a valid table prefix (letters, digits and underscore only)", prefix)
	}
	dialect := strings.ToLower(cfg.Dialect)
	switch dialect {
	case "postgres", "mysql", "sqlite":
	case "":
		dialect = "postgres"
	default:
		return nil, fmt.Errorf("ref/process: unsupported dialect %q", cfg.Dialect)
	}
	return &sqlStore{
		db:       cfg.DB,
		dialect:  dialect,
		runs:     prefix + "_runs",
		steps:    prefix + "_steps",
		timers:   prefix + "_timers",
		subs:     prefix + "_subscriptions",
		joins:    prefix + "_joins",
		leases:   prefix + "_leases",
		tasks:    prefix + "_tasks",
		events:   prefix + "_events",
		sequence: prefix + "_sequences",
	}, nil
}

// q rebinds a PostgreSQL-shaped statement for this dialect.
func (s *sqlStore) q(statement string) string { return rebind(s.dialect, statement) }

func (s *sqlStore) text() string      { return textColumn(s.dialect) }
func (s *sqlStore) timestamp() string { return timestampColumn(s.dialect) }

// Migrate implements Store.
func (s *sqlStore) Migrate(ctx context.Context) error {
	text, stamp := s.text(), s.timestamp()
	statements := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id               %s NOT NULL PRIMARY KEY,
			process          %s NOT NULL,
			version          INTEGER NOT NULL DEFAULT 1,
			status           %s NOT NULL,
			input            TEXT,
			output           TEXT,
			error            TEXT,
			failed_step      %s,
			tenant_id        %s,
			principal_id     %s,
			idempotency_key  %s,
			correlation_id   %s,
			frames           TEXT,
			visits           TEXT,
			step_count       INTEGER NOT NULL DEFAULT 0,
			compensating     TEXT,
			waiting          TEXT,
			revision         BIGINT NOT NULL DEFAULT 0,
			created_at       %s NOT NULL,
			updated_at       %s NOT NULL,
			started_at       %s NULL,
			completed_at     %s NULL,
			deadline_at      %s NULL,
			sla_target_at    %s NULL,
			sla_breach_at    %s NULL,
			sla_breached     INTEGER NOT NULL DEFAULT 0
		)`, s.runs, text, text, text, text, text, text, text, text, stamp, stamp, stamp, stamp, stamp, stamp, stamp),

		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_status_idx ON %s (process, status, updated_at)`, s.runs, s.runs),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_idem_idx ON %s (process, idempotency_key)`, s.runs, s.runs),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_tenant_idx ON %s (tenant_id, status)`, s.runs, s.runs),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			run_id       %s NOT NULL,
			step_key     %s NOT NULL,
			step         %s NOT NULL,
			status       %s NOT NULL,
			attempt      INTEGER NOT NULL DEFAULT 0,
			input        TEXT,
			result       TEXT,
			error        TEXT,
			sequence     BIGINT NOT NULL DEFAULT 0,
			started_at   %s NOT NULL,
			finished_at  %s NULL,
			compensated  INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (run_id, step_key)
		)`, s.steps, text, text, text, text, stamp, stamp),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_run_idx ON %s (run_id, sequence)`, s.steps, s.steps),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id         %s NOT NULL PRIMARY KEY,
			run_id     %s NOT NULL,
			fire_at    %s NOT NULL,
			kind       %s NOT NULL,
			step       %s,
			edge       %s,
			on_fire    %s,
			payload    TEXT,
			created_at %s NOT NULL
		)`, s.timers, text, text, stamp, text, text, text, text, stamp),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_due_idx ON %s (fire_at)`, s.timers, s.timers),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_run_idx ON %s (run_id, kind)`, s.timers, s.timers),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id          %s NOT NULL PRIMARY KEY,
			run_id      %s NOT NULL,
			event       %s NOT NULL,
			correlation %s,
			step        %s,
			edge        %s,
			expires_at  %s NULL,
			created_at  %s NOT NULL
		)`, s.subs, text, text, text, text, text, text, stamp, stamp),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_match_idx ON %s (event, correlation)`, s.subs, s.subs),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_run_idx ON %s (run_id)`, s.subs, s.subs),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			run_id     %s NOT NULL,
			edge       %s NOT NULL,
			sources    TEXT,
			results    TEXT,
			errors     TEXT,
			emitted    INTEGER NOT NULL DEFAULT 0,
			updated_at %s NOT NULL,
			PRIMARY KEY (run_id, edge)
		)`, s.joins, text, text, stamp),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			run_id     %s NOT NULL PRIMARY KEY,
			owner      %s NOT NULL,
			expires_at %s NOT NULL
		)`, s.leases, text, text, stamp),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id                %s NOT NULL PRIMARY KEY,
			run_id            %s NOT NULL,
			process           %s NOT NULL,
			step              %s NOT NULL,
			status            %s NOT NULL,
			title             TEXT,
			instructions      TEXT,
			assignee          %s,
			role              %s,
			queue             %s,
			skills            TEXT,
			forbid_principals TEXT,
			actions           TEXT,
			form_schema       %s,
			priority          INTEGER NOT NULL DEFAULT 0,
			tenant_id         %s,
			data              TEXT,
			claimed_by        %s,
			claimed_at        %s NULL,
			completed_by      %s,
			completed_at      %s NULL,
			action            %s,
			result            TEXT,
			due_at            %s NULL,
			reminder_at       %s NULL,
			created_at        %s NOT NULL,
			updated_at        %s NOT NULL,
			revision          BIGINT NOT NULL DEFAULT 0
		)`, s.tasks, text, text, text, text, text, text, text, text, text, text, text, stamp, text, stamp, text, stamp, stamp, stamp, stamp),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_queue_idx ON %s (status, role, queue)`, s.tasks, s.tasks),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_assignee_idx ON %s (assignee, status)`, s.tasks, s.tasks),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_due_idx ON %s (status, due_at)`, s.tasks, s.tasks),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_run_idx ON %s (run_id)`, s.tasks, s.tasks),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id          %s NOT NULL PRIMARY KEY,
			name        %s NOT NULL,
			correlation %s,
			payload     TEXT,
			received_at %s NOT NULL
		)`, s.events, text, text, text, stamp),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_name_idx ON %s (name, received_at)`, s.events, s.events),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			run_id %s NOT NULL PRIMARY KEY,
			value  BIGINT NOT NULL DEFAULT 0
		)`, s.sequence, text),
	}

	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			// MySQL before 8.0.29 has no CREATE INDEX IF NOT EXISTS, and every
			// restart after the first would fail on an index that already exists.
			// A genuinely missing index surfaces as a slow query, not as silent
			// data loss, so skipping is the right trade.
			if strings.Contains(statement, "CREATE INDEX") {
				continue
			}
			return fmt.Errorf("ref/process: migrate: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

const runColumns = `id, process, version, status, input, output, error, failed_step,
	tenant_id, principal_id, idempotency_key, correlation_id, frames, visits, step_count,
	compensating, waiting, revision, created_at, updated_at, started_at, completed_at,
	deadline_at, sla_target_at, sla_breach_at, sla_breached`

// CreateRun implements Store.
func (s *sqlStore) CreateRun(ctx context.Context, run *Run) error {
	run.Revision = 1
	frames, visits, compensating, waiting, err := encodeRunBlobs(run)
	if err != nil {
		return err
	}
	statement := s.q(fmt.Sprintf(`INSERT INTO %s (%s) VALUES
		($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)`, s.runs, runColumns))
	_, err = s.db.ExecContext(ctx, statement,
		run.ID, run.Process, run.Version, string(run.Status),
		blob(run.Input), blob(run.Output), null(run.Error), null(run.FailedStep),
		null(run.TenantID), null(run.PrincipalID), null(run.IdempotencyKey), null(run.CorrelationID),
		frames, visits, run.Steps, compensating, waiting,
		run.Revision, run.CreatedAt, run.UpdatedAt,
		run.StartedAt, run.CompletedAt, run.DeadlineAt, run.SLATargetAt, run.SLABreachAt, boolInt(run.SLABreached),
	)
	return err
}

// SaveRun implements Store. The revision assertion is the whole point.
func (s *sqlStore) SaveRun(ctx context.Context, run *Run) error {
	frames, visits, compensating, waiting, err := encodeRunBlobs(run)
	if err != nil {
		return err
	}
	run.UpdatedAt = time.Now().UTC()
	next := run.Revision + 1
	statement := s.q(fmt.Sprintf(`UPDATE %s SET
		status=$1, input=$2, output=$3, error=$4, failed_step=$5,
		tenant_id=$6, principal_id=$7, correlation_id=$8,
		frames=$9, visits=$10, step_count=$11, compensating=$12, waiting=$13,
		revision=$14, updated_at=$15, started_at=$16, completed_at=$17,
		deadline_at=$18, sla_target_at=$19, sla_breach_at=$20, sla_breached=$21, version=$22
		WHERE id=$23 AND revision=$24`, s.runs))
	result, err := s.db.ExecContext(ctx, statement,
		string(run.Status), blob(run.Input), blob(run.Output), null(run.Error), null(run.FailedStep),
		null(run.TenantID), null(run.PrincipalID), null(run.CorrelationID),
		frames, visits, run.Steps, compensating, waiting,
		next, run.UpdatedAt, run.StartedAt, run.CompletedAt,
		run.DeadlineAt, run.SLATargetAt, run.SLABreachAt, boolInt(run.SLABreached), run.Version,
		run.ID, run.Revision,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		// A driver that cannot report affected rows cannot enforce the revision
		// check, and a store that cannot enforce it must not pretend to.
		return fmt.Errorf("ref/process: this driver does not report affected rows, so optimistic concurrency cannot be enforced")
	}
	if affected == 0 {
		return ErrRevisionConflict
	}
	run.Revision = next
	return nil
}

// GetRun implements Store.
func (s *sqlStore) GetRun(ctx context.Context, id string) (*Run, error) {
	statement := s.q(fmt.Sprintf("SELECT %s FROM %s WHERE id=$1", runColumns, s.runs))
	run, err := s.scanRun(s.db.QueryRowContext(ctx, statement, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrRunNotFound
	}
	return run, err
}

// FindRunByIdempotency implements Store.
func (s *sqlStore) FindRunByIdempotency(ctx context.Context, process, key string) (*Run, error) {
	if key == "" {
		return nil, ErrRunNotFound
	}
	statement := s.q(fmt.Sprintf(
		"SELECT %s FROM %s WHERE process=$1 AND idempotency_key=$2 ORDER BY created_at DESC LIMIT 1", runColumns, s.runs))
	run, err := s.scanRun(s.db.QueryRowContext(ctx, statement, process, key))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrRunNotFound
	}
	return run, err
}

// ListRuns implements Store.
func (s *sqlStore) ListRuns(ctx context.Context, filter RunFilter) ([]*Run, error) {
	conditions, args := s.runConditions(filter)
	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	args = append(args, limit, filter.Offset)
	statement := s.q(fmt.Sprintf("SELECT %s FROM %s%s ORDER BY created_at DESC LIMIT $%d OFFSET $%d",
		runColumns, s.runs, whereClause(conditions), len(args)-1, len(args)))

	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Run
	for rows.Next() {
		run, err := s.scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

// CountRuns implements Store.
func (s *sqlStore) CountRuns(ctx context.Context, filter RunFilter) (int, error) {
	conditions, args := s.runConditions(filter)
	statement := s.q(fmt.Sprintf("SELECT COUNT(*) FROM %s%s", s.runs, whereClause(conditions)))
	var count int
	err := s.db.QueryRowContext(ctx, statement, args...).Scan(&count)
	return count, err
}

func (s *sqlStore) runConditions(filter RunFilter) ([]string, []any) {
	var (
		conditions []string
		args       []any
	)
	add := func(format string, value any) {
		args = append(args, value)
		conditions = append(conditions, fmt.Sprintf(format, len(args)))
	}
	if filter.Process != "" {
		add("process=$%d", filter.Process)
	}
	if filter.Status != "" {
		add("status=$%d", string(filter.Status))
	}
	if filter.TenantID != "" {
		add("tenant_id=$%d", filter.TenantID)
	}
	if filter.Since != nil {
		add("created_at >= $%d", *filter.Since)
	}
	if filter.Waiting {
		conditions = append(conditions, "status='waiting'")
	}
	if filter.Overdue {
		add("(sla_breach_at IS NOT NULL AND sla_breach_at <= $%d AND status NOT IN ('completed','failed','cancelled'))", time.Now().UTC())
	}
	return conditions, args
}

type rowScanner interface {
	Scan(dest ...any) error
}

func (s *sqlStore) scanRun(row rowScanner) (*Run, error) {
	var (
		run                                                    Run
		status                                                 string
		input, output                                          []byte
		errText, failedStep                                    sql.NullString
		tenant, principal, idempotency, correlation            sql.NullString
		frames, visits, compensating, waiting                  sql.NullString
		startedAt, completedAt, deadlineAt, slaTarget, slaFail sql.NullTime
		slaBreached                                            int
	)
	if err := row.Scan(
		&run.ID, &run.Process, &run.Version, &status, &input, &output, &errText, &failedStep,
		&tenant, &principal, &idempotency, &correlation, &frames, &visits, &run.Steps,
		&compensating, &waiting, &run.Revision, &run.CreatedAt, &run.UpdatedAt,
		&startedAt, &completedAt, &deadlineAt, &slaTarget, &slaFail, &slaBreached,
	); err != nil {
		return nil, err
	}
	run.Status = Status(status)
	run.Input, run.Output = json.RawMessage(input), json.RawMessage(output)
	run.Error, run.FailedStep = errText.String, failedStep.String
	run.TenantID, run.PrincipalID = tenant.String, principal.String
	run.IdempotencyKey, run.CorrelationID = idempotency.String, correlation.String
	run.SLABreached = slaBreached != 0

	if frames.Valid && frames.String != "" {
		if err := json.Unmarshal([]byte(frames.String), &run.Frames); err != nil {
			return nil, fmt.Errorf("ref/process: run %s has an unreadable cursor: %w", run.ID, err)
		}
	}
	if visits.Valid && visits.String != "" {
		_ = json.Unmarshal([]byte(visits.String), &run.Visits)
	}
	if compensating.Valid && compensating.String != "" {
		_ = json.Unmarshal([]byte(compensating.String), &run.Compensating)
	}
	if waiting.Valid && waiting.String != "" {
		var state WaitState
		if json.Unmarshal([]byte(waiting.String), &state) == nil {
			run.Waiting = &state
		}
	}
	run.StartedAt = nullableTime(startedAt)
	run.CompletedAt = nullableTime(completedAt)
	run.DeadlineAt = nullableTime(deadlineAt)
	run.SLATargetAt = nullableTime(slaTarget)
	run.SLABreachAt = nullableTime(slaFail)
	return &run, nil
}

func encodeRunBlobs(run *Run) (any, any, any, any, error) {
	frames, err := json.Marshal(run.Frames)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("ref/process: encode cursor: %w", err)
	}
	visits, err := json.Marshal(run.Visits)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	compensating, err := json.Marshal(run.Compensating)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	var waiting any
	if run.Waiting != nil {
		encoded, err := json.Marshal(run.Waiting)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		waiting = string(encoded)
	}
	return string(frames), string(visits), string(compensating), waiting, nil
}

// ---------------------------------------------------------------------------
// Steps
// ---------------------------------------------------------------------------

// SaveStep implements Store.
func (s *sqlStore) SaveStep(ctx context.Context, state *StepState) error {
	key := state.Key
	if key == "" {
		key = state.Step
	}
	statement := fmt.Sprintf(`INSERT INTO %s
		(run_id, step_key, step, status, attempt, input, result, error, sequence, started_at, finished_at, compensated)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, s.steps)
	if s.dialect == "mysql" {
		statement += ` ON DUPLICATE KEY UPDATE status=VALUES(status), attempt=VALUES(attempt),
			input=VALUES(input), result=VALUES(result), error=VALUES(error), sequence=VALUES(sequence),
			finished_at=VALUES(finished_at), compensated=VALUES(compensated)`
	} else {
		statement += ` ON CONFLICT (run_id, step_key) DO UPDATE SET status=EXCLUDED.status, attempt=EXCLUDED.attempt,
			input=EXCLUDED.input, result=EXCLUDED.result, error=EXCLUDED.error, sequence=EXCLUDED.sequence,
			finished_at=EXCLUDED.finished_at, compensated=EXCLUDED.compensated`
	}
	_, err := s.db.ExecContext(ctx, s.q(statement),
		state.RunID, key, state.Step, string(state.Status), state.Attempt,
		blob(state.Input), blob(state.Result), null(state.Error), state.Sequence,
		state.StartedAt, state.FinishedAt, boolInt(state.Compensated))
	return err
}

// ListSteps implements Store.
func (s *sqlStore) ListSteps(ctx context.Context, runID string) ([]*StepState, error) {
	statement := s.q(fmt.Sprintf(`SELECT run_id, step_key, step, status, attempt, input, result, error,
		sequence, started_at, finished_at, compensated FROM %s WHERE run_id=$1 ORDER BY sequence, step_key`, s.steps))
	rows, err := s.db.QueryContext(ctx, statement, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*StepState
	for rows.Next() {
		var (
			state       StepState
			status      string
			input       []byte
			result      []byte
			errText     sql.NullString
			finishedAt  sql.NullTime
			compensated int
		)
		if err := rows.Scan(&state.RunID, &state.Key, &state.Step, &status, &state.Attempt,
			&input, &result, &errText, &state.Sequence, &state.StartedAt, &finishedAt, &compensated); err != nil {
			return nil, err
		}
		state.Status = StepStatus(status)
		state.Input, state.Result = json.RawMessage(input), json.RawMessage(result)
		state.Error = errText.String
		state.FinishedAt = nullableTime(finishedAt)
		state.Compensated = compensated != 0
		out = append(out, &state)
	}
	return out, rows.Err()
}

// NextStepSequence implements Store. It is an upsert-and-return so two replicas
// cannot hand out the same sequence to two steps of one run.
func (s *sqlStore) NextStepSequence(ctx context.Context, runID string) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	selectStatement := fmt.Sprintf("SELECT value FROM %s WHERE run_id=$1", s.sequence)
	if s.dialect != "sqlite" {
		selectStatement += " FOR UPDATE"
	}
	var value int64
	switch err := tx.QueryRowContext(ctx, s.q(selectStatement), runID).Scan(&value); {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.ExecContext(ctx, s.q(fmt.Sprintf("INSERT INTO %s (run_id, value) VALUES ($1, 1)", s.sequence)), runID); err != nil {
			return 0, err
		}
		value = 1
	case err != nil:
		return 0, err
	default:
		value++
		if _, err := tx.ExecContext(ctx, s.q(fmt.Sprintf("UPDATE %s SET value=$1 WHERE run_id=$2", s.sequence)), value, runID); err != nil {
			return 0, err
		}
	}
	return value, tx.Commit()
}

// ---------------------------------------------------------------------------
// Timers
// ---------------------------------------------------------------------------

// AddTimer implements Store.
func (s *sqlStore) AddTimer(ctx context.Context, timer *Timer) error {
	statement := s.q(fmt.Sprintf(`INSERT INTO %s (id, run_id, fire_at, kind, step, edge, on_fire, payload, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, s.timers))
	_, err := s.db.ExecContext(ctx, statement,
		timer.ID, timer.RunID, timer.Fire.UTC(), timer.Kind,
		null(timer.Step), null(timer.Edge), null(timer.OnFire), blob(timer.Payload), timer.CreatedAt)
	return err
}

// DueTimers implements Store. Claiming is a delete-as-you-read inside one
// transaction, so a due timer fires exactly once across the deployment.
func (s *sqlStore) DueTimers(ctx context.Context, now time.Time, limit int) ([]*Timer, error) {
	if limit <= 0 {
		limit = 50
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	selectStatement := fmt.Sprintf(`SELECT id, run_id, fire_at, kind, step, edge, on_fire, payload, created_at
		FROM %s WHERE fire_at <= $1 ORDER BY fire_at LIMIT %d%s`, s.timers, limit, lockSuffix(s.dialect))
	rows, err := tx.QueryContext(ctx, s.q(selectStatement), now.UTC())
	if err != nil {
		return nil, err
	}
	var (
		out []*Timer
		ids []string
	)
	for rows.Next() {
		var (
			timer              Timer
			step, edge, onFire sql.NullString
			payload            []byte
		)
		if err := rows.Scan(&timer.ID, &timer.RunID, &timer.Fire, &timer.Kind,
			&step, &edge, &onFire, &payload, &timer.CreatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		timer.Step, timer.Edge, timer.OnFire = step.String, edge.String, onFire.String
		timer.Payload = json.RawMessage(payload)
		out = append(out, &timer)
		ids = append(ids, timer.ID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, s.q(fmt.Sprintf("DELETE FROM %s WHERE id=$1", s.timers)), id); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// DeleteTimer implements Store.
func (s *sqlStore) DeleteTimer(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, s.q(fmt.Sprintf("DELETE FROM %s WHERE id=$1", s.timers)), id)
	return err
}

// DeleteRunTimers implements Store.
func (s *sqlStore) DeleteRunTimers(ctx context.Context, runID string, kinds ...string) error {
	if len(kinds) == 0 {
		_, err := s.db.ExecContext(ctx, s.q(fmt.Sprintf("DELETE FROM %s WHERE run_id=$1", s.timers)), runID)
		return err
	}
	args := []any{runID}
	placeholders := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		args = append(args, kind)
		placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
	}
	statement := fmt.Sprintf("DELETE FROM %s WHERE run_id=$1 AND kind IN (%s)", s.timers, strings.Join(placeholders, ","))
	_, err := s.db.ExecContext(ctx, s.q(statement), args...)
	return err
}

// ListTimers implements Store.
func (s *sqlStore) ListTimers(ctx context.Context, runID string) ([]*Timer, error) {
	statement := s.q(fmt.Sprintf(`SELECT id, run_id, fire_at, kind, step, edge, on_fire, payload, created_at
		FROM %s WHERE run_id=$1 ORDER BY fire_at`, s.timers))
	rows, err := s.db.QueryContext(ctx, statement, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Timer
	for rows.Next() {
		var (
			timer              Timer
			step, edge, onFire sql.NullString
			payload            []byte
		)
		if err := rows.Scan(&timer.ID, &timer.RunID, &timer.Fire, &timer.Kind,
			&step, &edge, &onFire, &payload, &timer.CreatedAt); err != nil {
			return nil, err
		}
		timer.Step, timer.Edge, timer.OnFire = step.String, edge.String, onFire.String
		timer.Payload = json.RawMessage(payload)
		out = append(out, &timer)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

// Subscribe implements Store.
func (s *sqlStore) Subscribe(ctx context.Context, subscription *Subscription) error {
	statement := s.q(fmt.Sprintf(`INSERT INTO %s (id, run_id, event, correlation, step, edge, expires_at, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, s.subs))
	_, err := s.db.ExecContext(ctx, statement,
		subscription.ID, subscription.RunID, subscription.Event, null(subscription.Correlation),
		null(subscription.Step), null(subscription.Edge), subscription.ExpiresAt, subscription.CreatedAt)
	return err
}

// MatchSubscriptions implements Store.
//
// A subscription with no correlation matches any event of that name; one with a
// correlation matches only the same value. That asymmetry is deliberate: a
// broadcast subscriber is a legitimate pattern, an accidentally-broadcast
// correlated event is not.
func (s *sqlStore) MatchSubscriptions(ctx context.Context, event, correlation string) ([]*Subscription, error) {
	statement := s.q(fmt.Sprintf(`SELECT id, run_id, event, correlation, step, edge, expires_at, created_at
		FROM %s WHERE event=$1 AND (correlation IS NULL OR correlation=$2) ORDER BY created_at`, s.subs))
	rows, err := s.db.QueryContext(ctx, statement, event, correlation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSubscriptions(rows)
}

// DeleteSubscription implements Store.
func (s *sqlStore) DeleteSubscription(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, s.q(fmt.Sprintf("DELETE FROM %s WHERE id=$1", s.subs)), id)
	return err
}

// DeleteRunSubscriptions implements Store.
func (s *sqlStore) DeleteRunSubscriptions(ctx context.Context, runID string) error {
	_, err := s.db.ExecContext(ctx, s.q(fmt.Sprintf("DELETE FROM %s WHERE run_id=$1", s.subs)), runID)
	return err
}

// ListSubscriptions implements Store.
func (s *sqlStore) ListSubscriptions(ctx context.Context, runID string) ([]*Subscription, error) {
	statement := s.q(fmt.Sprintf(`SELECT id, run_id, event, correlation, step, edge, expires_at, created_at
		FROM %s WHERE run_id=$1 ORDER BY created_at`, s.subs))
	rows, err := s.db.QueryContext(ctx, statement, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSubscriptions(rows)
}

func scanSubscriptions(rows *sql.Rows) ([]*Subscription, error) {
	var out []*Subscription
	for rows.Next() {
		var (
			subscription            Subscription
			correlation, step, edge sql.NullString
			expiresAt               sql.NullTime
		)
		if err := rows.Scan(&subscription.ID, &subscription.RunID, &subscription.Event,
			&correlation, &step, &edge, &expiresAt, &subscription.CreatedAt); err != nil {
			return nil, err
		}
		subscription.Correlation, subscription.Step, subscription.Edge = correlation.String, step.String, edge.String
		subscription.ExpiresAt = nullableTime(expiresAt)
		out = append(out, &subscription)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Joins
// ---------------------------------------------------------------------------

// SaveJoin implements Store.
func (s *sqlStore) SaveJoin(ctx context.Context, join *Join) error {
	sources, err := json.Marshal(join.Sources)
	if err != nil {
		return err
	}
	results, err := json.Marshal(join.Results)
	if err != nil {
		return err
	}
	failures, err := json.Marshal(join.Errors)
	if err != nil {
		return err
	}
	join.UpdatedAt = time.Now().UTC()
	statement := fmt.Sprintf(`INSERT INTO %s (run_id, edge, sources, results, errors, emitted, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, s.joins)
	if s.dialect == "mysql" {
		statement += ` ON DUPLICATE KEY UPDATE sources=VALUES(sources), results=VALUES(results),
			errors=VALUES(errors), emitted=VALUES(emitted), updated_at=VALUES(updated_at)`
	} else {
		statement += ` ON CONFLICT (run_id, edge) DO UPDATE SET sources=EXCLUDED.sources, results=EXCLUDED.results,
			errors=EXCLUDED.errors, emitted=EXCLUDED.emitted, updated_at=EXCLUDED.updated_at`
	}
	_, err = s.db.ExecContext(ctx, s.q(statement),
		join.RunID, join.Edge, string(sources), string(results), string(failures), boolInt(join.Emitted), join.UpdatedAt)
	return err
}

// GetJoin implements Store, returning nil when the join has not started.
func (s *sqlStore) GetJoin(ctx context.Context, runID, edge string) (*Join, error) {
	statement := s.q(fmt.Sprintf(
		"SELECT run_id, edge, sources, results, errors, emitted, updated_at FROM %s WHERE run_id=$1 AND edge=$2", s.joins))
	var (
		join                       Join
		sources, results, failures sql.NullString
		emitted                    int
	)
	switch err := s.db.QueryRowContext(ctx, statement, runID, edge).Scan(
		&join.RunID, &join.Edge, &sources, &results, &failures, &emitted, &join.UpdatedAt); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, err
	}
	join.Emitted = emitted != 0
	if sources.Valid {
		_ = json.Unmarshal([]byte(sources.String), &join.Sources)
	}
	if results.Valid {
		_ = json.Unmarshal([]byte(results.String), &join.Results)
	}
	if failures.Valid {
		_ = json.Unmarshal([]byte(failures.String), &join.Errors)
	}
	return &join, nil
}

// ---------------------------------------------------------------------------
// Leases
// ---------------------------------------------------------------------------

// AcquireLease implements Store.
//
// One statement, no read-then-write window: the upsert only takes the row when
// the existing lease has expired or is already ours.
func (s *sqlStore) AcquireLease(ctx context.Context, runID, owner string, ttl time.Duration) (bool, error) {
	now := time.Now().UTC()
	expires := now.Add(ttl)
	statement := fmt.Sprintf(`INSERT INTO %s (run_id, owner, expires_at) VALUES ($1,$2,$3)`, s.leases)
	if s.dialect == "mysql" {
		statement += ` ON DUPLICATE KEY UPDATE owner=IF(expires_at <= VALUES(expires_at) - INTERVAL 0 SECOND AND expires_at <= NOW(6), VALUES(owner), owner),
			expires_at=IF(owner=VALUES(owner) OR expires_at <= NOW(6), VALUES(expires_at), expires_at)`
	} else {
		// Strictly exclusive, including against the same owner: one replica
		// advancing a run from two goroutines is the interleaving this prevents.
		statement += fmt.Sprintf(` ON CONFLICT (run_id) DO UPDATE SET owner=EXCLUDED.owner, expires_at=EXCLUDED.expires_at
			WHERE %s.expires_at <= $4`, s.leases)
	}

	if s.dialect == "mysql" {
		// MySQL's upsert cannot express the conditional take cleanly, so it uses a
		// conditional UPDATE followed by an INSERT-if-absent. Both are single
		// statements, so the pair is still race-free: whichever replica's UPDATE
		// matches wins, and a failed INSERT means somebody else got there first.
		update := s.q(fmt.Sprintf(
			"UPDATE %s SET owner=$1, expires_at=$2 WHERE run_id=$3 AND expires_at <= $4", s.leases))
		result, err := s.db.ExecContext(ctx, update, owner, expires, runID, now)
		if err != nil {
			return false, err
		}
		if affected, _ := result.RowsAffected(); affected > 0 {
			return true, nil
		}
		insert := s.q(fmt.Sprintf("INSERT INTO %s (run_id, owner, expires_at) VALUES ($1,$2,$3)", s.leases))
		if _, err := s.db.ExecContext(ctx, insert, runID, owner, expires); err != nil {
			// A duplicate-key error means another replica inserted first, which is
			// a lost race rather than a failure.
			return false, nil
		}
		return true, nil
	}

	result, err := s.db.ExecContext(ctx, s.q(statement), runID, owner, expires, now)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

// RefreshLease implements Store.
func (s *sqlStore) RefreshLease(ctx context.Context, runID, owner string, ttl time.Duration) (bool, error) {
	statement := s.q(fmt.Sprintf("UPDATE %s SET expires_at=$1 WHERE run_id=$2 AND owner=$3", s.leases))
	result, err := s.db.ExecContext(ctx, statement, time.Now().UTC().Add(ttl), runID, owner)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected > 0, err
}

// ReleaseLease implements Store.
func (s *sqlStore) ReleaseLease(ctx context.Context, runID, owner string) error {
	statement := s.q(fmt.Sprintf("DELETE FROM %s WHERE run_id=$1 AND owner=$2", s.leases))
	_, err := s.db.ExecContext(ctx, statement, runID, owner)
	return err
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

const taskColumns = `id, run_id, process, step, status, title, instructions, assignee, role, queue,
	skills, forbid_principals, actions, form_schema, priority, tenant_id, data,
	claimed_by, claimed_at, completed_by, completed_at, action, result,
	due_at, reminder_at, created_at, updated_at, revision`

// SaveTask implements Store.
func (s *sqlStore) SaveTask(ctx context.Context, task *Task) error {
	skills, _ := json.Marshal(task.Skills)
	forbid, _ := json.Marshal(task.ForbidPrincipals)
	actions, _ := json.Marshal(task.Actions)
	task.UpdatedAt = time.Now().UTC()

	if task.Revision == 0 {
		task.Revision = 1
		statement := s.q(fmt.Sprintf(`INSERT INTO %s (%s) VALUES
			($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28)`,
			s.tasks, taskColumns))
		_, err := s.db.ExecContext(ctx, statement,
			task.ID, task.RunID, task.Process, task.Step, string(task.Status),
			null(task.Title), null(task.Instructions), null(task.Assignee), null(task.Role), null(task.Queue),
			string(skills), string(forbid), string(actions), null(task.FormSchema), task.Priority, null(task.TenantID), blob(task.Data),
			null(task.ClaimedBy), task.ClaimedAt, null(task.CompletedBy), task.CompletedAt, null(task.Action), blob(task.Result),
			task.DueAt, task.ReminderAt, task.CreatedAt, task.UpdatedAt, task.Revision)
		return err
	}

	next := task.Revision + 1
	statement := s.q(fmt.Sprintf(`UPDATE %s SET status=$1, title=$2, instructions=$3, assignee=$4, role=$5, queue=$6,
		skills=$7, forbid_principals=$8, actions=$9, form_schema=$10, priority=$11, tenant_id=$12, data=$13,
		claimed_by=$14, claimed_at=$15, completed_by=$16, completed_at=$17, action=$18, result=$19,
		due_at=$20, reminder_at=$21, updated_at=$22, revision=$23 WHERE id=$24 AND revision=$25`, s.tasks))
	result, err := s.db.ExecContext(ctx, statement,
		string(task.Status), null(task.Title), null(task.Instructions), null(task.Assignee), null(task.Role), null(task.Queue),
		string(skills), string(forbid), string(actions), null(task.FormSchema), task.Priority, null(task.TenantID), blob(task.Data),
		null(task.ClaimedBy), task.ClaimedAt, null(task.CompletedBy), task.CompletedAt, null(task.Action), blob(task.Result),
		task.DueAt, task.ReminderAt, task.UpdatedAt, next, task.ID, task.Revision)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		// Two people claiming the same task is exactly the race this catches.
		return ErrRevisionConflict
	}
	task.Revision = next
	return nil
}

// GetTask implements Store.
func (s *sqlStore) GetTask(ctx context.Context, id string) (*Task, error) {
	statement := s.q(fmt.Sprintf("SELECT %s FROM %s WHERE id=$1", taskColumns, s.tasks))
	task, err := scanTask(s.db.QueryRowContext(ctx, statement, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrTaskNotFound
	}
	return task, err
}

// ListTasks implements Store.
func (s *sqlStore) ListTasks(ctx context.Context, filter TaskFilter) ([]*Task, error) {
	var (
		conditions []string
		args       []any
	)
	add := func(format string, value any) {
		args = append(args, value)
		conditions = append(conditions, fmt.Sprintf(format, len(args)))
	}
	if filter.Process != "" {
		add("process=$%d", filter.Process)
	}
	if filter.Status != "" {
		add("status=$%d", string(filter.Status))
	} else {
		conditions = append(conditions, "status IN ('open','claimed','escalated')")
	}
	if filter.RunID != "" {
		add("run_id=$%d", filter.RunID)
	}
	if filter.TenantID != "" {
		add("tenant_id=$%d", filter.TenantID)
	}
	if filter.Queue != "" {
		add("queue=$%d", filter.Queue)
	}
	if filter.Overdue {
		add("(due_at IS NOT NULL AND due_at <= $%d)", time.Now().UTC())
	}
	// A work list is "mine, plus my roles' queues". Expressing it as one OR group
	// keeps it a single indexed query rather than three round trips.
	if filter.Assignee != "" || len(filter.Roles) > 0 {
		var group []string
		if filter.Assignee != "" {
			args = append(args, filter.Assignee)
			group = append(group, fmt.Sprintf("assignee=$%d", len(args)))
			group = append(group, fmt.Sprintf("claimed_by=$%d", len(args)))
		}
		for _, role := range filter.Roles {
			args = append(args, role)
			group = append(group, fmt.Sprintf("(role=$%d AND (assignee IS NULL OR assignee=''))", len(args)))
		}
		if len(group) > 0 {
			conditions = append(conditions, "("+strings.Join(group, " OR ")+")")
		}
	}

	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	args = append(args, limit, filter.Offset)
	statement := s.q(fmt.Sprintf("SELECT %s FROM %s%s ORDER BY priority DESC, created_at LIMIT $%d OFFSET $%d",
		taskColumns, s.tasks, whereClause(conditions), len(args)-1, len(args)))

	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, task)
	}
	return out, rows.Err()
}

// CountOpenTasks implements Store.
func (s *sqlStore) CountOpenTasks(ctx context.Context, assignee string) (int, error) {
	statement := s.q(fmt.Sprintf(
		"SELECT COUNT(*) FROM %s WHERE status IN ('open','claimed','escalated') AND (assignee=$1 OR claimed_by=$1)", s.tasks))
	var count int
	err := s.db.QueryRowContext(ctx, statement, assignee).Scan(&count)
	return count, err
}

// DueTasks implements Store.
func (s *sqlStore) DueTasks(ctx context.Context, now time.Time, limit int) ([]*Task, error) {
	if limit <= 0 {
		limit = 50
	}
	statement := s.q(fmt.Sprintf(`SELECT %s FROM %s
		WHERE status IN ('open','claimed') AND ((due_at IS NOT NULL AND due_at <= $1) OR (reminder_at IS NOT NULL AND reminder_at <= $1))
		ORDER BY due_at LIMIT %d`, taskColumns, s.tasks, limit))
	rows, err := s.db.QueryContext(ctx, statement, now.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, task)
	}
	return out, rows.Err()
}

func scanTask(row rowScanner) (*Task, error) {
	var (
		task                                    Task
		status                                  string
		title, instructions                     sql.NullString
		assignee, role, queue                   sql.NullString
		skills, forbid, actions, formSchema     sql.NullString
		tenant, claimedBy, completedBy, action  sql.NullString
		data, result                            []byte
		claimedAt, completedAt, dueAt, reminder sql.NullTime
	)
	if err := row.Scan(
		&task.ID, &task.RunID, &task.Process, &task.Step, &status,
		&title, &instructions, &assignee, &role, &queue,
		&skills, &forbid, &actions, &formSchema, &task.Priority, &tenant, &data,
		&claimedBy, &claimedAt, &completedBy, &completedAt, &action, &result,
		&dueAt, &reminder, &task.CreatedAt, &task.UpdatedAt, &task.Revision,
	); err != nil {
		return nil, err
	}
	task.Status = TaskStatus(status)
	task.Title, task.Instructions = title.String, instructions.String
	task.Assignee, task.Role, task.Queue = assignee.String, role.String, queue.String
	task.FormSchema, task.TenantID = formSchema.String, tenant.String
	task.ClaimedBy, task.CompletedBy, task.Action = claimedBy.String, completedBy.String, action.String
	task.Data, task.Result = json.RawMessage(data), json.RawMessage(result)
	task.ClaimedAt = nullableTime(claimedAt)
	task.CompletedAt = nullableTime(completedAt)
	task.DueAt = nullableTime(dueAt)
	task.ReminderAt = nullableTime(reminder)
	if skills.Valid {
		_ = json.Unmarshal([]byte(skills.String), &task.Skills)
	}
	if forbid.Valid {
		_ = json.Unmarshal([]byte(forbid.String), &task.ForbidPrincipals)
	}
	if actions.Valid {
		_ = json.Unmarshal([]byte(actions.String), &task.Actions)
	}
	return &task, nil
}

// ---------------------------------------------------------------------------
// Events and retention
// ---------------------------------------------------------------------------

// RecordEvent implements Store.
func (s *sqlStore) RecordEvent(ctx context.Context, event *Event) error {
	statement := s.q(fmt.Sprintf(`INSERT INTO %s (id, name, correlation, payload, received_at)
		VALUES ($1,$2,$3,$4,$5)`, s.events))
	_, err := s.db.ExecContext(ctx, statement,
		event.ID, event.Name, null(event.Correlation), blob(event.Payload), event.ReceivedAt)
	return err
}

// PurgeRuns implements Store. It deletes each run's dependents before the run
// itself, so an interrupted purge never leaves orphan rows pointing at a run that
// no longer exists.
func (s *sqlStore) PurgeRuns(ctx context.Context, process string, before time.Time, limit int) (int, error) {
	if limit <= 0 {
		limit = 200
	}
	args := []any{before.UTC()}
	condition := "completed_at IS NOT NULL AND completed_at < $1 AND status IN ('completed','failed','cancelled')"
	if process != "" {
		args = append(args, process)
		condition += fmt.Sprintf(" AND process=$%d", len(args))
	}
	selectStatement := s.q(fmt.Sprintf("SELECT id FROM %s WHERE %s ORDER BY completed_at LIMIT %d", s.runs, condition, limit))
	rows, err := s.db.QueryContext(ctx, selectStatement, args...)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	purged := 0
	for _, id := range ids {
		for _, table := range []string{s.steps, s.timers, s.subs, s.joins, s.leases, s.tasks, s.sequence} {
			if _, err := s.db.ExecContext(ctx, s.q(fmt.Sprintf("DELETE FROM %s WHERE run_id=$1", table)), id); err != nil {
				return purged, err
			}
		}
		if _, err := s.db.ExecContext(ctx, s.q(fmt.Sprintf("DELETE FROM %s WHERE id=$1", s.runs)), id); err != nil {
			return purged, err
		}
		purged++
	}
	return purged, nil
}

// Close implements Store. The pool belongs to whoever opened it.
func (s *sqlStore) Close() error { return nil }

// ---------------------------------------------------------------------------
// Dialect helpers
// ---------------------------------------------------------------------------

// rebind converts $1-style placeholders to the dialect's own form.
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

func textColumn(dialect string) string {
	if dialect == "mysql" {
		// MySQL cannot index an unbounded TEXT column without a prefix length, and
		// every column typed here is one we index or key on. VARCHAR(500) allows
		// long idempotency keys and process names while remaining indexable.
		return "VARCHAR(500)"
	}
	return "TEXT"
}

func timestampColumn(dialect string) string {
	switch dialect {
	case "postgres":
		return "TIMESTAMPTZ"
	case "mysql":
		return "DATETIME(6)"
	default:
		return "TIMESTAMP"
	}
}

// lockSuffix returns the row-locking clause that lets replicas claim different
// rows without blocking each other. SQLite has neither the syntax nor concurrent
// writers, so it gets nothing.
func lockSuffix(dialect string) string {
	switch dialect {
	case "postgres", "mysql":
		return " FOR UPDATE SKIP LOCKED"
	default:
		return ""
	}
}

func whereClause(conditions []string) string {
	if len(conditions) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(conditions, " AND ")
}

func null(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func blob(value json.RawMessage) any {
	if len(value) == 0 {
		return nil
	}
	return string(value)
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func nullableTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil

	}
	utc := value.Time.UTC()
	return &utc
}
