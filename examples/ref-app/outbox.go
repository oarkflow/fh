package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"time"
)

// The outbox deliverer.
//
// Every notification this application sends leaves through here, and it can only
// send one that is already committed: the row was written inside the same
// transaction as the order. That gives at-least-once delivery with no lost
// notifications and no notifications for orders that never existed.
//
// Two things drive it:
//
//   - REF's DurableDelivery effect, which tries the send immediately after the
//     commit, so the common case is fast;
//   - the poller, which sweeps rows nobody delivered — because the process crashed,
//     the SMTP server was down, or the immediate attempt failed.
//
// Delivery is at-least-once, not exactly-once. A send that succeeds and then fails
// to mark the row will be retried, so a recipient can see a duplicate; that is the
// honest trade and the alternative (marking first) loses notifications instead.
const jobDeliverOutbox = "outbox.deliver"

type OutboxDeliverer struct {
	db       *sql.DB
	notifier Notifier
	log      *slog.Logger
	metrics  *Metrics

	maxAttempts int
	interval    time.Duration
	batch       int
}

func NewOutboxDeliverer(db *sql.DB, notifier Notifier, metrics *Metrics, log *slog.Logger, cfg Config) *OutboxDeliverer {
	return &OutboxDeliverer{
		db:          db,
		notifier:    notifier,
		log:         log,
		metrics:     metrics,
		maxAttempts: cfg.OutboxAttempts,
		interval:    cfg.OutboxInterval,
		batch:       20,
	}
}

// DeliverRow sends one known row. It is what the DurableDelivery effect calls, and
// it claims the row first so the poller cannot send it at the same moment.
func (d *OutboxDeliverer) DeliverRow(ctx context.Context, id int64, notification Notification) error {
	claimed, err := d.claim(ctx, id)
	if err != nil {
		return err
	}
	if !claimed {
		// Somebody else has it, or it is already delivered. Not an error.
		return nil
	}
	return d.deliver(ctx, id, notification)
}

// Run polls until the context is cancelled. Every replica may run it: the claim is
// an UPDATE with a status predicate, so two pollers cannot take the same row.
func (d *OutboxDeliverer) Run(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if sent, err := d.Sweep(ctx); err != nil && !errors.Is(err, context.Canceled) {
				d.log.Warn("outbox sweep failed", slog.String("error", err.Error()))
			} else if sent > 0 {
				d.log.Debug("outbox swept", slog.Int("delivered", sent))
			}
		}
	}
}

// Sweep delivers one batch of due rows and reports how many it sent.
func (d *OutboxDeliverer) Sweep(ctx context.Context) (int, error) {
	// FOR UPDATE SKIP LOCKED is what makes this safe to run on every replica: each
	// poller takes rows nobody else is holding instead of contending for the same
	// ones.
	rows, err := d.db.QueryContext(ctx,
		`WITH due AS (
			SELECT id FROM outbox
			 WHERE status='pending' AND visible_at<=NOW()
			 ORDER BY visible_at
			 LIMIT $1
			 FOR UPDATE SKIP LOCKED
		 )
		 UPDATE outbox SET status='sending', attempts=attempts+1
		  WHERE id IN (SELECT id FROM due)
		 RETURNING id, payload, attempts`, d.batch)
	if err != nil {
		return 0, err
	}
	type claimedRow struct {
		id       int64
		payload  []byte
		attempts int
	}
	var batch []claimedRow
	for rows.Next() {
		var row claimedRow
		if err := rows.Scan(&row.id, &row.payload, &row.attempts); err != nil {
			_ = rows.Close()
			return 0, err
		}
		batch = append(batch, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	_ = rows.Close()

	delivered := 0
	for _, row := range batch {
		var notification Notification
		if err := json.Unmarshal(row.payload, &notification); err != nil {
			// An unreadable payload will never become readable. Dead-letter it now
			// rather than retrying it eight times first.
			d.fail(ctx, row.id, d.maxAttempts, "payload is not valid JSON")
			continue
		}
		if err := d.deliver(ctx, row.id, notification); err == nil {
			delivered++
		}
	}
	return delivered, nil
}

// claim moves a specific row to 'sending' if it is still pending.
func (d *OutboxDeliverer) claim(ctx context.Context, id int64) (bool, error) {
	result, err := d.db.ExecContext(ctx,
		`UPDATE outbox SET status='sending', attempts=attempts+1
		  WHERE id=$1 AND status='pending'`, id)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

func (d *OutboxDeliverer) deliver(ctx context.Context, id int64, notification Notification) error {
	sendCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	if err := d.notifier.Notify(sendCtx, notification); err != nil {
		var attempts int
		if scanErr := d.db.QueryRowContext(ctx, `SELECT attempts FROM outbox WHERE id=$1`, id).Scan(&attempts); scanErr != nil {
			attempts = d.maxAttempts
		}
		d.fail(ctx, id, attempts, err.Error())
		d.metrics.AddBusiness("notifications.failed", 1)
		return err
	}
	if _, err := d.db.ExecContext(ctx,
		`UPDATE outbox SET status='delivered', delivered_at=NOW(), last_error=NULL WHERE id=$1`, id); err != nil {
		// The notification went out but the row still says 'sending'. The sweeper
		// will not pick it up (it only takes 'pending'), so this is logged loudly:
		// it is the one state a human may need to reconcile.
		d.log.Error("delivered a notification but could not mark it",
			slog.Int64("outbox_id", id), slog.String("error", err.Error()))
		return err
	}
	d.metrics.AddBusiness("notifications.delivered", 1)
	return nil
}

// fail schedules a retry with exponential backoff, or dead-letters the row once it
// has used its attempts. A dead row keeps its last error, because that is the only
// thing an operator has to work from.
func (d *OutboxDeliverer) fail(ctx context.Context, id int64, attempts int, reason string) {
	if attempts >= d.maxAttempts {
		if _, err := d.db.ExecContext(ctx,
			`UPDATE outbox SET status='dead', last_error=$2 WHERE id=$1`, id, reason); err != nil {
			d.log.Error("could not dead-letter an outbox row",
				slog.Int64("outbox_id", id), slog.String("error", err.Error()))
		}
		d.log.Warn("notification dead-lettered after exhausting its attempts",
			slog.Int64("outbox_id", id), slog.Int("attempts", attempts), slog.String("reason", reason))
		d.metrics.AddBusiness("notifications.dead", 1)
		return
	}
	// 2^attempts seconds, capped: a broken endpoint is retried less often, not more.
	delay := time.Duration(math.Min(math.Pow(2, float64(attempts)), 300)) * time.Second
	if _, err := d.db.ExecContext(ctx,
		`UPDATE outbox SET status='pending', visible_at=NOW()+$2::interval, last_error=$3 WHERE id=$1`,
		id, delay.String(), reason); err != nil {
		d.log.Error("could not reschedule an outbox row",
			slog.Int64("outbox_id", id), slog.String("error", err.Error()))
	}
}

// Stats reports the outbox by status, for /healthz and for anybody wondering whether
// notifications are actually going out.
func (d *OutboxDeliverer) Stats(ctx context.Context) (map[string]int, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM outbox GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]int{}
	for rows.Next() {
		var (
			status string
			count  int
		)
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		out[status] = count
	}
	return out, rows.Err()
}
