package cluster

// Transactionally idempotent PostgreSQL consumer group progress. This is a
// database-level exactly-once-effect primitive only when the business effect
// itself is performed in the *same transaction* as ConsumeNext. It cannot
// make external HTTP calls or deliveries exactly once.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
)

type ConsumeEffect func(context.Context, *sql.Tx, OutboxRecord) error

type ConsumerGroup struct{ DB *sql.DB }

// Next returns the next committed outbox record after the group's cursor.
// A fenced owner must process records serially for the (identity,group) pair.
// It is legal for global outbox cursors to have gaps for other identities.
func (c ConsumerGroup) Next(ctx context.Context, identity, group string) (OutboxRecord, error) {
	if c.DB == nil || identity == "" || group == "" || len(group) > 256 {
		return OutboxRecord{}, ErrInvalid
	}
	var cursor uint64
	err := c.DB.QueryRowContext(ctx, `SELECT COALESCE((SELECT cursor FROM mira_consumer_progress WHERE identity=$1 AND group_id=$2),0)`, identity, group).Scan(&cursor)
	if err != nil {
		return OutboxRecord{}, err
	}
	var out OutboxRecord
	out.Identity = identity
	err = c.DB.QueryRowContext(ctx, `SELECT cursor,operation_id,payload FROM mira_outbox WHERE identity=$1 AND cursor>$2 ORDER BY cursor LIMIT 1`, identity, cursor).Scan(&out.Cursor, &out.OperationID, &out.Payload)
	return out, err
}

// ConsumeNext owns the transaction, pins the lease's fencing token with a
// row lock, serializes the group's cursor and applies the business effect in
// the same SERIALIZABLE transaction. On retry, an existing operation ID with
// the same payload hash is skipped; changed payloads fail with ErrConflict.
// Returns applied=false for a duplicate/previously processed event.
func (c ConsumerGroup) ConsumeNext(ctx context.Context, lease Lease, group string, record OutboxRecord, apply ConsumeEffect) (applied bool, err error) {
	if c.DB == nil || lease.Identity == "" || lease.Fence == 0 || lease.Owner == "" || len(group) == 0 || len(group) > 256 || record.Identity != lease.Identity || record.OperationID == "" || record.Cursor == 0 || len(record.Payload) > 1<<20 || apply == nil {
		return false, ErrInvalid
	}
	tx, err := c.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var held int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM mira_leases WHERE identity=$1 AND owner=$2 AND fence=$3 AND expires_at>now() FOR SHARE`, lease.Identity, lease.Owner, lease.Fence).Scan(&held)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrFenced
	}
	if err != nil {
		return false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO mira_consumer_progress(identity,group_id,cursor) VALUES($1,$2,0) ON CONFLICT DO NOTHING`, lease.Identity, group)
	if err != nil {
		return false, err
	}
	var cursor uint64
	if err = tx.QueryRowContext(ctx, `SELECT cursor FROM mira_consumer_progress WHERE identity=$1 AND group_id=$2 FOR UPDATE`, lease.Identity, group).Scan(&cursor); err != nil {
		return false, err
	}
	if record.Cursor <= cursor {
		return false, tx.Commit()
	}
	// Never trust caller-supplied record bytes without checking immutable
	// outbox ownership, operation ID and payload hash inside the transaction.
	var storedHash []byte
	err = tx.QueryRowContext(ctx, `SELECT payload_hash FROM mira_outbox WHERE identity=$1 AND cursor=$2 AND operation_id=$3`, lease.Identity, record.Cursor, record.OperationID).Scan(&storedHash)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrInvalid
	}
	if err != nil {
		return false, err
	}
	hash := sha256.Sum256(record.Payload)
	if !bytes.Equal(storedHash, hash[:]) {
		return false, ErrConflict
	}
	// Enforce in-order application: do not allow an arbitrary higher cursor
	// to leap over an unprocessed event for this identity.
	var earliest uint64
	err = tx.QueryRowContext(ctx, `SELECT cursor FROM mira_outbox WHERE identity=$1 AND cursor>$2 ORDER BY cursor LIMIT 1`, lease.Identity, cursor).Scan(&earliest)
	if err != nil {
		return false, err
	}
	if earliest != record.Cursor {
		return false, ErrConflict
	}
	var insertedID string
	err = tx.QueryRowContext(ctx, `INSERT INTO mira_consumer_dedupe(identity,group_id,operation_id,payload_hash) VALUES($1,$2,$3,$4)
 ON CONFLICT DO NOTHING RETURNING operation_id`, lease.Identity, group, record.OperationID, hash[:]).Scan(&insertedID)
	if errors.Is(err, sql.ErrNoRows) {
		var priorHash []byte
		if err = tx.QueryRowContext(ctx, `SELECT payload_hash FROM mira_consumer_dedupe WHERE identity=$1 AND group_id=$2 AND operation_id=$3`, lease.Identity, group, record.OperationID).Scan(&priorHash); err != nil {
			return false, err
		}
		if !bytes.Equal(priorHash, hash[:]) {
			return false, ErrConflict
		}
	} else if err != nil {
		return false, err
	} else {
		if err = apply(ctx, tx, record); err != nil {
			return false, err
		}
		applied = true
	}
	if _, err = tx.ExecContext(ctx, `UPDATE mira_consumer_progress SET cursor=$3 WHERE identity=$1 AND group_id=$2`, lease.Identity, group, record.Cursor); err != nil {
		return false, err
	}
	err = tx.Commit()
	if err != nil {
		return false, err
	}
	return applied, nil
}
