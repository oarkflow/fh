package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
)

var ErrConflict = errors.New("cluster: idempotency key already used for different payload")

// Outbox writes an event inside the SAME transaction as application business
// effects. PostgreSQL enforces ownership with a row lock and fencing token.
// A separate delivery worker reads the outbox; it must not mark a consumer
// operation processed until the downstream transaction commits.
type Outbox struct{}

func (Outbox) PublishTx(ctx context.Context, tx *sql.Tx, lease Lease, operationID string, payload []byte) (uint64, error) {
	if tx == nil || lease.Identity == "" || lease.Owner == "" || lease.Fence == 0 || len(operationID) == 0 || len(operationID) > 256 || len(payload) > 1<<20 {
		return 0, ErrInvalid
	}
	var held int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM mira_leases WHERE identity=$1
 AND owner=$2 AND fence=$3 AND expires_at>now() FOR SHARE`, lease.Identity, lease.Owner, lease.Fence).Scan(&held)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrFenced
	}
	if err != nil {
		return 0, err
	}
	sum := sha256.Sum256(payload)
	var cursor uint64
	err = tx.QueryRowContext(ctx, `INSERT INTO mira_outbox(identity,operation_id,payload,payload_hash)
 VALUES($1,$2,$3,$4) ON CONFLICT(identity,operation_id) DO NOTHING
 RETURNING cursor`, lease.Identity, operationID, payload, sum[:]).Scan(&cursor)
	if err == nil {
		return cursor, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	var original []byte
	err = tx.QueryRowContext(ctx, `SELECT cursor,payload_hash FROM mira_outbox
 WHERE identity=$1 AND operation_id=$2`, lease.Identity, operationID).Scan(&cursor, &original)
	if err != nil {
		return 0, err
	}
	if !bytes.Equal(original, sum[:]) {
		return 0, ErrConflict
	}
	return cursor, nil
}
func (Outbox) ReadAfter(ctx context.Context, db *sql.DB, identity string, cursor uint64, limit int) ([]OutboxRecord, error) {
	if db == nil || identity == "" || limit < 1 || limit > 1000 {
		return nil, ErrInvalid
	}
	rows, err := db.QueryContext(ctx, `SELECT cursor,operation_id,payload FROM mira_outbox
 WHERE identity=$1 AND cursor>$2 ORDER BY cursor ASC LIMIT $3`, identity, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]OutboxRecord, 0, min(limit, 32))
	for rows.Next() {
		var r OutboxRecord
		r.Identity = identity
		if err := rows.Scan(&r.Cursor, &r.OperationID, &r.Payload); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type OutboxRecord struct {
	Cursor                uint64
	Identity, OperationID string
	Payload               []byte
}
