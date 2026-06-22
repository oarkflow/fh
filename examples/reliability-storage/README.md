# Reliability Storage Interfaces Example

This example shows how to replace fh's default file/directory reliability storage with custom implementations.

It wires custom implementations for:

- `fh.RequestJournalStore`
- `fh.IdempotencyRepository`
- `fh.QueueStorage`

The example uses in-memory implementations so it builds with the Go standard library only. In production, implement the same interfaces using SQLite, PostgreSQL, MySQL, Redis, or any other durable backend.

## Run

```bash
go run .
```

## Test idempotent async request

```bash
curl -i -X POST http://localhost:3000/email \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: email-dbms-001' \
  -d '{"to":"user@example.com","subject":"Hello","message":"stored through custom interfaces"}'

curl -i -X POST http://localhost:3000/email \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: email-dbms-001' \
  -d '{"to":"user@example.com","subject":"Hello","message":"stored through custom interfaces"}'

curl http://localhost:3000/stats
```

The second request replays the original response because the idempotency repository returns `fh.IdempotencyReplay`.

## DBMS implementation notes

### Request journal table

```sql
CREATE TABLE request_journal (
  id BIGSERIAL PRIMARY KEY,
  request_id TEXT NOT NULL,
  event TEXT NOT NULL,
  method TEXT,
  path TEXT,
  status INTEGER,
  body_hash TEXT,
  remote_ip TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`Append` should insert one immutable row per lifecycle event.

### Idempotency table

```sql
CREATE TABLE idempotency_keys (
  key TEXT PRIMARY KEY,
  request_hash TEXT NOT NULL,
  method TEXT,
  path TEXT,
  state TEXT NOT NULL,
  status_code INTEGER,
  content_type TEXT,
  headers JSONB,
  response BYTEA,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);
```

`Begin` must be atomic:

1. Try to insert `processing`.
2. If insert succeeds, return `fh.IdempotencyNew`.
3. If the key exists and hash differs, return `fh.IdempotencyConflict`.
4. If state is `completed`, return `fh.IdempotencyReplay`.
5. Otherwise return `fh.IdempotencyProcessing`.

### Queue table

```sql
CREATE TABLE queue_jobs (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  payload JSONB,
  headers JSONB,
  state TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL,
  visible_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  last_error TEXT
);

CREATE INDEX queue_jobs_claim_idx ON queue_jobs (state, visible_at, created_at);
```

`Claim` must atomically move one visible job from `pending` to `processing`. PostgreSQL can use `FOR UPDATE SKIP LOCKED`; SQLite can use a transaction with an `UPDATE ... WHERE id = (...) RETURNING ...` style pattern depending on SQLite version.
