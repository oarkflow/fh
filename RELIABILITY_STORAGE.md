# fh Reliability Storage Interfaces

fh's reliability runtime is storage-agnostic. File/directory persistence remains the default, but production applications can plug in DBMS-backed stores.

## Interfaces

```go
type RequestJournalStore interface {
    Append(fh.RequestJournalEntry) error
    Close() error
}

type IdempotencyRepository interface {
    Begin(key, reqHash, method, path string) (fh.IdempotencyDecision, *fh.IdempotencyRecord, error)
    Complete(key, reqHash string, status int, contentType string, headers map[string][]string, response []byte) error
    Close() error
}

type QueueStorage interface {
    Enqueue(context.Context, *fh.QueueJob) error
    Claim(context.Context, time.Time) (*fh.QueueJob, error)
    Complete(context.Context, *fh.QueueJob) error
    Retry(context.Context, *fh.QueueJob, error, time.Duration) error
    Fail(context.Context, *fh.QueueJob, error) error
    Recover(context.Context) error
    Stats(context.Context) (fh.QueueStats, error)
    Close() error
}
```

## Default backends

When no custom store is supplied, fh uses:

- `OpenRequestJournal(.fh-reliability/request-journal.jsonl)`
- `OpenIdempotencyStore(.fh-reliability/idempotency.jsonl)`
- `OpenFileQueueStorage(.fh-reliability/queue)`

Queue files are stored in:

```text
pending/
processing/
done/
failed/
events.jsonl
```

## Custom DBMS wiring

```go
app := fh.New(fh.Config{
    Reliability: fh.ReliabilityConfig{
        Enabled:                true,
        JournalEnabled:         true,
        IdempotencyEnabled:     true,
        QueueEnabled:           true,
        JournalStore:           postgresJournal,
        IdempotencyRepository:  postgresIdempotency,
        QueueStorage:           postgresQueue,
        QueueWorkers:           8,
        QueuePollInterval:      100 * time.Millisecond,
    },
})
```

## Production rules for DB implementations

### Request journal

Append must be immutable and durable. Do not update previous rows. Use this for audit and incident investigation.

### Idempotency

`Begin` must be atomic per key. It must never allow two concurrent requests with the same key to both return `fh.IdempotencyNew`.

Return values:

- `fh.IdempotencyNew`: caller should process request.
- `fh.IdempotencyReplay`: caller should replay stored response.
- `fh.IdempotencyConflict`: same key was reused for a different request hash.
- `fh.IdempotencyProcessing`: same request is already in-flight.

### Queue

`Claim` must atomically move a visible pending job into processing state. This is the key operation for safe multi-worker and multi-instance deployments.

A PostgreSQL implementation should use a transaction and `FOR UPDATE SKIP LOCKED`, or an atomic `UPDATE ... RETURNING` claim query.

A SQLite implementation should use a write transaction and ensure only one process can claim a row at a time.
