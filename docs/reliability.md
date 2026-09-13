# Reliability Layer

fh includes an optional, built-in reliability layer for production-grade request processing: request journaling, idempotency support, and a durable async job queue with outbox/inbox patterns and dead-letter queue.

## Enabling Reliability

```go
app := fh.NewWithConfig(fh.Config{
    Reliability: fh.ReliabilityConfig{
        Enabled:            true,
        JournalEnabled:     true,
        IdempotencyEnabled: true,
        QueueEnabled:       true,
        DataDir:            "./.fh-data",
        QueueWorkers:       5,
    },
})
```

---

## Request Journal

Persists request lifecycle events to a JSONL file for audit and tracing.

```go
// When enabled, each request is journaled:
// 1. On receipt: { "event": "received", "id": "...", "method": "POST", "path": "/orders", ... }
// 2. On completion: { "event": "completed", "id": "...", "status": 200, ... }
```

The default journal file is `{DataDir}/request-journal.jsonl`.

### Custom Journal Store

```go
type RequestJournalStore interface {
    Append(fh.RequestJournalEntry) error
    Close() error
}
```

---

## Idempotency

Deduplicates retries of unsafe methods (POST, PUT, PATCH and DELETE) and replays
completed responses. This provides idempotent HTTP retry behavior; it does not
by itself create an exactly-once transaction across an external database and
the idempotency repository.

```go
type CreateOrderRequest struct {
    ProductID string `json:"product_id"`
    Quantity  int    `json:"quantity"`
}

type CreateOrderResponse struct {
    OrderID string `json:"order_id"`
    Status  string `json:"status"`
}

app.Post("/orders", reliability.Endpoint(reliability.EndpointOptions[CreateOrderRequest, CreateOrderResponse]{
    Policy: fh.ReliabilityPolicy{Enabled: true, RequireIdempotency: true},
    Handle: func(ctx context.Context, c fh.Ctx, req CreateOrderRequest) (CreateOrderResponse, error) {
        order, err := createOrder(req)
        return CreateOrderResponse{OrderID: order.ID, Status: "created"}, err
    },
}))
```

### How It Works

1. Client sends `Idempotency-Key: unique-key` header with POST/PUT/PATCH/DELETE
2. On first request: handler executes, response is stored
3. On subsequent requests with same key: stored response is returned (safe replay)
4. In-flight requests return 409 Conflict (configurable status)

### Configuration

```go
Reliability: fh.ReliabilityConfig{
    IdempotencyEnabled:          true,
    IdempotencyHeader:           "Idempotency-Key",     // default
    RequireIdempotencyKey:       true,                   // reject unsafe methods without key
    IdempotencyTTL:              24 * time.Hour,         // keep completed responses
    IdempotencyProcessingStatus: 409,                    // status for in-flight
    IdempotencyReplayHeaderValue: "replayed",            // X-Idempotency-Replayed value
}
```

### Custom Idempotency Repository

```go
type IdempotencyRepository interface {
    Begin(key, requestHash, method, path string) (fh.IdempotencyDecision, *fh.IdempotencyRecord, error)
    Complete(key, requestHash string, status int, contentType string,
        headers map[string][]string, response []byte) error
    Close() error
}
```

---

## Durable Queue

File-backed async job queue with crash recovery, retries, and worker processing.

```go
app.Post("/process", func(c fh.Ctx) error {
    id, err := c.Queue().Enqueue("process-payment", paymentData)
    if err != nil {
        return err
    }
    return c.Status(fh.StatusAccepted).JSON(fh.Map{"job_id": id})
})
```

### Queue Directories

```
{DataDir}/queue/
├── pending/     # Jobs waiting to be processed
├── processing/  # Jobs currently being processed
├── done/        # Completed jobs
└── failed/      # Failed jobs after max retries
```

### Worker Processing

```go
// Register handlers before serving. Workers start with the reliability runtime.
app.Queue().Register("send-email", func(ctx context.Context, job *fh.QueueJob) error {
    return sendEmail(ctx, job.Payload)
})
```

### Job Features

- **Priority:** Jobs can have priority levels
- **Delayed Jobs:** Schedule jobs for future execution
- **Concurrency Keys:** Limit concurrent processing by key
- **Max Attempts:** Configurable retry count with exponential backoff
- **Crash Recovery:** On startup, processing jobs are moved back to pending

### Configuration

```go
Reliability: fh.ReliabilityConfig{
    QueueEnabled:               true,
    QueueDir:                   "./.fh-data/queue",
    QueueWorkers:               5,
    QueueMaxAttempts:           5,
    QueuePollInterval:          1 * time.Second,
    QueueBackoff:               5 * time.Second,
    QueueConcurrencyLimitByKey: true,
}
```

### Custom Queue Storage

```go
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

---

## Outbox / Inbox

Reliable event publishing and webhook deduplication helpers.

### Outbox

```go
// In handler:
outbox := c.ServerOutbox()
id, err := outbox.Publish(c.Context(), fh.OutboxEvent{
    Topic: "order.created",
    Key: order.ID,
    Payload: payload,
})

// Outbox events are queued as "outbox." + Topic.
app.Queue().Register("outbox.order.created", func(ctx context.Context, job *fh.QueueJob) error {
    return publishEvent(ctx, job.Payload)
})
```

### Inbox

```go
inbox := c.ServerInbox()
jobID, err := inbox.Accept(c.Context(), fh.InboxEvent{
    Source: "payments",
    EventID: providerEventID,
    Payload: payload,
}, "payments.webhook")
```

---

## Dead-Letter Queue (DLQ)

Failed jobs that exceed max attempts are moved to the dead-letter queue.

```go
ctx := context.Background()
queue := app.Queue()

// Inspect failed jobs when the configured QueueStorage supports QueueJobLister.
failedJobs, err := queue.ListJobs(ctx, "failed", 100)

// Retry or discard a specific failed job ID.
err = queue.RetryFailed(ctx, jobID)
err = queue.DiscardFailed(ctx, jobID)
```

---

## Request-to-job handoff

`AtomicHandoff` creates a queue job with optional priority, schedule and
concurrency-key fields. Despite its historical name, it does not make an
external application database transaction atomic with the queue write; use a
transactional outbox in that database when that guarantee is required.

```go
import "github.com/oarkflow/fh"

app.Post("/orders", func(c fh.Ctx) error {
    var req CreateOrderRequest
    c.BodyParser(&req)

    id, err := fh.AtomicHandoff(c, "order.fulfill", req, fh.QueueJob{
        Priority: 10,
        ConcurrencyKey: "customer:" + req.CustomerID,
    })
    if err != nil { return err }
    return c.Status(fh.StatusAccepted).JSON(fh.Map{"job_id": id})
})
```

For raw payload bytes and the complete job option set, use `AtomicJob`:

```go
result, err := fh.AtomicJob(c, fh.AtomicJobOptions{
    Type: "order.fulfill",
    Body: payload,
    Priority: fh.PriorityHigh,
    ConcurrencyKey: "customer:" + customerID,
})
```

---

## Staged reliability transaction

`Reliability.BeginTx` stages fh journal and queue writes until `Commit`. It is
an in-process transaction boundary, not a transaction spanning an external
database. Always roll it back on an early return.

```go
import "github.com/oarkflow/fh"

app.Post("/transfer", func(c fh.Ctx) error {
    tx, err := c.Reliability().BeginTx(c.Context())
    if err != nil { return err }
    defer tx.Rollback()

    if err := tx.Journal().Append(entry); err != nil { return err }
    if err := tx.Queue().Enqueue(c.Context(), job); err != nil { return err }
    if err := tx.Commit(); err != nil { return err }
    return c.SendStatus(fh.StatusAccepted)
})
```

---

## Complete Example

```go
package main

import (
    "context"
    "log"

    "github.com/oarkflow/fh"
    "github.com/oarkflow/fh/mw/reliability"
)

type CreateOrderRequest struct {
    CustomerID string `json:"customer_id"`
}

type CreateOrderResponse struct {
    OrderID string `json:"order_id"`
}

func main() {
    app := fh.NewWithConfig(fh.Config{
        Reliability: fh.ReliabilityConfig{
            Enabled:            true,
            JournalEnabled:     true,
            IdempotencyEnabled: true,
            QueueEnabled:       true,
            DataDir:            "./data",
            QueueWorkers:       3,
        },
    })

    // Typed request/response wrapper with route-local reliability policy.
    app.Post("/orders", reliability.Endpoint(reliability.EndpointOptions[CreateOrderRequest, CreateOrderResponse]{
        Policy: fh.ReliabilityPolicy{Enabled: true, RequireIdempotency: true},
        Handle: func(ctx context.Context, c fh.Ctx, req CreateOrderRequest) (CreateOrderResponse, error) {
            return CreateOrderResponse{OrderID: "ord_123"}, nil
        },
    }))

    log.Fatal(app.ListenWithGracefulShutdown(":8080"))
}
```

## Storage Backends

fh's reliability runtime is storage-agnostic. File/directory persistence is the default, but production applications can plug in DBMS-backed stores by implementing these interfaces:

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

Wire custom stores through configuration:

```go
app := fh.NewWithConfig(fh.Config{
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

### Production rules for DB implementations

**Request journal** — `Append` must be immutable and durable. Do not update previous rows; use this for audit and incident investigation.

**Idempotency** — `Begin` must be atomic per key. It must never allow two concurrent requests with the same key to both return `fh.IdempotencyNew`. Return values:

- `fh.IdempotencyNew` — caller should process the request.
- `fh.IdempotencyReplay` — caller should replay the stored response.
- `fh.IdempotencyConflict` — same key was reused for a different request hash.
- `fh.IdempotencyProcessing` — the same request is already in-flight.

**Queue** — `Claim` must atomically move a visible pending job into processing state; this is the key operation for safe multi-worker and multi-instance deployments. A PostgreSQL implementation should use a transaction with `FOR UPDATE SKIP LOCKED`, or an atomic `UPDATE ... RETURNING` claim query. A SQLite implementation should use a write transaction and ensure only one process can claim a row at a time.

### Default backends

When no custom store is supplied, fh uses:

- `OpenRequestJournal(.fh-reliability/request-journal.jsonl)`
- `OpenIdempotencyStore(.fh-reliability/idempotency.jsonl)`
- `OpenFileQueueStorage(.fh-reliability/queue)`

Queue files are stored under `pending/`, `processing/`, `done/`, `failed/`, and an append-only `events.jsonl`.
