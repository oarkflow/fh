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
├── failed/      # Failed jobs after max retries
└── quarantine/  # Invalid or damaged records retained for inspection
```

The file-backed queue requires its root and state directories to be private
(`0700`) and its event log to be a regular private file (`0600`) on Unix-like
systems. It rejects symlinked state directories and event logs. Queue job IDs
are validated before they are used as file names. State changes sync both the
job file and its containing directory; recovery keeps a persisted pending,
completed, or failed transition when a crash occurred before the old
processing file was removed. Windows permission enforcement depends on the
directory ACL configured by the operator. Job contents are flushed on Windows,
but directory-entry syncing is unavailable through Go's portable filesystem
API there, so rename durability across sudden power loss depends on the
filesystem and remains best-effort. `events.jsonl` is an observability log;
the job-state files are authoritative. Event append failures are reported via
`QueueLogError` (or stderr) and do not turn a successfully persisted enqueue
into an ambiguous error response.

Invalid, malformed, or symlinked job files encountered during claim or startup
recovery are moved into `quarantine/` rather than deleted. They are excluded
from worker processing and queue counts; inspect and remove them through
operator-controlled maintenance.

When retrying an enqueue after an uncertain network response, pass the same
explicit job ID with the same job type, payload, headers, retry limit, priority,
run time, and concurrency key. The file-backed queue treats that request as
idempotent while the ID exists in any state; reusing the ID for different work
returns `fh.ErrQueueJobIDConflict` instead of replacing the existing job.

### Worker Processing

```go
// Register handlers before serving. Workers start with the reliability runtime.
app.Queue().Register("send-email", func(ctx context.Context, job *fh.QueueJob) error {
    return sendEmail(ctx, job.Payload)
})
```

A panic from a job handler is contained to that job and treated as a failed
attempt. The worker remains available; normal retry limits and dead-letter
handling still apply. Failed queue storage operations during retry or
completion are reported through `QueueLogError`, or stderr when no logger is
configured, so jobs left in `processing` are visible to operators.

### Job Features

- **Priority:** Jobs can have priority levels
- **Delayed Jobs:** Schedule jobs for future execution
- **Concurrency Keys:** Limit concurrent processing by key within one
  `DurableQueue` instance. For multiple replicas, the SQL adapter persists
  expiring key leases for PostgreSQL and MySQL. Other storage adapters must
  explicitly support cross-replica key concurrency; the production deployment
  gate rejects this option when the configured store does not provide that
  capability. A job waiting for its key is deferred without consuming a handler
  attempt by the built-in memory, file and SQL adapters.
- **Max Attempts:** Configurable retry count with exponential backoff
- **File queue recovery:** On startup, interrupted processing jobs are moved back to pending

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

The file queue is process-local and its startup recovery may return interrupted
processing jobs to pending. For shared PostgreSQL or MySQL queues, use
`pkg/storage/sql` and set `sqlstore.Config.QueueLeaseTTL` when opening the
adapter (30 seconds by default). SQL claims increment `QueueJob.LeaseFence`,
store an expiry using the database clock, and use that same clock to decide
when pending jobs are due and to schedule retry backoff. Replicas do not make
claim, expiry, or retry-delay decisions from their local clocks. PostgreSQL
claimers use `FOR UPDATE SKIP LOCKED` to distribute concurrent work without
waiting on another replica's candidate row. When `QueueConcurrencyLimitByKey` is
enabled, SQL stores acquire a per-key lease tied to the claim fence and renew it
with the job lease. Completion, retry and failure release the key lease in the
same transaction as the job state change.
Completion, retry and failure updates require the current unexpired
fence. Any replica can atomically reclaim an expired claim when polling, so a
worker crash does not wait for another replica to restart. Explicit recovery
also only requeues expired claims. SQLite does not advertise the multi-replica
capability.

This provides at-least-once delivery. A handler may run again after a crash or
lease loss. Make external effects idempotent; where effects share a database
transaction, include `job.LeaseFence` in the write's ownership check. Direct
users of `QueueStorage.Claim` must call `QueueLeaseRenewer.RenewLease` during
long jobs themselves. Upgrade from a pre-lease SQL adapter with all old workers
stopped, because those binaries cannot renew leases or enforce claim fences.

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

Queue job snapshots redact common credential fields in payload previews, header
values, and failure text. Keep queue listing behind the same authorization and
data-access controls as other operational endpoints; redaction cannot identify
application-specific sensitive fields unless they use recognizable names.

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

The optional `pkg/storage/memory` adapters are process-local and non-durable.
Their journal defaults to 50,000 entries and returns `memory.ErrJournalCapacity`
when full; idempotency and queue entries are capped at 50,000 by default.
The in-memory journal defaults to a 16 MiB retained-byte budget. The
idempotency cache defaults to 64 MiB and drops a response from the cache when it
cannot fit. The in-memory queue evicts its oldest job when full and defaults to
a 64 MiB estimated retained-byte budget. `memory.NewWithConfig` can tune these
limits. Use a durable backend when the journal or queued work must survive
process restarts or capacity exhaustion.

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
