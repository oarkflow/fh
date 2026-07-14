# Reliability Layer

fh includes an optional, built-in reliability layer for production-grade request processing: request journaling, idempotency support, and a durable async job queue with outbox/inbox patterns and dead-letter queue.

## Enabling Reliability

```go
app := fh.New(fh.Config{
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

The journal file is at `{DataDir}/journal.jsonl`.

### Custom Journal Store

```go
type RequestJournalStore interface {
    Append(entry []byte) error
    Close() error
}
```

---

## Idempotency

Ensures that unsafe methods (POST, PUT, PATCH, DELETE) are processed exactly once.

```go
type CreateOrderRequest struct {
    ProductID string `json:"product_id"`
    Quantity  int    `json:"quantity"`
}

type CreateOrderResponse struct {
    OrderID string `json:"order_id"`
    Status  string `json:"status"`
}

app.PostTyped("/orders", reliability.Endpoint[CreateOrderRequest, CreateOrderResponse]{
    Handler: func(c *fh.Ctx, req CreateOrderRequest) (CreateOrderResponse, error) {
        // This handler will only be called once per unique Idempotency-Key
        order, err := createOrder(req)
        return CreateOrderResponse{OrderID: order.ID, Status: "created"}, err
    },
})
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
    Get(key string) ([]byte, bool, error)
    Set(key string, data []byte, ttl time.Duration) error
    Delete(key string) error
    Close() error
}
```

---

## Durable Queue

File-backed async job queue with crash recovery, retries, and worker processing.

```go
app.Post("/process", func(c *fh.Ctx) error {
    // Enqueue a job
    return c.ServerOutbox().Enqueue("process-payment", paymentData)
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
// Workers are started automatically when QueueEnabled is set
// Each worker picks up jobs from the pending directory,
// processes them, and moves them to done or failed.

// Custom worker registration:
app.OnListen(func() {
    app.ServerOutbox().RegisterWorker("send-email", func(job Job) error {
        return sendEmail(job.Data)
    })
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
    Enqueue(job *Job) error
    Dequeue() (*Job, error)
    Ack(id string) error
    Nack(id string, requeue bool) error
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
outbox.Enqueue("order.created", orderEvent)

// Register worker at startup:
app.OnListen(func() {
    app.ServerOutbox().RegisterWorker("order.created", func(job Job) error {
        return publishEvent("order.created", job.Data)
    })
})
```

### Inbox

```go
// For webhook handlers with idempotency:
inbox := c.ServerInbox()
// Automatically deduplicates based on webhook ID
```

---

## Dead-Letter Queue (DLQ)

Failed jobs that exceed max attempts are moved to the dead-letter queue.

```go
// Retry failed jobs
app.ServerOutbox().RetryFailed()

// Discard failed jobs
app.ServerOutbox().DiscardFailed()

// List failed jobs
failedJobs, err := app.ServerOutbox().ListFailed()
```

---

## AtomicJob (Request-to-Job Handoff)

Atomically processes a request and enqueues a follow-up job in a single reliability transaction.

```go
import "github.com/oarkflow/fh"

app.Post("/orders", func(c *fh.Ctx) error {
    var req CreateOrderRequest
    c.BodyParser(&req)

    return fh.AtomicJob(c, req, "order.fulfill", func() error {
        // This runs atomically with job enqueue
        return createOrder(req)
    })
})
```

---

## BeginTx (Transactional API)

```go
import "github.com/oarkflow/fh"

app.Post("/transfer", func(c *fh.Ctx) error {
    return fh.BeginTx(c, func() error {
        // All operations within this transaction are reliable
        c.ServerOutbox().Enqueue("audit.log", event)
        c.ServerInbox().Process(webhookID, data)
        return nil
    })
})
```

---

## Complete Example

```go
package main

import (
    "github.com/oarkflow/fh"
    "github.com/oarkflow/fh/mw/reliability"
)

func main() {
    app := fh.New(fh.Config{
        Reliability: fh.ReliabilityConfig{
            Enabled:            true,
            JournalEnabled:     true,
            IdempotencyEnabled: true,
            QueueEnabled:       true,
            DataDir:            "./data",
            QueueWorkers:       3,
        },
    })

    // Reliability middleware (generates request IDs, handles idempotency)
    app.Use(reliability.New())

    // Typed endpoint with idempotency
    app.PostTyped("/orders", reliability.Endpoint[CreateOrderRequest, CreateOrderResponse]{
        Handler: func(c *fh.Ctx, req CreateOrderRequest) (CreateOrderResponse, error) {
            // Create order (only once per Idempotency-Key)
            return CreateOrderResponse{OrderID: "ord_123"}, nil
        },
    })

    app.Listen(":8080")
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
