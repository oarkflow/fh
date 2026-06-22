# fh Advanced Server Runtime

`fh` is a high-performance Go HTTP server/framework with a Fiber-like API and an advanced application-runtime layer for reliability, security, observability, API gateway behavior, async jobs, workflows, signed requests, lifecycle tracking, and route policies.

This version preserves the existing route method API:

```go
app.Get(...)
app.Post(...)
app.Put(...)
app.Patch(...)
app.Delete(...)
app.Group(...)
```

All advanced behavior is added using middleware, handlers, policies, and runtime helpers. Existing applications can adopt these features gradually without changing their route registration style.

---

## Table of Contents

* [Core Idea](#core-idea)
* [Quick Start](#quick-start)
* [Advanced Platform Example](#advanced-platform-example)
* [Reliability Configuration](#reliability-configuration)
* [Per-Route Reliability Policy](#per-route-reliability-policy)
* [Reliable Endpoint Abstraction](#reliable-endpoint-abstraction)
* [Request-to-Job Atomic Handoff](#request-to-job-atomic-handoff)
* [Transactional Reliability API](#transactional-reliability-api)
* [Request Journal](#request-journal)
* [Idempotency](#idempotency)
* [Deterministic Idempotency](#deterministic-idempotency)
* [Durable Queue](#durable-queue)
* [Queue Priority](#queue-priority)
* [Delayed Jobs](#delayed-jobs)
* [Queue Concurrency Key](#queue-concurrency-key)
* [Dead-Letter Queue Management](#dead-letter-queue-management)
* [Outbox Pattern](#outbox-pattern)
* [Inbox Pattern](#inbox-pattern)
* [Request Lifecycle State Machine](#request-lifecycle-state-machine)
* [Request Compensation](#request-compensation)
* [Metrics Endpoint](#metrics-endpoint)
* [Access Logs](#access-logs)
* [Reverse Proxy Mode](#reverse-proxy-mode)
* [API Gateway Mode](#api-gateway-mode)
* [Circuit Breaker Middleware](#circuit-breaker-middleware)
* [Native SSE Support](#native-sse-support)
* [Advanced Static File Server](#advanced-static-file-server)
* [Signed Request Middleware](#signed-request-middleware)
* [Secret Redaction](#secret-redaction)
* [Security Event Stream](#security-event-stream)
* [Data Sensitivity Policies](#data-sensitivity-policies)
* [Secure Data Envelope](#secure-data-envelope)
* [Policy-Based Route Behavior](#policy-based-route-behavior)
* [API Contract Firewall](#api-contract-firewall)
* [Actor-Per-Key Middleware](#actor-per-key-middleware)
* [HTTP-Native Workflow Engine](#http-native-workflow-engine)
* [Queue and Journal Repair / Compaction](#queue-and-journal-repair--compaction)
* [API Version Compatibility](#api-version-compatibility)
* [Recommended Middleware Order](#recommended-middleware-order)
* [Real-World Usage Matrix](#real-world-usage-matrix)
* [Storage Backends](#storage-backends)
* [Production Notes](#production-notes)

---

# Core Idea

Most HTTP frameworks only handle:

```text
request → route → handler → response
```

`fh` can also handle durable application behavior:

```text
request
  ↓
route policy
  ↓
contract validation
  ↓
signed request verification
  ↓
idempotency check
  ↓
request lifecycle tracking
  ↓
journal record
  ↓
handler or reliable endpoint
  ↓
transactional queue/outbox/inbox handoff
  ↓
response replay capture
  ↓
metrics/access/security events
```

The goal is to make unsafe and important HTTP operations safer by default.

Examples:

```text
POST /payments       → idempotent, journaled, signed, replay-safe
POST /webhooks       → signed, inbox-deduped, queued
POST /emails         → idempotent, queued, retryable
POST /imports        → file upload, queued, SSE progress
GET  /_fh/metrics    → runtime visibility
GET  /events         → server-sent events
```

---

# Quick Start

```go
package main

import (
	"log"
	"time"

	"github.com/oarkflow/fh"
)

func main() {
	app := fh.New(fh.Config{
		Reliability: fh.ReliabilityConfig{
			Enabled:            true,
			DataDir:            ".fh-data",
			JournalEnabled:     true,
			IdempotencyEnabled: true,
			QueueEnabled:       true,
		},
	})

	app.EnableMetrics("/_fh/metrics")

	app.Use(fh.AccessLog(fh.AccessLogConfig{
		Mode: "json",
	}))

	app.Post("/orders",
		fh.Reliable(fh.ReliabilityPolicy{
			Enabled:             true,
			RequireIdempotency:  true,
			Journal:             true,
			ReplayResponse:      true,
			ConflictOnBodyDrift: true,
			MaxReplayAge:        24 * time.Hour,
		}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{
				"status":     "created",
				"request_id": c.Get("X-Request-ID"),
			})
		},
	)

	log.Fatal(app.Listen(":3000"))
}
```

Test:

```bash
curl -i -X POST http://localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-001' \
  -d '{"sku":"A1","qty":2}'
```

Retry with the same key and same body:

```bash
curl -i -X POST http://localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-001' \
  -d '{"sku":"A1","qty":2}'
```

The second request replays the stored response instead of creating duplicate work.

---

# Advanced Platform Example

A runnable example is included at:

```text
examples/advanced-platform
```

Run it:

```bash
cd examples/advanced-platform
go mod tidy
go run .
```

Useful endpoints:

```text
GET  /_fh/metrics
GET  /events
GET  /static/*
POST /orders
POST /email
POST /webhooks/payment
GET  /gateway/*
GET  /proxy/*
GET  /workflow/demo
```

The example demonstrates:

```text
per-route reliability policy
typed reliable endpoint
durable queue
request-to-job atomic handoff
outbox/inbox helpers
metrics
access logs
SSE
reverse proxy
API gateway
signed requests
contract firewall
route policy
actor-per-key
lifecycle tracking
request compensation
data sensitivity policy
secure data envelope
API version middleware
workflow engine
```

---

# Reliability Configuration

Reliability is configured through `fh.Config`.

```go
app := fh.New(fh.Config{
	Reliability: fh.ReliabilityConfig{
		Enabled:            true,
		DataDir:            ".fh-data",
		JournalEnabled:     true,
		IdempotencyEnabled: true,
		QueueEnabled:       true,
	},
})
```

Default file-backed layout:

```text
.fh-data/
  request-journal.jsonl
  idempotency.jsonl
  queue/
    events.jsonl
    pending/
    processing/
    done/
    failed/
```

Use this when:

```text
you want durable local development
you want embedded reliability without external DB
you want crash-recoverable jobs
you want audit logs without separate infrastructure
```

For production DBMS backends, implement:

```go
RequestJournalStore
IdempotencyRepository
QueueStorage
ReliabilityTx
```

---

# Per-Route Reliability Policy

`Reliable(policy)` applies reliability behavior to one route without changing route method APIs.

```go
app.Post("/payments",
	fh.Reliable(fh.ReliabilityPolicy{
		Enabled:             true,
		RequireIdempotency:  true,
		Journal:             true,
		ReplayResponse:      true,
		ConflictOnBodyDrift: true,
		MaxReplayAge:        24 * time.Hour,
	}),
	createPayment,
)
```

Use it when:

```text
the endpoint mutates state
the client may retry
duplicate processing is dangerous
you want an audit trail
you want replay-safe responses
```

Good routes for this:

```text
POST /orders
POST /payments
POST /emails
POST /webhooks/payment
POST /imports
POST /users
PATCH /account
```

Avoid strict idempotency for:

```text
GET routes
login routes unless specifically designed
streaming routes
routes where replayed response is unsafe
```

---

# Reliable Endpoint Abstraction

`ReliableEndpoint[Req, Res]` is a typed abstraction for reliable request handling.

It combines:

```text
body parsing
validation
idempotency
journal
handler execution
response replay
```

Example:

```go
type CreateOrderRequest struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

type CreateOrderResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

endpoint := fh.ReliableEndpoint[CreateOrderRequest, CreateOrderResponse]{
	Policy: fh.ReliabilityPolicy{
		Enabled:            true,
		RequireIdempotency: true,
		Journal:            true,
		ReplayResponse:     true,
	},
	Validate: func(c *fh.Ctx, req CreateOrderRequest) error {
		if req.SKU == "" || req.Qty <= 0 {
			return fh.NewHTTPError(400, "invalid order")
		}
		return nil
	},
	Handle: func(c *fh.Ctx, req CreateOrderRequest) (CreateOrderResponse, error) {
		return CreateOrderResponse{
			OrderID: "ord_123",
			Status:  "created",
		}, nil
	},
}

app.Post("/orders", endpoint.Handler())
```

Use this when:

```text
you want typed request/response handling
you want validation before business logic
you want fewer manual reliability steps
you want consistent response replay behavior
```

---

# Request-to-Job Atomic Handoff

For async work, the safest model is:

```text
HTTP request
  ↓
validate
  ↓
persist job
  ↓
return 202 only after durable enqueue
```

Example:

```go
app.Post("/email",
	fh.Reliable(fh.ReliabilityPolicy{
		Enabled:            true,
		RequireIdempotency: true,
		Journal:            true,
		ReplayResponse:     true,
	}),
	func(c *fh.Ctx) error {
		body := c.BodyCopy()

		job, err := fh.AtomicJob(c, fh.AtomicJobOptions{
			Type: "email.send",
			Body: body,
			Priority: fh.PriorityNormal,
		})
		if err != nil {
			return err
		}

		return c.Status(202).JSON(fh.Map{
			"status": "accepted",
			"job_id": job.ID,
		})
	},
)
```

Use this when:

```text
the operation may be slow
the operation talks to external systems
the operation should survive process restart
duplicate execution must be controlled
```

Good use cases:

```text
email sending
SMS sending
webhook processing
invoice generation
file imports
report generation
notification fanout
PDF generation
```

---

# Transactional Reliability API

The transactional reliability API provides a transaction-like boundary for journal, idempotency, and queue operations.

```go
tx, err := app.Reliability().BeginTx(c.Context())
if err != nil {
	return err
}
defer tx.Rollback()

err = tx.Journal().Append(c.Context(), fh.RequestJournalRecord{
	RequestID: c.Get("X-Request-ID"),
	Event:     "accepted",
	Method:    string(c.Method()),
	Path:      string(c.Path()),
})
if err != nil {
	return err
}

err = tx.Queue().Enqueue(c.Context(), fh.Job{
	Type: "email.send",
	Body: c.BodyCopy(),
})
if err != nil {
	return err
}

if err := tx.Commit(); err != nil {
	return err
}

return c.Status(202).JSON(fh.Map{"status": "accepted"})
```

Use it when:

```text
a request must journal and enqueue together
a webhook must dedupe and enqueue together
an order must publish an outbox event
you are implementing a SQLite/PostgreSQL backend
```

Important:

```text
The default file backend buffers operations and commits them together at the fh layer.
For true database atomicity, implement ReliabilityTx using a DB transaction.
```

---

# Request Journal

The request journal records request lifecycle activity.

Typical events:

```text
received
validated
authorized
accepted
queued
completed
failed
replayed
compensated
```

File-backed location:

```text
.fh-data/request-journal.jsonl
```

Use it for:

```text
audit trail
debugging
incident investigation
request recovery
compliance evidence
production support
```

Example journal append:

```go
app.Reliability().Journal().Append(c.Context(), fh.RequestJournalRecord{
	RequestID: c.Get("X-Request-ID"),
	Event:     "custom.business.accepted",
	Method:    string(c.Method()),
	Path:      string(c.Path()),
	Status:    202,
})
```

---

# Idempotency

Idempotency prevents duplicate unsafe operations.

Client header:

```http
Idempotency-Key: unique-client-generated-key
```

Behavior:

```text
new key + body           → process request
same key + same body     → replay previous response
same key + different body → 409 conflict
processing key           → 409 still processing
```

Use it for:

```text
orders
payments
email dispatch
file imports
webhooks
account creation
inventory mutation
admin actions
```

Do not rely only on client retries. Always combine with:

```text
body hash
conflict detection
response replay
expiry
journal
```

---

# Deterministic Idempotency

Some APIs should derive idempotency from business data instead of trusting only a header.

Example:

```go
app.Post("/payments",
	fh.DeterministicIdempotency(func(c *fh.Ctx) string {
		userID := c.Get("X-User-ID")
		externalID := c.Get("X-External-Order-ID")
		return userID + ":payment:" + externalID
	}),
	fh.Reliable(fh.ReliabilityPolicy{
		Enabled:             true,
		RequireIdempotency:  true,
		Journal:             true,
		ReplayResponse:      true,
		ConflictOnBodyDrift: true,
	}),
	createPayment,
)
```

Use it when:

```text
clients are unreliable
you already have a natural business key
duplicate payment/order creation is dangerous
webhook event IDs should be deduped
```

Good deterministic keys:

```text
tenant_id + order_id
user_id + external_payment_id
provider + webhook_event_id
tenant_id + import_id
```

---

# Durable Queue

The embedded queue supports durable file-backed job execution by default.

Queue states:

```text
pending
processing
done
failed
```

Queue lifecycle events:

```text
enqueued
claimed
completed
retry_scheduled
failed
recovered
discarded
```

File-backed layout:

```text
.fh-data/queue/
  events.jsonl
  pending/
  processing/
  done/
  failed/
```

Register a worker:

```go
app.Queue().Register("email.send", func(ctx context.Context, job fh.Job) error {
	log.Printf("sending email job=%s body=%s", job.ID, string(job.Body))
	return nil
})
```

Start workers automatically with the app if queue is enabled.

Use queue for:

```text
email
SMS
webhook delivery
PDF generation
CSV import
invoice generation
background cleanup
notification fanout
report generation
```

---

# Queue Priority

Jobs can include a priority.

```go
app.Queue().Enqueue(c.Context(), fh.Job{
	Type:     "email.send",
	Body:     c.BodyCopy(),
	Priority: fh.PriorityHigh,
})
```

Suggested priority levels:

```text
critical
high
normal
low
bulk
```

Use priority when:

```text
password reset emails should beat newsletters
payment webhooks should beat analytics jobs
admin tasks should beat bulk imports
tenant-critical tasks need faster processing
```

---

# Delayed Jobs

Jobs can be scheduled for later execution.

```go
app.Queue().Enqueue(c.Context(), fh.Job{
	Type:  "reminder.send",
	Body:  payload,
	RunAt: time.Now().Add(10 * time.Minute),
})
```

Use delayed jobs for:

```text
reminders
retry-later behavior
subscription renewal
scheduled email
abandoned cart notification
temporary-ban expiry action
```

---

# Queue Concurrency Key

Concurrency keys serialize jobs for the same logical resource.

```go
app.Queue().Enqueue(c.Context(), fh.Job{
	Type:           "billing.recalculate",
	Body:           payload,
	ConcurrencyKey: "tenant:123",
})
```

Guarantee:

```text
jobs with the same concurrency key do not run at the same time
jobs with different keys may run concurrently
```

Use it for:

```text
wallet balance updates
tenant billing
inventory mutation
user quota recalculation
workflow state mutation
account ledger operations
```

---

# Dead-Letter Queue Management

Failed jobs are moved into the failed state.

DLQ helpers allow:

```text
list failed jobs
retry failed job
discard failed job
inspect failed job
```

Example:

```go
app.Post("/admin/queue/failed/:id/retry", func(c *fh.Ctx) error {
	id := c.Param("id")
	if err := app.Queue().RetryFailed(c.Context(), id); err != nil {
		return err
	}
	return c.JSON(fh.Map{"status": "requeued", "job_id": id})
})
```

Use this for:

```text
admin queue dashboard
manual recovery
failed webhook replay
failed email retry
failed import investigation
```

---

# Outbox Pattern

The outbox pattern safely publishes events after business changes.

Flow:

```text
write business data
  ↓
write outbox event
  ↓
commit
  ↓
worker publishes event later
```

Example:

```go
event := fh.OutboxEvent{
	Topic: "order.created",
	Key:   orderID,
	Body:  []byte(`{"order_id":"ord_123"}`),
}

if err := app.Reliability().Outbox().Publish(c.Context(), event); err != nil {
	return err
}
```

Use outbox for:

```text
order.created events
invoice.created events
user.registered events
payment.succeeded events
notifications
cross-service integration
```

Why it matters:

```text
Without outbox, the database write may succeed but event publish may fail.
With outbox, event publishing becomes retryable.
```

---

# Inbox Pattern

The inbox pattern deduplicates incoming external events such as webhooks.

Example:

```go
app.Post("/webhooks/payment",
	fh.SignedRequest(fh.SignedRequestConfig{
		Secret: []byte("webhook-secret"),
	}),
	func(c *fh.Ctx) error {
		eventID := c.Get("X-Webhook-ID")

		accepted, err := app.Reliability().Inbox().Accept(c.Context(), fh.InboxEvent{
			Source:  "payment-provider",
			EventID: eventID,
			Body:    c.BodyCopy(),
		})
		if err != nil {
			return err
		}

		if !accepted {
			return c.JSON(fh.Map{"status": "duplicate_ignored"})
		}

		return c.Status(202).JSON(fh.Map{"status": "accepted"})
	},
)
```

Use inbox for:

```text
payment webhooks
email provider events
third-party callbacks
delivery receipts
external system notifications
```

---

# Request Lifecycle State Machine

Each request can have a lifecycle.

States/events:

```text
received
validated
authorized
accepted
queued
processing
completed
failed
replayed
compensated
```

Example:

```go
app.Post("/orders", func(c *fh.Ctx) error {
	c.Lifecycle().Mark("validated")
	c.Lifecycle().Mark("accepted")

	return c.JSON(fh.Map{"status": "ok"})
})
```

Use lifecycle tracking when:

```text
you need audit visibility
requests span multiple stages
requests enqueue background work
you need compensation or recovery
you want causal request/job traces later
```

---

# Request Compensation

Compensation hooks are used when an operation partially succeeds and later fails.

Example:

```go
app.Post("/orders", func(c *fh.Ctx) error {
	paymentID := "pay_123"

	c.Compensate(func(ctx context.Context) error {
		// refund payment if later steps fail
		log.Println("refund payment", paymentID)
		return nil
	})

	if err := createOrder(); err != nil {
		return c.RunCompensations(err)
	}

	return c.JSON(fh.Map{"status": "created"})
})
```

Use compensation for:

```text
payment refund
inventory release
temporary file cleanup
external resource cleanup
workflow rollback
```

---

# Metrics Endpoint

Enable built-in metrics:

```go
app.EnableMetrics("/_fh/metrics")
```

Endpoint:

```text
GET /_fh/metrics
```

Metrics include:

```text
requests_total
requests_inflight
responses_by_status
panics_total
idempotency_replays_total
idempotency_conflicts_total
queue_pending
queue_processing
queue_done
queue_failed
security_events_total
```

Use it for:

```text
health dashboards
load testing
production monitoring
debugging reliability behavior
queue visibility
```

Example:

```bash
curl http://localhost:3000/_fh/metrics
```

---

# Access Logs

`AccessLog` provides first-class logging modes.

```go
app.Use(fh.AccessLog(fh.AccessLogConfig{
	Mode: "json",
}))
```

Supported modes:

```text
json
common
combined
```

JSON log example:

```json
{
  "time": "2026-06-22T10:30:00Z",
  "request_id": "req_123",
  "method": "POST",
  "path": "/orders",
  "status": 201,
  "latency_ms": 2.31,
  "ip": "127.0.0.1"
}
```

Use JSON mode for:

```text
production logs
log processors
audit pipelines
structured debugging
```

Use common/combined modes for:

```text
classic web server logs
simple terminal debugging
compatibility with log parsers
```

---

# Reverse Proxy Mode

`ReverseProxy` forwards requests to an upstream.

Example:

```go
proxy := fh.ReverseProxy(fh.ReverseProxyConfig{
	Target: "http://localhost:4000",
})

app.All("/proxy/*", proxy)
```

Use reverse proxy mode for:

```text
frontend/backend split
legacy backend migration
internal service proxying
local development
traffic forwarding
```

Recommended pairings:

```text
access logs
circuit breaker
signed internal requests
route policy
metrics
```

---

# API Gateway Mode

`APIGateway` provides route-to-upstream behavior.

Example:

```go
gateway := fh.APIGateway(fh.APIGatewayConfig{
	Routes: []fh.GatewayRoute{
		{
			Prefix: "/gateway/users",
			Target: "http://localhost:4001",
		},
		{
			Prefix: "/gateway/orders",
			Target: "http://localhost:4002",
		},
	},
})

app.All("/gateway/*", gateway)
```

Use API gateway mode for:

```text
path-based service routing
internal API aggregation
service migration
gateway-level auth/rate-limit/logging
simple edge gateway
```

Common middleware stack:

```go
app.Use(fh.AccessLog(fh.AccessLogConfig{Mode: "json"}))
app.Use(fh.CircuitBreakerMiddleware(...))
app.Use(fh.SignedRequest(...))
```

---

# Circuit Breaker Middleware

Circuit breakers protect the server from repeatedly calling failing dependencies.

```go
breaker := fh.NewCircuitBreaker(fh.CircuitBreakerConfig{
	FailureThreshold: 5,
	ResetAfter:      30 * time.Second,
})

app.Use(fh.CircuitBreakerMiddleware(breaker))
```

States:

```text
closed
open
half-open
```

Use it for:

```text
reverse proxy upstreams
API gateway upstreams
expensive routes
external service calls
database-dependent endpoints
```

Behavior:

```text
too many failures → circuit opens
open circuit → fast failure
after reset period → half-open trial
successful trial → circuit closes
```

---

# Native SSE Support

SSE is useful for one-way server-to-client streaming.

Example:

```go
app.Get("/events", func(c *fh.Ctx) error {
	return c.SSE(func(s *fh.SSE) error {
		s.Event("hello", fh.Map{
			"message": "connected",
		})
		return nil
	})
})
```

Client:

```html
<script>
const es = new EventSource("/events");
es.addEventListener("hello", ev => {
  console.log(JSON.parse(ev.data));
});
</script>
```

Use SSE for:

```text
job progress
queue stats
admin dashboards
logs
notifications
import progress
workflow progress
```

Choose SSE instead of WebSocket when:

```text
server only needs to push data
client does not need bidirectional messaging
you want simple browser-native streaming
```

---

# Advanced Static File Server

`StaticAdvanced` and `StaticFilesAdvanced` provide a better static file server.

Features:

```text
ETag
Last-Modified
Cache-Control
safe path handling
content type detection
directory index control
download support
range-friendly behavior where supported
```

Example:

```go
app.Get("/static/*", fh.StaticAdvanced("./public", fh.StaticAdvancedConfig{
	CacheControl: "public, max-age=3600",
	ETag:         true,
	LastModified: true,
	Index:        false,
}))
```

Use it for:

```text
admin UI assets
public frontend files
downloadable reports
generated PDFs
uploaded documents
```

Recommended:

```text
disable directory listing
set immutable cache for fingerprinted assets
use attachment disposition for downloads
validate paths
```

---

# Signed Request Middleware

Signed requests protect webhooks and internal APIs.

Example:

```go
app.Post("/webhooks/payment",
	fh.SignedRequest(fh.SignedRequestConfig{
		Secret:         []byte("webhook-secret"),
		SignatureHeader: "X-Signature",
		TimestampHeader: "X-Timestamp",
		MaxSkew:        5 * time.Minute,
	}),
	handleWebhook,
)
```

Client signing format:

```text
HMAC-SHA256(timestamp + "." + body)
```

Use signed requests for:

```text
webhooks
internal service calls
server-to-server APIs
high-trust callbacks
```

Validation includes:

```text
body integrity
timestamp freshness
constant-time signature comparison
replay reduction
```

---

# Secret Redaction

Redaction prevents sensitive values from leaking into logs, journals, metrics, and debug output.

Default sensitive names:

```text
password
passwd
secret
token
authorization
cookie
set-cookie
api_key
access_token
refresh_token
private_key
```

Example:

```go
redactor := fh.NewRedactor(fh.RedactorConfig{
	Fields: []string{"password", "token", "authorization"},
})

safe := redactor.RedactMap(map[string]any{
	"email":    "user@example.com",
	"password": "secret",
})
```

Use it when handling:

```text
access logs
security events
request journal metadata
debug captures
admin views
error reports
```

---

# Security Event Stream

Security events provide structured visibility into security-relevant activity.

Events include:

```text
signed_request.failed
contract.rejected
rate_limit.blocked
idempotency.conflict
circuit.open
auth.failed
csrf.failed
body_limit.exceeded
panic.recovered
```

Example:

```go
app.SecurityEvents().Emit(fh.SecurityEvent{
	Type:      "signed_request.failed",
	RequestID: c.Get("X-Request-ID"),
	Path:      string(c.Path()),
	Severity:  "warning",
})
```

Use it for:

```text
security dashboards
audit logs
SIEM forwarding
admin notifications
incident investigation
```

---

# Data Sensitivity Policies

Routes can declare how sensitive their data is.

Example:

```go
app.Post("/kyc",
	fh.DataSensitivity(fh.DataPolicy{
		Level:       fh.SensitivePII,
		RedactLogs:  true,
		EncryptBody: true,
		JournalMode: fh.JournalHashOnly,
	}),
	handleKYC,
)
```

Sensitivity levels:

```text
public
internal
personal
sensitive_pii
financial
secret
```

Use data sensitivity policies for:

```text
KYC documents
financial APIs
personal data
authentication flows
admin actions
health records
private files
```

Policy can affect:

```text
logging
journaling
debug capture
queue payload handling
response capture
redaction
encryption
```

---

# Secure Data Envelope

Secure envelopes protect stored payloads.

Example:

```go
env, err := fh.SealEnvelope(fh.SecureEnvelopeConfig{
	KeyID: "local-key-1",
	Key:   key,
}, []byte(`{"secret":"payload"}`))
if err != nil {
	return err
}

plain, err := fh.OpenEnvelope(fh.SecureEnvelopeConfig{
	KeyID: "local-key-1",
	Key:   key,
}, env)
```

Use secure envelopes for:

```text
queue payloads
journal payload snapshots
idempotency response storage
debug captures
sensitive workflow data
```

Capabilities:

```text
encryption at rest
key ID tracking
nonce
body hash
tamper detection
future key rotation support
```

---

# Policy-Based Route Behavior

Route policy combines several route behaviors into one middleware.

Example:

```go
app.Post("/payments",
	fh.RoutePolicy(fh.Policy{
		Name:             "payments",
		RequireAuth:      true,
		RequireSignature: false,
		RequireIdempotency: true,
		DataSensitivity:  fh.Financial,
		MaxBodyBytes:     64 << 10,
		Timeout:          3 * time.Second,
	}),
	createPayment,
)
```

Use policy-based routes when:

```text
you want consistent endpoint hardening
you want route behavior documented in code
you want future doctor/audit tooling
you want safer defaults for dangerous routes
```

Good policy categories:

```text
public_read
user_write
admin_action
payment
webhook
file_upload
internal_service
```

---

# API Contract Firewall

The API contract firewall rejects bad requests before handlers run.

Example:

```go
app.Post("/users",
	fh.ContractFirewall(fh.Contract{
		Methods:      []string{"POST"},
		ContentTypes: []string{"application/json"},
		MaxBodyBytes: 32 << 10,
		RequireHeaders: []string{
			"Idempotency-Key",
		},
	}),
	createUser,
)
```

Rejects:

```text
wrong method
wrong content type
missing required headers
oversized body
unknown API version
invalid route policy
```

Use it for:

```text
public APIs
webhooks
payment routes
admin routes
strict JSON APIs
SDK-facing endpoints
```

---

# Actor-Per-Key Middleware

Actor-per-key serializes requests for the same logical key.

Example:

```go
app.Post("/tenants/:id/billing",
	fh.ActorPerKey(func(c *fh.Ctx) string {
		return "tenant:" + c.Param("id")
	}),
	recalculateBilling,
)
```

Guarantee:

```text
only one request for tenant:123 runs at a time
requests for other tenants can run concurrently
```

Use it for:

```text
wallets
billing
inventory
account balances
workflow state
tenant configuration
quota updates
```

This avoids many race conditions without locking the whole server.

---

# HTTP-Native Workflow Engine

The workflow engine models HTTP steps and jobs as a simple workflow.

Example:

```go
wf := fh.NewWorkflow("signup")
wf.Step("validate")
wf.Step("create_user")
wf.Step("send_email")
wf.Step("complete")

app.Post("/signup", func(c *fh.Ctx) error {
	result, err := wf.Run(c.Context(), fh.WorkflowInput{
		Data: c.BodyCopy(),
	})
	if err != nil {
		return err
	}
	return c.JSON(result)
})
```

Use workflows for:

```text
signup flows
KYC approval
order fulfillment
payment confirmation
file processing
human approval
multi-step forms
```

Workflow capabilities:

```text
named steps
HTTP handlers
queue jobs
branching-ready structure
state tracking
result propagation
```

---

# Queue and Journal Repair / Compaction

File-backed reliability storage includes repair/compaction hooks.

Use cases:

```text
recover orphaned processing jobs
compact old idempotency records
archive old journal records
discard corrupt queue files
verify queue directory layout
rotate event logs
```

Example:

```go
err := app.Reliability().Maintenance().Repair(c.Context())
if err != nil {
	return err
}

err = app.Reliability().Maintenance().Compact(c.Context(), fh.CompactOptions{
	OlderThan: 7 * 24 * time.Hour,
})
```

Use it for:

```text
startup self-healing
admin maintenance endpoint
scheduled cleanup
local file backend stability
```

Suggested admin endpoints:

```text
POST /admin/reliability/repair
POST /admin/reliability/compact
GET  /admin/reliability/stats
```

---

# API Version Compatibility

API version middleware helps evolve APIs safely.

Example:

```go
app.Use(fh.APIVersion(fh.APIVersionConfig{
	Header:   "Accept-Version",
	Default:  "2026-06-01",
	Allowed:  []string{"2026-01-01", "2026-06-01"},
	Deprecated: map[string]string{
		"2026-01-01": "2026-12-31",
	},
}))
```

Client:

```bash
curl -H 'Accept-Version: 2026-06-01' http://localhost:3000/api/users
```

Use versioning for:

```text
public APIs
mobile app compatibility
SDK clients
enterprise integrations
long-lived clients
```

Behavior:

```text
missing version → default version
unsupported version → 400 or 406
deprecated version → warning/sunset headers
allowed version → request continues
```

---

# Recommended Middleware Order

A strong production order:

```go
app.Use(fh.AccessLog(fh.AccessLogConfig{Mode: "json"}))
app.Use(fh.RoutePolicy(defaultPolicy))
app.Use(fh.ContractFirewall(defaultContract))
app.Use(fh.APIVersion(versionConfig))
app.Use(fh.SignedRequest(signatureConfig))       // only for signed zones/routes
app.Use(fh.CircuitBreakerMiddleware(breaker))
```

For individual unsafe routes:

```go
app.Post("/payments",
	fh.DataSensitivity(fh.DataPolicy{
		Level:       fh.Financial,
		RedactLogs:  true,
		JournalMode: fh.JournalHashOnly,
	}),
	fh.DeterministicIdempotency(paymentKey),
	fh.Reliable(fh.ReliabilityPolicy{
		Enabled:             true,
		RequireIdempotency:  true,
		Journal:             true,
		ReplayResponse:      true,
		ConflictOnBodyDrift: true,
	}),
	createPayment,
)
```

For webhooks:

```go
app.Post("/webhooks/payment",
	fh.SignedRequest(webhookSignatureConfig),
	fh.ContractFirewall(webhookContract),
	fh.Reliable(fh.ReliabilityPolicy{
		Enabled:            true,
		RequireIdempotency: false,
		Journal:            true,
	}),
	handleWebhook,
)
```

For async job APIs:

```go
app.Post("/email",
	fh.Reliable(fh.ReliabilityPolicy{
		Enabled:            true,
		RequireIdempotency: true,
		Journal:            true,
		ReplayResponse:     true,
	}),
	acceptEmailJob,
)
```

---

# Real-World Usage Matrix

| Use Case             | Features to Use                                                                          |
| -------------------- | ---------------------------------------------------------------------------------------- |
| Payment API          | Reliable policy, deterministic idempotency, journal, data sensitivity, contract firewall |
| Order creation       | Reliable endpoint, idempotency, response replay, outbox                                  |
| Webhook receiver     | Signed request, inbox, journal, queue                                                    |
| Email sending        | Atomic request-to-job handoff, queue, priority, delayed jobs                             |
| CSV import           | Multipart upload, queue, SSE progress, DLQ                                               |
| Invoice generation   | Queue, outbox, static file server                                                        |
| API gateway          | Reverse proxy, gateway routes, circuit breaker, access logs, metrics                     |
| Admin dashboard      | Metrics, queue stats, DLQ management, security events                                    |
| Multi-tenant billing | Actor-per-key, queue concurrency key, lifecycle                                          |
| KYC/document flow    | Data sensitivity, secure envelope, workflow, audit journal                               |
| Internal service API | Signed request or mTLS, API contract firewall, metrics                                   |
| Public API           | API versioning, contract firewall, access logs, route policy                             |

---

# Storage Backends

The default backend is file/directory based.

Production systems can replace it with DBMS-backed implementations.

Interfaces to implement:

```go
type RequestJournalStore interface {
	Append(ctx context.Context, record RequestJournalRecord) error
}

type IdempotencyRepository interface {
	Check(ctx context.Context, record IdempotencyRecord) (IdempotencyDecision, error)
	Complete(ctx context.Context, key string, response StoredResponse) error
	Fail(ctx context.Context, key string, err error) error
}

type QueueStorage interface {
	Enqueue(ctx context.Context, job Job) error
	Claim(ctx context.Context, workerID string, limit int) ([]Job, error)
	Complete(ctx context.Context, jobID string) error
	Fail(ctx context.Context, jobID string, err error) error
}

type ReliabilityTx interface {
	Journal() RequestJournalStore
	Idempotency() IdempotencyRepository
	Queue() QueueStorage
	Commit() error
	Rollback() error
}
```

Recommended DBMS tables:

```text
fh_request_journal
fh_idempotency
fh_queue_jobs
fh_queue_events
fh_outbox
fh_inbox
fh_operation_ledger
```

For SQLite/PostgreSQL, implement true DB transactions around:

```text
journal append
idempotency state update
queue enqueue
outbox publish
inbox accept
```

---

# Production Notes

## Idempotency

For unsafe routes:

```text
require Idempotency-Key
hash request body
reject body drift
store response
replay exact response
expire old keys
```

## Queue

For durable jobs:

```text
make handlers idempotent
use concurrency keys for mutable resources
use DLQ for permanent failures
use priority for critical jobs
use delayed jobs for retry/reminder flows
```

## Security

For sensitive routes:

```text
use signed requests or auth
apply data sensitivity policy
redact secrets
avoid journaling full bodies
use secure envelopes for stored sensitive payloads
```

## Observability

Enable:

```text
metrics endpoint
access logs
security events
queue event log
request journal
```

## API Gateway

Use with:

```text
circuit breaker
timeouts
access logs
contract firewall
API versioning
route policy
```

## Static Files

Use:

```text
ETag
Last-Modified
Cache-Control
disabled directory index
safe path handling
```

## Workflows

Use workflow engine when:

```text
the process has multiple steps
some steps are async
some steps need user approval
you need lifecycle visibility
you need compensation on failure
```

---

# Example Curl Commands

## Metrics

```bash
curl http://localhost:3000/_fh/metrics
```

## Idempotent order

```bash
curl -i -X POST http://localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-001' \
  -d '{"sku":"A1","qty":2}'
```

## Reliable email job

```bash
curl -i -X POST http://localhost:3000/email \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: email-001' \
  -d '{"to":"user@example.com","subject":"Hello","message":"Queued safely"}'
```

## SSE

```bash
curl -N http://localhost:3000/events
```

## Static file

```bash
curl -i http://localhost:3000/static/app.js
```

## API version

```bash
curl -i http://localhost:3000/api/users \
  -H 'Accept-Version: 2026-06-01'
```

## Signed webhook

```bash
BODY='{"event":"payment.succeeded","id":"evt_001"}'
TS="$(date +%s)"
SIG="$(printf '%s.%s' "$TS" "$BODY" | openssl dgst -sha256 -hmac 'webhook-secret' -hex | awk '{print $2}')"

curl -i -X POST http://localhost:3000/webhooks/payment \
  -H "Content-Type: application/json" \
  -H "X-Timestamp: $TS" \
  -H "X-Signature: sha256=$SIG" \
  -H "X-Webhook-ID: evt_001" \
  -d "$BODY"
```

---

# Summary

This advanced `fh` runtime adds a production-oriented application layer on top of the existing HTTP server.

The most important capabilities are:

```text
per-route reliability policy
typed reliable endpoints
request journal
idempotency and response replay
transactional queue handoff
outbox and inbox patterns
durable queue with priority/delay/concurrency keys
dead-letter management
metrics and access logs
SSE
reverse proxy and API gateway
signed requests
secret redaction
security event stream
data sensitivity policies
secure envelopes
actor-per-key request serialization
workflow engine
API contract firewall
API version compatibility
repair and compaction hooks
```

The goal is not only to serve HTTP quickly, but to make important HTTP operations:

```text
durable
auditable
idempotent
recoverable
observable
secure
policy-driven
```
