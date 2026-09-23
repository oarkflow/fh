# Webhook Relay — a complete BCL-owned HTTP application

This is a BCL-defined event intake and asynchronous webhook delivery service.
It accepts and validates events, authenticates callers, authorizes publishing
and status reads, persists events, deduplicates retries, throttles clients,
records a redacted audit trail, queues delivery, retries receiver failures, and
exposes status lookup.

The application and HTTP contract live in [app.bcl](./app.bcl). The Go host in
[main.go](./main.go) only loads the BCL, mounts it on fh, logs routes, and drains
the HTTP server on SIGINT/SIGTERM. It contains no route handlers, SQL business
logic, or delivery function.

## Platform features used

| Concern | Where |
|---|---|
| Input contracts | Strict event shape validated before intent execution |
| API authentication | X-API-Key against a configured auth.api_key resource |
| Authorization | publisher role and webhook:publish / webhook:read permissions |
| Durable persistence | PostgreSQL event ledger and startup migrations |
| Rate limiting | 60 event writes/min and 120 status reads/min per authenticated client |
| Idempotency | Required Idempotency-Key, scoped to event_id, kept for 24 hours |
| Auditing | Hash-chained route audit entries; event data is redacted from audit payload |
| Background work | File queue, worker intent, bounded retries and failed-job records |
| Outbound safety | Explicit host allowlist, private-network block, timeout, response cap |
| API contract | Descriptions, tags, status codes, body size limit, UUID path parameter |
| Runtime lifecycle | Startup compilation/resource checks and graceful shutdown |

## Request lifecycle

~~~text
POST /events
   |
   v
authenticate API key -> authorize webhook:publish -> rate limit
   |-- validate input against shape webhook.event
   |-- replay stored response for a repeated idempotency key
   |-- insert event as queued
   |-- publish webhook.deliver job
   '-- return HTTP 202

delivery_queue
   '-- worker webhook-delivery
          '-- webhook.deliver
                 |-- POST event to configured receiver
                 '-- mark ledger row delivered

GET /events/:id
   '-- authenticate -> authorize webhook:read -> rate limit -> query ledger
~~~

The request path does not wait for the receiver. The event ID and type are sent
in X-Event-ID and X-Event-Type; the receiver should deduplicate by event ID.
Only HTTP 200, 201, 202, and 204 count as successful delivery. Network failures
and other statuses are retried by the queue.

Delivery is at least once. If the receiver accepts an event but the ledger update
fails, the queue can deliver it again. If queue publication fails after the SQL
insert, the request fails while the row remains queued. This sample keeps those
two effects separate to make each feature visible; use the platform's
transactional outbox when closing that crash window is a requirement. Jobs that
exhaust retries are recorded under the queue's failed directory; their ledger
row remains queued here.

## Run locally

Requirements: Go 1.26.5+, PostgreSQL, and a writable working directory. Run
from the repository root because the default config path is relative.

~~~sh
createdb webhook_demo
export DATABASE_URL='postgres://localhost/webhook_demo?sslmode=disable'
export WEBHOOK_API_KEY="$(openssl rand -hex 32)"
export WEBHOOK_TARGET_URL='https://hooks.example.test/events'
export WEBHOOK_ALLOWED_HOST='hooks.example.test'
go run ./examples/ref-platform-complete
~~~

The server listens on http://localhost:8090. Replace the illustrative webhook
URL/host with a reachable endpoint. The URL host must match the allowlist.

| Variable | Required | Default | Purpose |
|---|---:|---|---|
| DATABASE_URL | yes | — | PostgreSQL DSN |
| WEBHOOK_API_KEY | yes | — | Inbound API key, at least 16 characters |
| WEBHOOK_TARGET_URL | yes | — | Receiver URL |
| WEBHOOK_ALLOWED_HOST | yes | — | Exact permitted receiver host |
| WEBHOOK_QUEUE_DIR | no | .data/ref-platform-webhook/queue | Queue and failed-job files |
| WEBHOOK_ALLOW_PRIVATE | no | false | Allow private/loopback destinations |

For a local receiver, set WEBHOOK_TARGET_URL=http://127.0.0.1:8099/events,
WEBHOOK_ALLOWED_HOST=127.0.0.1, and WEBHOOK_ALLOW_PRIVATE=true. Start a receiver
that accepts POST /events. Keep private-network access disabled outside local
development.

The main package accepts -config and -addr flags. Startup resolves required
environment values, connects to PostgreSQL, applies migrations, compiles the
intent graphs, validates resource/action references, then mounts the routes.
SIGINT and SIGTERM trigger graceful shutdown.

## API

All request and response bodies are JSON. Both routes require the X-API-Key
header. Use the configured key for local requests.

| Method | Path | Permission | Limit |
|---|---|---|---|
| POST | /events | webhook:publish | 60/minute per client |
| GET | /events/:id | webhook:read | 120/minute per client |

### Accept an event

POST /events accepts this shape:

~~~json
{
  "event_id": "b77a8b99-d26f-477c-8bf3-349b5b14ec03",
  "event_type": "invoice.paid",
  "occurred_at": "2026-09-23T10:00:00Z",
  "data": {
    "invoice_id": "inv_123",
    "amount": 4200
  }
}
~~~

event_id, event_type, and data are required. event_id must be a UUID;
event_type is 1–120 characters; occurred_at is optional and must be a date-time.
Unknown top-level properties are rejected. data is intentionally an open object
so different event types can carry their own fields.

Every POST also requires an Idempotency-Key header. The first request persists
the response for 24 hours. Repeating the same key for the same event ID returns
the stored response without inserting or enqueuing again. A different key with
an already-used event ID gets a conflict.

~~~sh
curl -i -H 'X-API-Key: YOUR_WEBHOOK_API_KEY' \
  -H 'Idempotency-Key: invoice-paid-b77a8b99' \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"b77a8b99-d26f-477c-8bf3-349b5b14ec03","event_type":"invoice.paid","data":{"invoice_id":"inv_123","amount":4200}}' \
  http://localhost:8090/events
~~~

A valid new event returns HTTP 202 with the database receipt and queue job ID.
An invalid shape is rejected before the intent performs effects. Authentication
failures return 401; permission denials return 403; rate limits return 429.

### Check status

~~~sh
curl -i -H 'X-API-Key: YOUR_WEBHOOK_API_KEY' \
  http://localhost:8090/events/b77a8b99-d26f-477c-8bf3-349b5b14ec03
~~~

The response includes state, timestamps, receiver status, and last error. A
missing event returns 404. In this compact sample last_error is reserved for
future failure-state persistence; exhausted jobs are visible in the queue's
failed directory and the event remains queued.

## How to read app.bcl

- resource blocks configure PostgreSQL, cache, rate limiter, API-key auth,
  RBAC, queue, and constrained outbound HTTP.
- role publisher grants only webhook publish/read permissions.
- shape blocks define the accepted event body.
- intent webhook.accept validates, stores, and queues the event.
- intent webhook.deliver runs in the worker and updates delivery status.
- intent webhook.status reads the event by a path value supplied through
  request.param.
- route blocks declare auth, permissions, throttles, idempotency, audit, and
  transport-level constraints in one place. The route audit block is spelled
  route_audit because audit is a reserved BCL schema keyword.

Nodes declare required/provided facts. The platform compiles these graphs at
startup, opens resources, applies migrations, and mounts routes before serving.
The BCL remains the application source: change the contract, policy, route, or
workflow there without writing HTTP handler code.

## Scope

The included file queue and in-memory rate-limit/idempotency cache are for one
process and local development. Multi-replica deployments need shared queue and
cache resources. Store the API key in a secret manager, terminate TLS in the
deployment, and keep WEBHOOK_ALLOW_PRIVATE=false. The public permissions here
are intentionally narrow; add distinct roles and keys when publishers and
operators need different administrative access.
