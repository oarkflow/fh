# fh Real-World Examples

This archive contains production-style example applications for `github.com/oarkflow/fh`.

Copy the `examples/realworld` directory into the root of the `fh` repository, then run any example:

```bash
cd examples/realworld/reliable-email
go mod tidy
go run . -addr :3000
```

Each example has its own `go.mod`, `main.go`, and `README.md`. The examples use `replace github.com/oarkflow/fh => ../../..`, so they are intended to run from inside the `fh/examples/realworld/<example>` path.

## Included examples

| Example | Main features |
|---|---|
| `reliable-email` | idempotency, request journal, durable queue, async worker, queue stats |
| `orders-idempotency` | safe order creation, idempotency replay, conflict protection, request journal |
| `webhook-receiver` | HMAC signature validation, replay-safe webhook handling, durable queue |
| `csv-importer` | multipart upload, body limits, file persistence, durable import worker |
| `invoice-generator` | async invoice generation, static file serving, durable queue |
| `notification-fanout` | multi-channel notification jobs, queue fanout, WebSocket-ready payload model |
| `payment-api` | required idempotency, payment intent creation, webhook queueing |
| `admin-dashboard` | Basic Auth, security headers, rate limiting, queue stats, HTML admin page |
| `secure-file-processing` | upload validation, durable file-processing pipeline, static downloads |
| `api-gateway` | CORS, security headers, rate limits, route groups, rewrite, proxy-style gateway responses |
| `multi-tenant-api` | tenant middleware, route groups, per-tenant idempotent writes, tenant-scoped queue jobs |
| `secure-api-middleware-stack` | complete route-oriented middleware stack for public, partner, browser, webhook, operations, static, and proxy boundaries |
| `workflow-reliable-checkout` | actor-serialized checkout workflow, lifecycle hooks, idempotent response replay, and durable queue handoff |

See [`realworld/README.md`](realworld/README.md) for the complete middleware-to-example coverage matrix.

## Common reliability files

Examples that enable reliability write to `.fh-data` by default:

```text
.fh-data/request-journal.jsonl
.fh-data/idempotency.jsonl
.fh-data/queue/events.jsonl
.fh-data/queue/pending/
.fh-data/queue/processing/
.fh-data/queue/done/
.fh-data/queue/failed/
```

Use a unique `Idempotency-Key` when you want a new mutating operation. Reusing the same key with the same request body replays the stored response by design.
