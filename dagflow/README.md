# Dagflow Production Runtime

Dagflow is a BCL-configured workflow/pipeline orchestration server for Go, served natively by [`github.com/oarkflow/fh`](https://github.com/oarkflow/fh). It supports workflow-as-route, route groups, middlewares, reusable workflows, workflow chaining, background/distributed node execution, rich DAG edges, task operations, runtime metadata APIs, SVG graphs, idempotency, DLQ, leases, outbox records, Postgres storage, and interpreter-backed script nodes.

This package fixes the previous startup race/loader issue where a route could validate before all workflow blocks were reliably loaded. `LoadBCLDir` now decodes every `.bcl`/`.hcl` file independently, merges fragments deterministically, and validates after all workflows, routes, scripts, schemas, conditions, and chains are registered. The sample BCL has also been split so each workflow is a separate file.

## Quick start

```bash
go mod tidy
go run .
```

Default development storage is file-backed at `data/dagflow.json` with an in-process broker.

Test the main workflow route:

```bash
curl -s -X POST localhost:8080/api/email/send \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-secret' \
  -H 'Idempotency-Key: demo-email-1' \
  -d '{"to":"a@b.com","subject":"Hi","body":"Hello"}' | jq
```

Test the interpreter/SPL workflow route:

```bash
curl -s -X POST localhost:8080/api/email/script-enrich \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-secret' \
  -d '{"to":"a@b.com","subject":"Hi","body":"Hello"}' | jq
```

Validate config:

```bash
go run . validate ./bcl
curl -s -H 'X-Admin-Token: dev-admin' localhost:8080/ops/validate | jq
```

## Production start with Postgres

Create a database, then run with Postgres storage and durable queue enabled:

```bash
export DAGFLOW_ENV=production
export DAGFLOW_STORE=postgres
export DAGFLOW_POSTGRES_DSN='postgres://dagflow:dagflow@localhost:5432/dagflow?sslmode=disable'
export DAGFLOW_ADMIN_TOKEN='change-this-admin-token'
export DAGFLOW_SIGNING_SECRET='change-this-long-random-signing-secret'
export DAGFLOW_WORKER_ID='worker-1'

go run .
```

`PostgresStorage` runs migrations automatically. The storage implementation is behind the `Storage`/`TaskStore`/`ChainStore`/`DurableQueueStore` interfaces, so another store can be implemented without changing the workflow engine.

## Project layout

```txt
.
├── bcl/
│   ├── 00-server/server.bcl
│   ├── 10-security/middleware.bcl
│   ├── 15-schemas.bcl
│   ├── 20-workflows/
│   │   ├── batch_uppercase.bcl
│   │   ├── chains.bcl
│   │   ├── conditions.bcl
│   │   ├── email.bcl
│   │   ├── email_approval.bcl
│   │   ├── parallel_order.bcl
│   │   └── script_email_enrich.bcl
│   ├── 25-scripts.bcl
│   └── 30-routes/routes.bcl
├── config.go                  # deterministic BCL directory loader/merger
├── engine.go                  # workflow execution engine
├── advanced_edges.go           # FanIn/FanOut/Parallel/Race helpers
├── storage_production.go       # Postgres Storage implementation
├── postgres_broker.go          # durable queue broker using Postgres jobs
├── durable_store.go            # file-backed dev store
├── http.go                     # native fh dynamic routes and middleware
├── metadata_api.go             # native fh workflow/task metadata APIs
├── validation_openapi.go       # validator and OpenAPI generation
├── interpreter_runtime.go      # github.com/oarkflow/interpreter script runtime
└── main.go                     # CLI and fh server bootstrap
```

## BCL directory loading

The loader walks the BCL directory, sorts files by path, decodes each file independently, and merges the result. This gives deterministic behavior and prevents large concatenated config parsing issues.

Supported top-level blocks:

```hcl
server { address ":8080" }
global_middlewares ["recover", "request_id", "logger"]

middleware "api_key" { ... }
condition "is_email_valid" { ... }
schema "EmailRequest" { ... }
script "script_enrich_email" { ... }
workflow "email_flow" { ... }
chain "email_api_chain" { ... }
route_group "api" { ... }
route "standalone_route" { ... }
```

## Route groups

Route groups inherit prefix and middleware.

```hcl
route_group "api" {
  prefix "/api"
  middlewares ["cors", "api_key", "small_body"]

  route_group "email" {
    prefix "/email"
    middlewares ["email_rate_limit"]

    route "send_email" {
      method "POST"
      path "/send"
      workflow "email_flow"
      mode sync
    }
  }
}
```

Final route:

```txt
POST /api/email/send
middlewares: recover, request_id, logger, cors, api_key, small_body, email_rate_limit
```

Routes may be static, dynamic, or wildcard:

```hcl
path "/send"              # static
path "/:order_id/check"   # dynamic param
path "/{order_id}/check"  # dynamic param
path "/*path"             # wildcard
```

Route matching priority is static > param > wildcard.

## Workflows

A workflow is a named graph of nodes and edges.

```hcl
workflow "email_flow" {
  name "Reliable Email Workflow"
  version "1.0.0"
  first "receive"
  max_visits 256
  debug true

  node "receive" {
    type function
    handler "receive_email"
  }

  node "validate" {
    type function
    handler "validate_email"
    timeout "2s"
  }

  edge "receive_to_validate" {
    from "receive"
    to "validate"
    type simple
  }
}
```

## Node types

```txt
function   Go handler from registry
script     interpreter-backed runtime script
workflow   reusable workflow-as-node
page       pause/wait node
join       synchronization point
noop       pass-through
```

Execution modes:

```txt
inline       execute in current worker
background  execute through local queue
localized distributed  execute through Broker; PostgresBroker gives durable queue
```

Node config supports:

```hcl
node "send" {
  type function
  handler "send_email"
  mode background
  await true
  timeout "3s"
  retry_policy {
    max_attempts 3
    strategy "exponential_jitter"
    initial_delay "100ms"
    max_delay "2s"
    jitter true
  }
}
```

## Edge types

Supported edge types:

```txt
simple       one source to one target
branch       condition-selected targets
iterator     iterate slice/array input
fanout       copy result to many targets
fanin        collect many source results into one target
parallel     run targets concurrently
join         synchronization alias/point
race         first successful target wins
error        route failure payload
fallback     fallback path
retry        retry path
timeout      timeout path
compensate   compensation/saga path
```

FanOut/FanIn example:

```hcl
edge "send_fanout" {
  from "send"
  targets ["store_valid", "audit"]
  type fanout
}

edge "valid_join" {
  sources ["store_valid", "audit"]
  to "notify_success"
  type fanin
  strategy "all"
}
```

Parallel example:

```hcl
edge "parallel_checks" {
  from "receive"
  targets ["check_inventory", "check_payment", "check_fraud"]
  type parallel
  max_concurrency 3
  fail_fast true
}
```

## Conditions

Conditions use `github.com/oarkflow/bcl` evaluation.

```hcl
condition "is_email_valid" {
  all [
    `result.valid == true`,
    `result.request.to != ""`
  ]
}
```

Use from an edge:

```hcl
edge "valid_to_send" {
  from "validate"
  to "send"
  type branch
  condition "is_email_valid"
}
```

## Interpreter script nodes

Scripts are defined in BCL and executed by `github.com/oarkflow/interpreter`.

```hcl
script "script_enrich_email" {
  source `
    let req = input;
    {
      "to": req.to,
      "subject": "[script] " + req.subject,
      "body": req.body,
      "enriched": true
    };
  `
}
```

Workflow:

```hcl
workflow "script_email_enrich" {
  first "enrich"

  node "enrich" {
    type script
    handler "script_enrich_email"
    input_schema "EmailRequest"
    output_schema "EmailResponse"
    last true
  }
}
```

Runtime facts passed to scripts include:

```txt
input
params
task
result
nodes
workflow
node
```

## Storage interfaces

The runtime is not tied to Postgres. Implement these interfaces for another backend:

```go
type TaskStore interface {
    Create(*Task) error
    Save(*Task) error
    Get(taskID string) (*Task, error)
    List() []*Task
    GetIdempotency(key string) (*IdempotencyRecord, error)
    PutIdempotency(rec IdempotencyRecord) error
    AddDLQ(item DLQItem) error
    ListDLQ() []DLQItem
    DeleteDLQ(id string) error
}
```

Production storage extends it with:

```go
type Storage interface {
    ExtendedStore
    Migrate(context.Context) error
    Health(context.Context) error
    Close() error
}
```

Postgres tables created automatically:

```txt
dagflow_tasks
dagflow_chains
dagflow_idempotency
dagflow_dlq
dagflow_outbox
dagflow_leases
dagflow_snapshots
dagflow_jobs
dagflow_node_dedup
```

## Idempotency

Route-level idempotency uses the `Idempotency-Key` header.

```bash
curl -s -X POST localhost:8080/api/email/send \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-secret' \
  -H 'Idempotency-Key: email-demo-1' \
  -d '{"to":"a@b.com","subject":"Hi","body":"Hello"}' | jq
```

Node-level dedup is implemented using:

```txt
workflow_id + workflow_version + task_id + node_id + input_hash
```

Completed node results can be reused safely during retry/recovery.

## Operations API

All `/ops/*` endpoints are protected by `DAGFLOW_ADMIN_TOKEN` when set. In production, the token is required.

```txt
GET  /ops/metadata
GET  /ops/validate
GET  /ops/workflows
GET  /ops/workflows/{id}
GET  /ops/workflows/{id}/metadata
GET  /ops/workflows/{id}/versions
GET  /ops/workflows/{id}/graph.svg
GET  /ops/tasks
GET  /ops/tasks/{id}
GET  /ops/tasks/{id}/audit
GET  /ops/tasks/{id}/graph.svg
POST /ops/tasks/{id}/pause
POST /ops/tasks/{id}/resume
POST /ops/tasks/{id}/cancel
POST /ops/tasks/{id}/restart
POST /ops/tasks/{id}/continue
GET  /ops/dlq
POST /ops/dlq/{id}/replay
POST /ops/dlq/{id}/discard
GET  /ops/outbox
GET  /ops/leases
GET  /metrics
GET  /openapi.json
```

Examples:

```bash
curl -s -H 'X-Admin-Token: dev-admin' localhost:8080/ops/workflows | jq
curl -s -H 'X-Admin-Token: dev-admin' localhost:8080/ops/workflows/email_flow/graph.svg > email.svg
curl -s -H 'X-Admin-Token: dev-admin' localhost:8080/ops/tasks/<task_id>/graph.svg > task.svg
```

Pause/resume:

```bash
TASK=$(curl -s -X POST localhost:8080/api/email/approval \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-secret' \
  -d '{"to":"a@b.com","subject":"Need approval","body":"Hello"}' | jq -r .id)

curl -s -H 'X-Admin-Token: dev-admin' localhost:8080/ops/tasks/$TASK | jq

curl -s -X POST localhost:8080/ops/tasks/$TASK/resume \
  -H 'X-Admin-Token: dev-admin' \
  -H 'Content-Type: application/json' \
  -d '{"approved":true,"approved_by":"admin"}' | jq
```

## Security

Implemented security features:

```txt
opsGuard for operations APIs
DAGFLOW_ADMIN_TOKEN required in production
API key middleware
body size middleware
signed resume tokens with HMAC
secret redaction for audit/log payloads
route/group/global middleware layering
schema validation hooks
```

Production recommendations:

```txt
set DAGFLOW_ENV=production
set DAGFLOW_ADMIN_TOKEN
set DAGFLOW_SIGNING_SECRET
run behind TLS
rotate API keys/admin token
restrict /ops routes by network/firewall
use Postgres, not file store
monitor DLQ/outbox/leases
```

## Metrics

```bash
curl -s localhost:8080/metrics | jq
```

Counters include workflow/node started/completed/failed totals, idempotency hits, retry/failure counters, and runtime queues where available.

## CLI

```bash
go run . serve
go run . validate ./bcl
go run . graph email_flow > email.svg
go run . inspect <task_id>
```

## Troubleshooting

### `workflow script_email_enrich not found`

This was caused by directory BCL decoding as one large concatenated document. The loader now decodes files independently, and `script_email_enrich` lives in its own BCL file:

```txt
bcl/20-workflows/script_email_enrich.bcl
```

### `edge <id> requires from/to or sources/targets`

Use multiline BCL blocks. Avoid one-line inline blocks for nodes/edges/fields.

Good:

```hcl
edge "receive_to_validate" {
  from "receive"
  to "validate"
  type simple
}
```

### Postgres connection fails

Check:

```bash
echo $DAGFLOW_POSTGRES_DSN
psql "$DAGFLOW_POSTGRES_DSN" -c 'select 1'
```

### Missing interpreter module

This project imports `github.com/oarkflow/interpreter v0.0.2`. Run:

```bash
go mod tidy
```

## Validation checklist

Before production traffic:

```bash
go run . validate ./bcl
go test ./...
go test -race ./...
```

Required environment:

```bash
DAGFLOW_ENV=production
DAGFLOW_STORE=postgres
DAGFLOW_POSTGRES_DSN=...
DAGFLOW_ADMIN_TOKEN=...
DAGFLOW_SIGNING_SECRET=...
```

## Notes on production readiness

This generation includes the production primitives: Postgres storage, durable queue operations, leases, DLQ, outbox, idempotency, node dedup, validation, signed resume tokens, metadata/graph APIs, and guarded operations APIs. For a regulated or money-moving system, add external integration-specific idempotency keys and audit retention policies at your application layer.
