# DAGFlow FH

DAGFlow FH is a BCL-configured workflow and pipeline orchestration runtime for Go applications served with [`github.com/oarkflow/fh`](https://github.com/oarkflow/fh).

It is designed as a reusable, business-agnostic workflow core plus an example application layer:

```txt
pkg/dagflow   reusable generic workflow engine/runtime
app           example application, BCL files, business handlers, scripts, schemas
main.go       tiny entrypoint calling app.Run()
```

The core package does **not** know about email, orders, users, payments, KYC, approvals, or any other business domain. Business behavior is added by:

- registering Go handlers from the application,
- defining workflows/routes/middlewares/conditions/schemas in BCL,
- defining script handlers in BCL,
- executing interpreter-backed script nodes with `github.com/oarkflow/interpreter`.

DAGFlow supports workflow-as-route, route groups, middlewares, reusable workflows, workflow chaining, workflow-as-node, background/distributed node execution, rich DAG edges, task operations, runtime metadata APIs, SVG graphs, idempotency, DLQ, leases, outbox records, Postgres storage, and interpreter-backed script nodes.

---

## Table of contents

- [Project layout](#project-layout)
- [Quick start](#quick-start)
- [Production start with Postgres](#production-start-with-postgres)
- [Configuration model](#configuration-model)
- [BCL directory loading](#bcl-directory-loading)
- [Route groups and dynamic routes](#route-groups-and-dynamic-routes)
- [Middlewares](#middlewares)
- [Workflows](#workflows)
- [Nodes](#nodes)
- [Edges](#edges)
- [Conditions](#conditions)
- [Interpreter script nodes](#interpreter-script-nodes)
- [Storage](#storage)
- [Durable queue and workers](#durable-queue-and-workers)
- [Idempotency and deduplication](#idempotency-and-deduplication)
- [DLQ and outbox](#dlq-and-outbox)
- [Task operations](#task-operations)
- [Operations API](#operations-api)
- [Metadata and graph APIs](#metadata-and-graph-apis)
- [OpenAPI and schemas](#openapi-and-schemas)
- [Metrics](#metrics)
- [CLI commands](#cli-commands)
- [Adding app handlers](#adding-app-handlers)
- [Security](#security)
- [Production checklist](#production-checklist)
- [Troubleshooting](#troubleshooting)

---

## Project layout

Recommended layout after the refactor:

```txt
.
├── main.go
├── go.mod
├── go.sum
├── README.md
├── data/
│   └── dagflow.json              # local/dev file store, created at runtime
├── app/
│   ├── run.go                    # app bootstrap
│   ├── handlers.go               # app/business handlers
│   └── bcl/
│       ├── 00-server/
│       │   └── server.bcl
│       ├── 10-security/
│       │   └── middleware.bcl
│       ├── 15-schemas.bcl
│       ├── 20-workflows/
│       │   ├── batch_uppercase.bcl
│       │   ├── chains.bcl
│       │   ├── conditions.bcl
│       │   ├── email.bcl
│       │   ├── email_approval.bcl
│       │   ├── parallel_order.bcl
│       │   └── script_email_enrich.bcl
│       ├── 25-scripts.bcl
│       └── 30-routes/
│           └── routes.bcl
└── pkg/
    └── dagflow/
        ├── advanced_edges.go
        ├── config.go
        ├── condition.go
        ├── durable_store.go
        ├── engine.go
        ├── graph.go
        ├── http.go
        ├── interpreter_runtime.go
        ├── metadata_api.go
        ├── operations.go
        ├── postgres_broker.go
        ├── storage_production.go
        ├── store.go
        ├── validation_openapi.go
        ├── worker.go
        └── ...
```

The important rule is:

```txt
pkg/dagflow must remain generic.
app owns business handlers, BCL artifacts, examples, secrets, and business-specific behavior.
```

---

## Quick start

Install dependencies and run:

```bash
go mod tidy
go run .
```

Default development behavior:

```txt
BCL path:        app/bcl
storage:         file-backed dev store
data file:       data/dagflow.json
queue/broker:    in-process memory broker
server address:  from app/bcl/00-server/server.bcl, usually :8080
```

Test the main email workflow route:

```bash
curl -s -X POST localhost:8080/api/email/send \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-secret' \
  -H 'Idempotency-Key: demo-email-1' \
  -d '{"to":"a@b.com","subject":"Hi","body":"Hello"}' | jq
```

Test the interpreter-backed workflow route:

```bash
curl -s -X POST localhost:8080/api/email/script-enrich \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-secret' \
  -d '{"to":"a@b.com","subject":"Hi","body":"Hello"}' | jq
```

Validate config:

```bash
go run . validate app/bcl
```

Generate workflow SVG from the CLI:

```bash
go run . graph app/bcl email_flow > email_flow.svg
```

Show metadata from the CLI:

```bash
go run . metadata app/bcl
```

Generate OpenAPI from the CLI:

```bash
go run . openapi app/bcl > openapi.json
```

---

## Production start with Postgres

For production, use Postgres storage and a durable queue.

```bash
export DAGFLOW_ENV=production
export DAGFLOW_STORE=postgres
export DAGFLOW_POSTGRES_DSN='postgres://dagflow:dagflow@localhost:5432/dagflow?sslmode=disable'
export DAGFLOW_ADMIN_TOKEN='change-this-admin-token'
export DAGFLOW_SIGNING_SECRET='change-this-long-random-signing-secret'
export DAGFLOW_WORKER_ID='worker-1'

go run .
```

`PostgresStorage` should run migrations automatically when `Migrate` is called during startup.

Production mode should fail fast if these are missing:

```txt
DAGFLOW_POSTGRES_DSN
DAGFLOW_ADMIN_TOKEN
DAGFLOW_SIGNING_SECRET
```

Use file storage only for development, tests, and demos.

---

## Configuration model

DAGFlow is driven by BCL files. Supported top-level blocks include:

```hcl
server { ... }

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

The runtime loads all BCL fragments first, merges them, validates the complete model, then starts serving traffic. This avoids startup races where a route validates before the workflow or schema it references has been loaded.

---

## BCL directory loading

`LoadBCLDir` should:

1. walk the BCL directory recursively,
2. collect `.bcl` and `.hcl` files,
3. sort files by path,
4. decode each file independently,
5. merge fragments deterministically,
6. validate after everything is registered.

Recommended BCL layout:

```txt
app/bcl/
├── 00-server/
│   └── server.bcl
├── 10-security/
│   └── middleware.bcl
├── 15-schemas.bcl
├── 20-workflows/
│   ├── batch_uppercase.bcl
│   ├── chains.bcl
│   ├── conditions.bcl
│   ├── email.bcl
│   ├── email_approval.bcl
│   ├── parallel_order.bcl
│   └── script_email_enrich.bcl
├── 25-scripts.bcl
└── 30-routes/
    └── routes.bcl
```

Use multiline blocks for nodes, edges, fields, routes, and workflows. Avoid compact one-line blocks for complex objects.

Good:

```hcl
edge "receive_to_validate" {
  from "receive"
  to "validate"
  type simple
}
```

Avoid:

```hcl
edge "receive_to_validate" { from "receive" to "validate" type simple }
```

---

## Route groups and dynamic routes

Route groups support prefix inheritance and middleware inheritance.

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
```

Middleware order:

```txt
global middlewares → parent group middlewares → child group middlewares → route middlewares
```

Supported route path forms:

```txt
/static/path
/users/:id
/users/{id}
/files/*path
```

Route matching priority:

```txt
static > dynamic parameter > wildcard
```

Route validation should reject duplicate or ambiguous normalized route shapes.

Examples:

```hcl
route "order_check" {
  method "POST"
  path "/orders/:order_id/check"
  workflow "parallel_order_checks"
  mode sync
}

route "file_upload" {
  method "POST"
  path "/files/*path"
  workflow "batch_uppercase"
  mode sync
}
```

Route params are exposed in the execution context:

```txt
route.params.order_id
request.path
request.method
request.query
request.headers
request.body
```

---

## Middlewares

Middlewares may be global, group-level, or route-level.

Example:

```hcl
global_middlewares ["recover", "request_id", "logger"]

middleware "api_key" {
  type "api_key"
  header "X-API-Key"
  value "dev-secret"
}

middleware "small_body" {
  type "body_limit"
  max_bytes 1048576
}

middleware "email_rate_limit" {
  type "rate_limit"
  limit 60
  window "1m"
}
```

A route inherits middlewares from:

```txt
global → route group ancestors → route
```

Production applications should add their own authentication, authorization, tenant, and audit middlewares in `app`.

---

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

Recommended workflow fields:

```txt
name
version
first
max_visits
timeout
debug
failure_policy
```

The runtime should snapshot workflow versions using:

```txt
workflow_id
workflow_version
definition_hash
```

Tasks should always resume using the workflow version/hash they started with.

---

## Nodes

Supported node types:

```txt
function   Go handler from registry
script     interpreter-backed runtime script
workflow   reusable workflow-as-node
page       pause/wait node
join       synchronization point
noop       pass-through
```

Supported execution modes:

```txt
inline       execute in current workflow execution
background  execute through local background queue
distributed execute through broker interface; PostgresBroker gives durable queue
```

Example function node:

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

Example nested workflow node:

```hcl
node "audit" {
  type workflow
  workflow "audit_flow"
  mode inline
}
```

Example pause/page node:

```hcl
node "approval" {
  type page
  pause true
}
```

---

## Edges

Supported edge types:

```txt
simple       one source to one target
branch       condition-selected targets
iterator     iterate slice/array input
fanout       copy result to many targets
fanin        collect many source results into one target
parallel     run targets concurrently
join         synchronization point / alias
race         first successful target wins
error        route failure payload
fallback     fallback path
retry        retry path
timeout      timeout path
compensate   compensation/saga path
```

### Simple

```hcl
edge "receive_to_validate" {
  from "receive"
  to "validate"
  type simple
}
```

### Branch

```hcl
edge "valid_to_send" {
  from "validate"
  to "send"
  type branch
  condition "is_email_valid"
}

edge "invalid_to_store" {
  from "validate"
  to "store_invalid"
  type branch
  condition "is_email_invalid"
}
```

### Iterator

```hcl
edge "each" {
  from "items"
  to "upper"
  type iterator
}
```

### FanOut

```hcl
edge "send_fanout" {
  from "send"
  targets ["store_valid", "audit"]
  type fanout
}
```

### FanIn

```hcl
edge "valid_join" {
  sources ["store_valid", "audit"]
  to "notify_success"
  type fanin
  strategy "all"
}
```

Supported FanIn strategies should include:

```txt
all
any
quorum
first_success
first_completed
timeout_then_continue
timeout_then_fail
```

### Parallel

```hcl
edge "parallel_checks" {
  from "receive"
  targets ["check_inventory", "check_payment", "check_fraud"]
  type parallel
  max_concurrency 3
  fail_fast true
}
```

### Race

```hcl
edge "send_fastest" {
  from "prepare"
  targets ["send_provider_a", "send_provider_b", "send_provider_c"]
  type race
  cancel_losers true
}
```

### Error

```hcl
edge "send_error" {
  from "send"
  to "store_failed"
  type error
}
```

Error edges receive an error payload containing the failing node, input, error message, attempt, and task context.

---

## Conditions

Conditions are reusable blocks or inline expressions evaluated with `github.com/oarkflow/bcl`.

```hcl
condition "is_email_valid" {
  all [
    `result.valid == true`,
    `result.request.to != ""`
  ]
}

condition "is_email_invalid" {
  all [
    `result.valid == false`
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

Inline condition:

```hcl
edge "approval_to_send" {
  from "approval"
  to "send"
  type branch
  when `result.approved == true`
}
```

Facts commonly available to conditions:

```txt
input
result
error
task
node
nodes
workflow
request
route.params
vars
metadata
```

---

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
  name "Script Email Enrichment"
  version "1.0.0"
  first "enrich"
  max_visits 32

  node "enrich" {
    type script
    handler "script_enrich_email"
    last true
  }
}
```

The interpreter receives facts such as:

```txt
input
params
task
result
nodes
workflow
node
route
request
```

Production script safety requirements:

```txt
always set node timeout
register only trusted scripts
avoid exposing dangerous helpers
validate script output
redact secrets from script facts
limit loops/recursion when supported
```

---

## Workflow chains

Chains execute workflows one after another.

```hcl
chain "email_api_chain" {
  name "Email then Audit Chain"
  workflows ["email_flow", "audit_flow"]
}
```

Route using a chain:

```hcl
route "send_email_chain_api" {
  method "POST"
  path "/send-and-audit"
  chain "email_api_chain"
  mode sync
}
```

---

## Storage

The core exposes storage interfaces so applications can implement their own backends.

Development mode uses file-backed storage. Production should use Postgres.

Important interface shape:

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

type Storage interface {
    ExtendedStore
    Migrate(context.Context) error
    Health(context.Context) error
    Close() error
}
```

Postgres storage should include tables for:

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

Applications can implement another store by satisfying the interfaces in `pkg/dagflow`.

---

## Durable queue and workers

Distributed/background nodes should use a broker abstraction.

Required queue behavior:

```txt
enqueue
claim with lease
ack
nack
retry later
dead-letter after max attempts
visibility timeout
priority
delayed jobs
worker heartbeat
stale lease recovery
```

Worker identity is controlled by:

```bash
export DAGFLOW_WORKER_ID='worker-1'
```

Leases prevent duplicate long-running execution. If a worker dies, the lease recovery process can make the job claimable again.

---

## Idempotency and deduplication

Route-level idempotency uses the `Idempotency-Key` header.

```bash
curl -s -X POST localhost:8080/api/email/send \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-secret' \
  -H 'Idempotency-Key: email-demo-1' \
  -d '{"to":"a@b.com","subject":"Hi","body":"Hello"}' | jq
```

Expected behavior:

```txt
same key + same input      return previous result
same key + different input reject
```

Node-level deduplication should use:

```txt
workflow_id + workflow_version + task_id + node_id + input_hash
```

This prevents duplicate side effects during retry/recovery.

---

## DLQ and outbox

Dead letter queue records failed jobs after retry exhaustion.

DLQ operations:

```http
GET  /ops/dlq
POST /ops/dlq/{id}/replay
POST /ops/dlq/{id}/discard
```

Outbox records reliable external event publishing.

Outbox operation pattern:

```txt
node writes business state + outbox event in same transaction
outbox worker publishes event
outbox worker marks event delivered
failed publish retries or moves to DLQ
```

Useful for:

```txt
webhooks
email events
domain events
audit streams
integration messages
```

---

## Task operations

Tasks support operational control:

```txt
pause
resume
cancel
restart
continue
retry failed node
skip failed node
continue with manual result
route to error edge
```

Pause/resume example:

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

Continue after error example:

```bash
curl -s -X POST localhost:8080/ops/tasks/$TASK/continue \
  -H 'X-Admin-Token: dev-admin' \
  -H 'Content-Type: application/json' \
  -d '{"strategy":"retry_failed"}' | jq
```

---

## Operations API

Public endpoints:

```http
GET /openapi.json
GET /metrics
```

Admin endpoints, guarded by `DAGFLOW_ADMIN_TOKEN` when set:

```http
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
GET  /ops/chains
```

Examples:

```bash
curl -s -H 'X-Admin-Token: dev-admin' localhost:8080/ops/metadata | jq
curl -s -H 'X-Admin-Token: dev-admin' localhost:8080/ops/workflows | jq
curl -s -H 'X-Admin-Token: dev-admin' localhost:8080/ops/workflows/email_flow/graph.svg > email.svg
curl -s -H 'X-Admin-Token: dev-admin' localhost:8080/ops/tasks/<task_id>/audit | jq
curl -s -H 'X-Admin-Token: dev-admin' localhost:8080/ops/tasks/<task_id>/graph.svg > task.svg
```

---

## Metadata and graph APIs

Workflow metadata:

```http
GET /ops/workflows
GET /ops/workflows/{id}
GET /ops/workflows/{id}/metadata
GET /ops/workflows/{id}/versions
```

Task metadata:

```http
GET /ops/tasks
GET /ops/tasks/{id}
GET /ops/tasks/{id}/audit
```

Definition graph:

```http
GET /ops/workflows/{id}/graph.svg
```

Execution graph:

```http
GET /ops/tasks/{id}/graph.svg
```

Runtime graph should show:

```txt
pending
running
completed
failed
waiting
skipped
retrying
cancelled
compensated
```

---

## OpenAPI and schemas

Schemas are defined in BCL.

```hcl
schema "EmailRequest" {
  type object
  required ["to", "subject", "body"]

  field "to" {
    type string
    required true
    format "email"
  }

  field "subject" {
    type string
    required true
  }

  field "body" {
    type string
    required true
  }
}
```

Routes may reference schemas:

```hcl
route "send_email" {
  method "POST"
  path "/send"
  workflow "email_flow"
  mode sync
  input_schema "EmailRequest"
  output_schema "EmailResponse"
}
```

OpenAPI endpoint:

```http
GET /openapi.json
```

CLI generation:

```bash
go run . openapi app/bcl > openapi.json
```

---

## Metrics

Metrics endpoint:

```bash
curl -s localhost:8080/metrics | jq
```

Metrics should include:

```txt
workflow_started_total
workflow_completed_total
workflow_failed_total
node_started_total
node_completed_total
node_failed_total
idempotency_hit_total
retry_total
timeout_total
dlq_total
lease_expired_total
queue_depth
```

---

## CLI commands

Supported commands should include:

```bash
go run . serve
go run . validate app/bcl
go run . metadata app/bcl
go run . openapi app/bcl > openapi.json
go run . graph app/bcl email_flow > email.svg
go run . inspect <task_id>
```

If no command is given, the application should default to `serve`.

---

## Adding app handlers

App-specific handlers belong in `app/handlers.go`.

```go
package app

import "your/module/pkg/dagflow"

func RegisterHandlers(e *dagflow.Engine) {
    e.Register("create_order", func(ctx *dagflow.ExecutionContext, input any) (any, error) {
        return map[string]any{
            "created": true,
            "input": input,
        }, nil
    })
}
```

The core package should expose:

```go
e.Register("handler_name", handler)
e.RegisterHandlers(map[string]dagflow.Handler{...})
e.RegisterScript("script_name", source)
e.HandlerNames()
e.ScriptNames()
```

Registered handler and script names are visible through:

```http
GET /ops/metadata
```

---

## Embedding the core in another app

```go
package main

import (
    "context"
    "log"

    "your/module/pkg/dagflow"
)

func main() {
    err := dagflow.RunServer(context.Background(), dagflow.ServerOptions{
        BCLPath: "app/bcl",
        Register: func(e *dagflow.Engine) {
            e.Register("my_handler", func(ctx *dagflow.ExecutionContext, input any) (any, error) {
                return input, nil
            })
        },
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

---

## Security

Implemented or expected security controls:

```txt
opsGuard for operations APIs
DAGFLOW_ADMIN_TOKEN required in production
API key middleware
body size middleware
signed resume tokens with HMAC
secret redaction for audit/log payloads
route/group/global middleware layering
schema validation hooks
admin operation audit
```

Production recommendations:

```txt
set DAGFLOW_ENV=production
set DAGFLOW_STORE=postgres
set DAGFLOW_ADMIN_TOKEN
set DAGFLOW_SIGNING_SECRET
run behind TLS
place /ops behind firewall or private network
rotate API keys/admin token
use tenant/user-aware middlewares in app
register only trusted interpreter scripts
avoid exposing secrets to scripts
monitor DLQ/outbox/leases
backup Postgres
```

---

## Production checklist

Before production traffic:

```bash
go run . validate app/bcl
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

Operational checklist:

```txt
Use Postgres storage
Enable durable queue
Use unique worker IDs
Set node timeouts
Set retry policies
Use Idempotency-Key on side-effecting routes
Keep handlers idempotent
Monitor DLQ
Monitor outbox
Monitor leases
Review BCL changes
Version BCL files
Restrict /ops access
Back up Postgres
Run race tests before release
```

---

## Troubleshooting

### `workflow script_email_enrich not found`

This is usually caused by BCL loading or layout mismatch.

Expected file:

```txt
app/bcl/20-workflows/script_email_enrich.bcl
```

Expected route:

```hcl
route "script_enrich_api" {
  method "POST"
  path "/script-enrich"
  workflow "script_email_enrich"
  mode sync
}
```

Run:

```bash
go run . validate app/bcl
```

### `node script_email_enrich.enrich references missing input schema EmailRequest`

This means the validator sees the node schema reference before the schema registry contains `EmailRequest`, or the schema file is missing/misdecoded.

Fixes:

```txt
make sure app/bcl/15-schemas.bcl exists
make sure BCL loader merges all files before validating
make sure schema "EmailRequest" is decoded into the global schema registry
avoid node-level schema references until loader validation is verified
```

A safe script node is:

```hcl
node "enrich" {
  type script
  handler "script_enrich_email"
  last true
}
```

The route can still reference schemas for OpenAPI:

```hcl
input_schema "EmailRequest"
output_schema "EmailResponse"
```

### `edge <id> requires from/to or sources/targets`

Use multiline BCL blocks.

Good:

```hcl
edge "receive_to_validate" {
  from "receive"
  to "validate"
  type simple
}
```

### Route does not match

Check route group prefixes. A route inside:

```hcl
route_group "api" {
  prefix "/api"

  route_group "email" {
    prefix "/email"

    route "send" {
      path "/send"
    }
  }
}
```

becomes:

```txt
/api/email/send
```

### Postgres connection fails

Check:

```bash
echo $DAGFLOW_POSTGRES_DSN
psql "$DAGFLOW_POSTGRES_DSN" -c 'select 1'
```

### Missing interpreter module

Run:

```bash
go mod tidy
```

The project imports `github.com/oarkflow/interpreter`.

---

## Design guarantees

The refactor is safe because:

```txt
pkg/dagflow does not import app
pkg/dagflow has no email/order-specific structs
app registers business handlers
app owns BCL artifacts
main.go only starts app.Run()
```

This lets the same core support different business requirements through Go handlers, BCL workflows, route definitions, schemas, middlewares, conditions, and interpreter scripts.

---

## Notes on production readiness

This runtime includes the production primitives expected from a workflow orchestration server:

```txt
Postgres storage
durable queue hooks
leases
heartbeats
DLQ
outbox
idempotency
node deduplication
validation
signed resume tokens
metadata APIs
graph APIs
OpenAPI generation
guarded operations APIs
```

For regulated, money-moving, or highly critical systems, also implement application-specific:

```txt
external provider idempotency keys
audit retention policy
tenant isolation
RBAC permission model
PII redaction rules
backup/restore policy
disaster recovery plan
SLO dashboards
```

---
