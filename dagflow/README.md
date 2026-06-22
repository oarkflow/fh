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

---

## Enterprise data handling layer

This build adds a generic data-handling layer to `pkg/dagflow` without moving business behavior into the core. Data handling can now be applied at these boundaries:

- route input before schema validation and workflow dispatch,
- workflow input and workflow output,
- node input before handler/script/workflow execution,
- node output after execution and before persistence/edge routing,
- edge handoff for simple, branch, fanout, iterator, error, fallback, timeout, retry, and compensation edges,
- standalone node execution,
- chain execution through normal workflow input/output boundaries.

The engine keeps data operations generic. Business applications can register data sources with:

```go
engine.RegisterDataSource("service:tenant_config", func(ctx context.Context, dc *dagflow.DataContext, key string) (any, error) {
    return map[string]any{"tenant":"demo", "plan":"enterprise", "key":key}, nil
})

engine.RegisterDataSource("integration:crm", func(ctx context.Context, dc *dagflow.DataContext, key string) (any, error) {
    return map[string]any{"external_id":"crm-123", "key":key}, nil
})
```

The bundled example app registers two concrete example sources: `service:tenant_config` and `integration:crm`.

### Data sources available to rules

Data rules can read from:

```txt
input                  current payload at the boundary
result                 current payload/result at the boundary
request.body           HTTP request body when used on routes
request.path           HTTP path parameters
request.query          HTTP query parameters
request.headers        HTTP headers
request.method         HTTP method
request.client_ip      resolved client IP
task.input             original task input
task.last_result       latest task result
task.results           results by node id
workflow.id/version    current workflow metadata
node.id/type/handler   current node metadata
node.params            node params
edge.id/from/to/type    current edge metadata
env.NAME or $env.NAME  environment variables
service.NAME:key       registered service data source
integration.NAME:key   registered integration data source
```

### Data operations

`DataSpec` supports real operations, not stubs:

```txt
source       replace the current payload with a source path or expression
pick         keep only selected paths
omit         remove paths
rename       move one path to another path
map          set target paths from source paths or expressions
set          set literal, env, service, integration, or expression values
defaults     set values only when the target path is missing
env          set fields from environment variables
services     set fields from registered service data sources
integrations set fields from registered integration data sources
transform    upper/lower/trim/prefix/suffix/replace/int/float/bool/json/string/expr
filter       keep/drop payloads using BCL boolean expressions
append       append a value or list into a list field
prepend      prepend a value or list into a list field
flatten      flatten a one-level nested list field
strict       fail when expected source paths are missing
```

### BCL examples

Route-level request ETL (route `data {}` defaults to source `request`, so `source "request"` may be omitted):

```hcl
route "send_email_normalized" {
  method "POST"
  path "/send-normalized"
  workflow "email_flow"
  mode sync
  envelope true

  data {
    map {
      to      "body.to"
      subject "body.subject"
      body    "body.body"
      ip      "client_ip"
    }
    env {
      app_env "DAGFLOW_ENV"
    }
    services {
      tenant "tenant_config:default"
    }
    transform {
      field "subject"
      op trim
    }
    filter {
      expr `input.to != "" && input.subject != "" && input.body != ""`
    }
  }
}
```

Node input and output transformation:

```hcl
node "validate" {
  type function
  handler "validate_email"

  input_data {
    defaults {
      subject "Untitled"
    }
    transform {
      field "to"
      op lower
    }
  }

  output_data {
    map {
      request "input.request"
      valid   "input.valid"
      reason  "input.reason"
    }
    set {
      checked_by "dagflow"
    }
  }
}
```

Edge-level mapping, filtering, and ETL:

```hcl
edge "valid_to_send" {
  from "validate"
  to "send"
  type branch
  condition "is_email_valid"

  data {
    source "result"
    filter {
      expr `input.valid == true`
    }
    map {
      to      "request.to"
      subject "request.subject"
      body    "request.body"
    }
    integrations {
      crm "crm:contact"
    }
  }
}
```

Iterator ETL:

```hcl
edge "each" {
  from "items"
  to "upper"
  type iterator

  data {
    source "result.items"
    filter {
      expr `len(input) > 0`
    }
  }
}
```

The old `edge.map` field remains supported for backward compatibility. If an edge has no `data {}` block but has `map`, the engine treats it as `data.map`.


### Runnable cURL examples for the updated app BCL

Start the server:

```bash
export DAGFLOW_ENV=dev
# from the extracted project root
go run . serve
```

All `/api/*` routes require the demo API key:

```bash
export API='http://localhost:8080'
export KEY='X-API-Key: dev-secret'
```

#### 1) Sync email workflow: route data mapping → workflow input transforms → branch → fanout/fanin

This exercises route `data { ... }` with the default request source, workflow `input_data`, node `input_data/output_data`, branch edge filters, fanout edge data, fanin edge data, service source, integration source, and environment source.

```bash
curl -s -X POST "$API/api/email/send" \
  -H "$KEY" \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: req-demo-001' \
  -d '{
    "to": "ALICE@EXAMPLE.COM",
    "subject": "  Hello from DAGFlow data ETL  ",
    "body": " This message is normalized before execution. "
  }' | jq
```

Expected: HTTP `200` with only the business JSON response object. It must not include `node_states`, `node_results`, `audit`, cursor, visits, or workflow execution internals. Use `/ops/tasks/<task_id>/debug` or `/ops/tasks/<task_id>/activities` for operational details.

#### 2) Invalid email branch: route mapping → validation branch → invalid store → failure notification

```bash
curl -s -X POST "$API/api/email/send" \
  -H "$KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "to": "not-an-email",
    "subject": "Rejected example",
    "body": "This should go to the invalid branch."
  }' | jq
```

Expected: HTTP `200` with a clean failure-notification business object. The invalid branch stores the rejected message and returns the response object only; internal validation/node logs remain available through ops activity endpoints.

#### 3) Async email workflow with transformed request data

```bash
curl -s -X POST "$API/api/email/send/async" \
  -H "$KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "to": "ASYNC@EXAMPLE.COM",
    "subject": "Async ETL",
    "body": "Async route still maps and transforms data."
  }' | jq
```

Expected: HTTP `202` with a small receipt containing `task_id`, `workflow_id`, `status`, `status_url`, and `audit_url`, not the full task internals. Use the returned `task_id` with the ops task endpoint.

#### 4) Script enrichment route: route transform + script node input/output data

```bash
curl -s -X POST "$API/api/email/script-enrich?source=docs" \
  -H "$KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "to": "SCRIPT@EXAMPLE.COM",
    "subject": "  Script subject  ",
    "body": "Script body"
  }' | jq
```

Expected: subject is prefixed at the route, then prefixed again by the script; output is a JSON object, not a string, and includes `enriched_by`, `app_env`, and `tenant_config`.

#### 5) Chain route: one normalized payload through `email_flow` then `audit_flow`

```bash
curl -s -X POST "$API/api/email/send-and-audit?source=curl" \
  -H "$KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "to": "CHAIN@EXAMPLE.COM",
    "subject": "Chain ETL",
    "body": "Email result becomes audit input."
  }' | jq
```

Expected: a clean final chain result object. The chain run and child task details are stored for operations but are not returned by the public API route.

#### 6) Inline workflow reuse route: workflow node data mapping between sub-workflows

```bash
curl -s -X POST "$API/api/email/send-inline-reuse" \
  -H "$KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "to": "INLINE@EXAMPLE.COM",
    "subject": "Inline workflow reuse",
    "body": "The email workflow output is mapped into audit workflow input."
  }' | jq
```

Expected: a clean final object from the parent workflow. The workflow-node execution details remain in the task activity/debug endpoints.

#### 7) Data ETL demo: body + path params + query + headers + env + service + integration

This is the clearest end-to-end data-handling example.

```bash
curl -s -X POST "$API/api/data/acct_123/etl?source=partner" \
  -H "$KEY" \
  -H 'Content-Type: application/json' \
  -H 'X-Trace-ID: trace-789' \
  -d '{
    "user": {
      "name": "  Jane Doe  ",
      "email": "JANE@EXAMPLE.COM",
      "role": "manager"
    },
    "tags": ["new", "priority"]
  }' | jq
```

Expected: the workflow maps `body.user.*`, `path.account_id`, `query.source`, `headers.X-Trace-ID`, and `client_ip`; applies defaults; lowercases email; trims name; appends/prepends tags; adds service/integration/env values; and returns an `api_response` shape.

#### 8) Parallel order checks: route path/query/body/header mapping → parallel fanout → fanin approval

```bash
curl -s -X POST "$API/api/orders/order_1001/check" \
  -H "$KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: idem-order-1001' \
  -d '{
    "customer_id": "cust_99",
    "amount": 149.95,
    "currency": "usd"
  }' | jq
```

Expected: a clean approval result object. `receive` normalizes route/body/header data, the three checks run in parallel, and `approve` receives the joined result; per-node details remain in `/ops/tasks/<task_id>/debug`.

#### 9) Iterator data handling: list extraction from request envelope → per-item transform

```bash
curl -s -X POST "$API/api/files/uploads/demo.txt?source=curl" \
  -H "$KEY" \
  -H 'Content-Type: application/json' \
  -d '{"items": [" alpha ", "beta", "Gamma"]}' | jq
```

Expected: route data extracts `body.items` and wildcard path, iterator edge emits each item to `upper`, and node transforms produce uppercase values.

#### 10) Route data filter failure

```bash
curl -i -s -X POST "$API/api/email/send" \
  -H "$KEY" \
  -H 'Content-Type: application/json' \
  -d '{"to": "", "subject": "", "body": "filtered"}'
```

Expected: HTTP `422` with `request filtered by route data policy` because the route `filter` rejects empty `to`/`subject` before the workflow starts.


### Data failure behavior

- Missing paths are ignored by default so existing workflows do not break.
- `strict true` turns missing paths into errors.
- A filter returning false produces `ErrDataFiltered`; routes return `422`, nodes are marked skipped, and edges simply do not emit a run item.
- Public `/api/*` responses return only the route/workflow business result. Transformed inputs/results, node state, node results, retries, waits, and audit events are stored as operational activity data and exposed through `/ops/tasks/:id/debug`, `/ops/tasks/:id/activities`, `/ops/tasks/:id/summary`, and graph/status APIs.


### Public response vs operational activity separation

DAGFlow separates **business response data** from **workflow execution activity**:

- Public BCL routes return the final business result as a JSON object or an array of objects. If a handler accidentally returns a scalar, null, or an interpreter-inspected object string, DAGFlow normalizes or wraps it before writing the HTTP response. They do not return `Task`, `NodeState`, `NodeResults`, `Audit`, cursor, visit counters, or internal workflow logs.
- Async routes return a compact receipt containing `task_id`, `workflow_id`, `status`, `status_url`, and `audit_url`.
- Operational activity is persisted on the task and logged through the background activity logger. Use ops endpoints for inspection:

```bash
curl -s -H 'X-Admin-Token: dev-admin' "$API/ops/tasks" | jq
curl -s -H 'X-Admin-Token: dev-admin' "$API/ops/tasks/<task_id>" | jq
curl -s -H 'X-Admin-Token: dev-admin' "$API/ops/tasks/<task_id>/summary" | jq
curl -s -H 'X-Admin-Token: dev-admin' "$API/ops/tasks/<task_id>/activities" | jq
curl -s -H 'X-Admin-Token: dev-admin' "$API/ops/tasks/<task_id>/debug" | jq
```

Route-level `response { ... }` can shape final API output and headers independently from route request mapping and workflow/node/edge data handling. Response `data {}` defaults to source `result`, so `source "result"` may be omitted:

```bcl
route "send_email" {
  method "POST"
  path "/send"
  workflow "email_flow"
  mode sync

  data {
    # source defaults to request
    map {
      to "body.to"
      subject "body.subject"
      body "body.body"
    }
  }

  response {
    status 200
    header {
      X_DAGFlow_Route "route.id"       # underscores are emitted as hyphens
      X_DAGFlow_Workflow "route.workflow"
    }
    data {
      omit ["debug", "node_results", "data_context"]
    }
  }
}
```

The legacy `response_data { ... }` block is still accepted for backward compatibility, but new app BCL should use `response { data { ... } header { ... } }`. This keeps API responses stable while preserving full traceability for audit, support, replay, DLQ, debugging, and compliance.


### Runtime configuration validation fixes

The loader now performs deterministic per-file BCL loading plus a schema-block reconciliation pass. This prevents runtime-only failures such as:

```json
{"detail":"schema EmailRequest not found","error":"schema validation failed"}
```

Run validation before serving:

```bash
go run . validate app/bcl
```

The validation command now fails fast for route-level `input_schema` and `output_schema` references, not only node-level schema references. That means a missing `EmailRequest`, `OrderCheckRequest`, or any future route schema is caught at startup instead of during the first request.

The request envelope mapper was also fixed so route data expressions like `body.to`, `path.order_id`, `query.source`, and `headers.X-Request-ID` resolve against the actual HTTP request envelope used by curl examples.

---

## Notifications, filters, and approvals

This build adds first-class workflow and node policies for enterprise notifications, task filtering, rejection, approval, and manual gates. The core remains generic: channels, rules, approval records, and delivery records live in `pkg/dagflow`, while business-specific handlers stay in `app`.

### Notification channels

Notification channels are globally configured in BCL and can be reused by any workflow or node.

```bcl
notification_channel "ops_log" {
  type log
  enabled true
}

notification_channel "ops_webhook" {
  type webhook
  enabled true
  endpoint "https://example.com/dagflow/events"
  secret "change-me"
  retries 3
  timeout "5s"
}
```

Built-in channel types:

- `log`: local structured process log.
- `callback`: custom Go callback channel via `RegisterNotificationHandler`.
- `webhook`: signed JSON HTTP delivery with `X-Dagflow-Signature`.
- `email`: SMTP delivery using configured host, port, sender, recipients, and credentials.
- `sms`: generic signed HTTP SMS provider adapter, so the core is vendor-neutral and can work with Twilio-compatible gateways, local SMS gateways, or an internal notification service.

Custom future channels are added without changing workflow execution:

```go
engine.RegisterNotificationHandler("slack", dagflow.NotificationHandlerFunc(
    func(ctx context.Context, ch dagflow.NotificationChannel, msg dagflow.NotificationMessage) error {
        // Deliver to any provider here.
        return nil
    },
))
```

### Workflow and node notification rules

Rules can be attached to a workflow or a specific node. They match lifecycle events, optionally evaluate a BCL condition, optionally reshape data with `data`, and deliver to one or more channels.

```bcl
workflow "notification_approval_demo" {
  first "receive"

  notification "workflow_activity" {
    events ["task.created", "task.completed", "task.failed", "approval.required"]
    channels ["ops_log"]
    title "DAGFlow {{event}}"
    message "workflow={{task.workflow_id}} task={{task.id}} status={{task.status}}"
    severity "info"
  }

  node "send" {
    type function
    handler "send_email"

    notification "send_completed" {
      events ["node.completed"]
      channels ["ops_log"]
      title "Email send node completed"
      message "task={{task.id}} node={{node.id}}"
    }
  }
}
```

Notification deliveries are persisted by stores that implement `NotificationStore`; the memory, file, and Postgres stores now do. Inspect deliveries at:

```bash
curl -s -H 'X-Admin-Token: change-this-admin-token' localhost:8080/ops/notifications | jq
```

### Task rules: filters, rejection, and manual approval

Task rules can be attached to a workflow or node. They run on configured lifecycle events and can notify, reject, auto-approve, pause, cancel, or require manual approval.

```bcl
node "send" {
  type function
  handler "send_email"

  rule "manual_approval_for_sensitive_subject" {
    events ["node.before"]
    when `input.request.subject == "approval" || input.request.subject == "sensitive"`
    message "manual approval required before sending sensitive email"
    action {
      type require_approval
      mode single
      approvers ["ops"]
      reason "sensitive email requires approval"
      channels ["ops_log"]
      timeout "24h"
    }
  }
}
```

Rule action types:

- `notify`: emits a notification only.
- `reject`: fails the task with the configured reason.
- `approve`: records an automatic approval audit event.
- `require_approval`: pauses the task, saves an approval request, and resumes only after approval.
- `pause`: pauses execution.
- `cancel`: cancels execution.

Approval records are persisted by stores that implement `ApprovalStore`; the memory, file, and Postgres stores now do.

### Approval operations

List pending approvals:

```bash
curl -s -H 'X-Admin-Token: change-this-admin-token' \
  'localhost:8080/ops/approvals?status=pending' | jq
```

Approve or reject a single task:

```bash
curl -s -X POST localhost:8080/ops/tasks/<task-id>/approve \
  -H 'Content-Type: application/json' \
  -H 'X-Admin-Token: change-this-admin-token' \
  -d '{"approver":"ops","reason":"approved"}' | jq

curl -s -X POST localhost:8080/ops/tasks/<task-id>/reject \
  -H 'Content-Type: application/json' \
  -H 'X-Admin-Token: change-this-admin-token' \
  -d '{"approver":"ops","reason":"rejected"}' | jq
```

Bulk approve or reject:

```bash
curl -s -X POST localhost:8080/ops/approvals/bulk/approve \
  -H 'Content-Type: application/json' \
  -H 'X-Admin-Token: change-this-admin-token' \
  -d '{"task_ids":["task_1","task_2"],"approver":"ops","reason":"bulk approved"}' | jq
```

### Demo workflow

`app/bcl/20-workflows/notification_approval_demo.bcl` demonstrates workflow-level notifications, node-level notifications, a rule-based rejection, and a manual approval gate. The demo now references named conditions from `app/bcl/20-workflows/conditions.bcl` instead of duplicating inline rule logic, so rules, branches, routes, and middleware all use the same condition engine.

Important named conditions used by this workflow:

```bcl
condition "blocked_recipient_domain" {
  all [
    `node.id == "validate"`,
    `input.to == "blocked@blocked.test"`
  ]
}

condition "sensitive_email_subject" {
  all [`node.id == "send"`]
  any [
    `input.request.subject == "approval"`,
    `input.request.subject == "sensitive"`
  ]
}
```

The blocked-domain reject rule should only fire for `blocked@blocked.test`; normal recipients such as `user@example.com` should pass validation and continue to send/store.

Run a normal request:

```bash
curl -s -X POST localhost:8080/workflow/notification_approval_demo \
  -H 'Content-Type: application/json' \
  -d '{"to":"a@b.com","subject":"Hi","body":"Hello"}' | jq
```

Trigger manual approval:

```bash
curl -s -X POST localhost:8080/workflow/notification_approval_demo \
  -H 'Content-Type: application/json' \
  -d '{"to":"a@b.com","subject":"approval","body":"Please approve"}' | jq
```

Trigger rule rejection:

```bash
curl -s -X POST localhost:8080/workflow/notification_approval_demo \
  -H 'Content-Type: application/json' \
  -d '{"to":"blocked@blocked.test","subject":"Hi","body":"Blocked"}' | jq
```

---

## Managed queues, workflow consumers, and sync/async queue execution

DAGFlow now has a managed broker layer so a workflow can run behind a background queue consumer instead of being executed only from an HTTP request goroutine. The same workflow definition can run inline, async, distributed-node mode, or queue-backed mode.

### Core concepts

- `queue` defines a named broker queue with capacity, retry metadata, visibility timeout, and DLQ name.
- `consumer` binds a queue to a workflow and starts one or more background workers.
- `mode queue` on a BCL route enqueues the request as a workflow task instead of executing it directly.
- `await=true` on a queue route or enqueue API waits for the queue result and returns the completed workflow result.
- Without `await=true`, the route/API returns `202 Accepted` with a task receipt while the consumer processes the task in the background.
- Consumers are runtime-controlled with pause, resume, and stop operations.

### BCL queue configuration

```bcl
queue "email_jobs" {
  capacity 4096
  max_attempts 3
  visibility_timeout "30s"
  dlq "email_jobs_dlq"
}

queue "email_jobs_dlq" {
  capacity 4096
}

consumer "email_jobs_consumer" {
  queue "email_jobs"
  workflow "notification_approval_demo"
  concurrency 4
  enabled true
}
```

### Queue-backed route

```bcl
route "notification_approval_demo_queue" {
  method "POST"
  path "/notification-approval-demo/queue"
  workflow "notification_approval_demo"
  queue "email_jobs"
  mode queue
  input_schema "EmailRequest"
  envelope true

  data {
    map {
      to "body.to"
      subject "body.subject"
      body "body.body"
    }
  }
}
```

### Queue operations API

List queues:

```bash
curl -H "X-API-Key: dev-secret" http://localhost:8080/ops/queues
```

List consumers:

```bash
curl -H "X-API-Key: dev-secret" http://localhost:8080/ops/consumers
```

Pause/resume/stop a consumer:

```bash
curl -X POST -H "X-API-Key: dev-secret" http://localhost:8080/ops/consumers/email_jobs_consumer/pause
curl -X POST -H "X-API-Key: dev-secret" http://localhost:8080/ops/consumers/email_jobs_consumer/resume
curl -X POST -H "X-API-Key: dev-secret" http://localhost:8080/ops/consumers/email_jobs_consumer/stop
```

Enqueue a workflow task through the protected operations API:

```bash
curl -X POST http://localhost:8080/ops/queues/email_jobs/workflows/notification_approval_demo/enqueue \
  -H "X-API-Key: dev-secret" \
  -H "Content-Type: application/json" \
  -d '{"to":"user@example.com","subject":"hello","body":"queued message"}'
```

Wait synchronously for the queued result:

```bash
curl -X POST 'http://localhost:8080/ops/queues/email_jobs/workflows/notification_approval_demo/enqueue?await=true' \
  -H "X-API-Key: dev-secret" \
  -H "Content-Type: application/json" \
  -d '{"to":"user@example.com","subject":"hello","body":"queued message"}'
```

Use the public BCL route in async queue mode:

```bash
curl -X POST http://localhost:8080/api/email/notification-approval-demo/queue \
  -H "X-API-Key: dev-secret" \
  -H "Content-Type: application/json" \
  -d '{"to":"user@example.com","subject":"hello","body":"queued message"}'
```

Use the same route synchronously through the queue:

```bash
curl -X POST 'http://localhost:8080/api/email/notification-approval-demo/queue?await=true' \
  -H "X-API-Key: dev-secret" \
  -H "Content-Type: application/json" \
  -d '{"to":"user@example.com","subject":"hello","body":"queued message"}'
```

### Production storage behavior

With the default memory runtime, queue state is in-process and suitable for local development and tests. With `DAGFLOW_STORE=postgres`, jobs are stored in `dagflow_jobs` with queue name, status, attempts, visibility timestamp, lease owner, result, and error data. Postgres consumers claim jobs using `FOR UPDATE SKIP LOCKED`, heartbeat through the existing lease model, retry failed jobs, and recover expired running jobs back to retry state.

Existing workflow features continue to work with queue-backed execution: notification rules, task filters, manual approvals, node stats, task audit logs, idempotency, distributed node execution, retries, DLQ recording, route data mapping, schemas, and response shaping.

## Enterprise hardening update

This build adds the next production-hardening layer without removing the previous workflow, notification, approval, queue, BCL, and ops features.

### Queue and consumer fixes

- Inline BCL expressions are normalized before evaluation, fixing failures such as `unexpected expression token "\\n"` for backtick/raw-string `when` rules.
- Managed queue consumers now recover from handler panics and convert panics into failed job results instead of killing the consumer goroutine.
- Queue retry behavior no longer prematurely completes awaited jobs on the first failed attempt. Failed jobs are retried with backoff until `max_attempts` is reached.
- Memory and Postgres brokers now honor queue/job max-attempt selection consistently.
- Terminally failed jobs can be copied into the configured DLQ queue.
- Local background-node execution starts lazily when the engine is used without the HTTP server bootstrap.
- Async workflows detach from the request cancellation context while still using engine panic recovery.

### Security and operations

- Production mode now refuses to start unless `DAGFLOW_SIGNING_SECRET` and `DAGFLOW_ADMIN_TOKEN` are configured.
- Added health endpoints:
  - `GET /health/live`
  - `GET /health/ready`
  - `GET /health/startup`
- Non-log notification delivery runs outside the critical workflow path and is protected by engine panic recovery.
- Approval resume is protected against repeatedly triggering the same `require_approval` rule after it has already been approved.

### Queue example

```bash
curl -X POST 'http://localhost:8080/api/email/notification-approval-demo/queue?await=true' \
  -H 'Content-Type: application/json' \
  -d '{"to":"user@example.com","subject":"hello","message":"queued email"}'
```

Consumer controls:

```bash
curl -X POST http://localhost:8080/ops/consumers/email_jobs_consumer/pause
curl -X POST http://localhost:8080/ops/consumers/email_jobs_consumer/resume
curl -X POST http://localhost:8080/ops/consumers/email_jobs_consumer/stop
```

## Queue and background transparency

The runtime now exposes queue, broker, consumer, and workflow-processing transparency by default. Every queue and consumer lifecycle transition is logged with a `dagflow broker` prefix, including queue creation, subscription, consumer start/stop/pause/resume, heartbeat, job publish, job pickup, job retry, job ack/nack, terminal failure, DLQ copy, and result completion.

Useful operations endpoints:

```bash
# Full runtime picture: workflows, tasks by status, queues, consumers, metrics, health, warnings, recent broker events
curl -H "X-API-Key: dev-secret" http://localhost:8080/ops/diagnostics

# Broker-focused picture: queue stats, consumers, recent broker events
curl -H "X-API-Key: dev-secret" http://localhost:8080/ops/broker

# Recent queue/consumer/job timeline
curl -H "X-API-Key: dev-secret" 'http://localhost:8080/ops/broker/events?limit=200'

# Queue and consumer summaries
curl -H "X-API-Key: dev-secret" http://localhost:8080/ops/queues
curl -H "X-API-Key: dev-secret" http://localhost:8080/ops/consumers
```

When an enqueued task is not moving, check `/ops/diagnostics` first. It reports warnings such as `queue email_jobs has depth 3 but no running consumer` and stale consumer heartbeats. `/ops/broker/events` shows the exact timeline: publish → subscribe → consumer heartbeat → job picked → workflow started → job completed or retry scheduled.

Expected log examples:

```text
dagflow engine starting workflows=... queues=... consumers=...
dagflow queue configured id=email_jobs capacity=4096 max_attempts=3 dlq=email_jobs_dlq
dagflow consumer starting id=email_jobs_consumer queue=email_jobs workflow=notification_approval_demo concurrency=4
dagflow broker component=memory_broker event=consumer.started queue=email_jobs consumer=email_jobs_consumer ...
dagflow queue enqueue accepted workflow=notification_approval_demo queue=email_jobs task=... job=...
dagflow broker component=memory_broker event=consumer.job.started queue=email_jobs consumer=email_jobs_consumer job=...
dagflow queue workflow job starting queue=email_jobs job=... task=... workflow=notification_approval_demo attempt=1
dagflow queue workflow job completed queue=email_jobs job=... task=... workflow=notification_approval_demo status=completed
```

## Queue/rule hardening notes

This build includes defensive fixes for queue-driven workflow execution and task-rule evaluation:

- Task rules now evaluate `when` / `condition` with JSON-normalized facts so expressions like `input.to` and `input.request.subject` work even when Go handlers return structs.
- Parsed/stringified BCL block values are normalized back to their raw backtick expression when possible, preventing accidental evaluation of values like `[map[body:map[...] id:\`...\`]]`.
- Control actions (`reject`, `approve`, `require_approval`, `pause`, `cancel`) require an explicit `when` or named `condition`; use `when \`true\`` for intentional unconditional control rules.
- Business rejections are terminal outcomes and are no longer retried by the in-memory queue broker.
- Background queue panics now emit stack traces in broker diagnostics and logs.
- Task runtime maps are initialized after load/clone and before execution to prevent nil-map panics from deserialized tasks.
