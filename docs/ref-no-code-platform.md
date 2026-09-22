# The `ref` no-code platform

An application on this platform is a BCL document. The document declares its
connections, its identity model, its request graphs, its durable processes and its
HTTP surface; the host program opens the document, mounts what it declares and
listens. There is no generated code and no plugin build step, and the host process
contains no business logic:

```go
app, err := platform.LoadFile(ctx, "app.bcl", platform.DefaultLoadOptions())
if err != nil {
    log.Fatal(err)
}
defer app.Close()

server := fh.NewFast()
if err := app.Mount(server); err != nil {
    log.Fatal(err)
}
log.Fatal(server.Listen(":8089"))
```

The working proof is [`examples/ref-platform`](../examples/ref-platform): an
order-fulfilment application with registration, RBAC, cached catalogue reads,
stock reservation under a cross-replica lock, a human approval above a threshold
with a four-eyes control, payment with retry and a saga refund, concurrent
fulfilment and notification joined before invoicing, a background worker, a cron
schedule and a signed payment webhook. All of it configuration.

What the platform gives you that a general workflow engine does not is *proof at
load time*. A misspelled action, a fact nobody produces, two nodes claiming one
fact, an unreachable process step, a missing secret, an empty authorization gate,
a durable node in a request graph — each is a startup failure with a message
naming the node, not a 500 on the first request that happened to take that path.

---

## Contents

- [Two tiers, and why](#two-tiers-and-why)
- [The document](#the-document)
- [Tier 1: intents](#tier-1-intents)
- [Tier 2: processes](#tier-2-processes)
- [Routes and guards](#routes-and-guards)
- [Workers, schedules, triggers](#workers-schedules-triggers)
- [The expression language](#the-expression-language)
- [The data pipeline](#the-data-pipeline)
- [Resource catalog](#resource-catalog)
- [Node families](#node-families)
- [Edge types](#edge-types)
- [Actions](#actions)
- [The driver SPI](#the-driver-spi)
- [Authoring constraints in BCL](#authoring-constraints-in-bcl)
- [Operating it](#operating-it)
- [The trust boundary](#the-trust-boundary)
- [What it deliberately does not do](#what-it-deliberately-does-not-do)

---

## Two tiers, and why

The platform has two execution tiers because request work and long-running work
have genuinely different correctness requirements, and a single engine that tries
to be both is worse at each.

**Tier 1 — the intent.** A REF fact-DAG, scoped to one request. Nodes declare the
facts they require and provide; the compiler proves every required fact has exactly
one producer and rejects cycles. *Every node in a compiled plan runs.* There is no
traversal and no skipping: execution is gated only by deny-dominant policy
decisions and by speculation class, so a decision node's deny blocks every effect
in the graph regardless of how many allows were recorded. Nothing is persisted, and
the whole invocation lives and dies with the request.

Because every node runs, control flow cannot be expressed as an edge you don't
take. It lives *inside* a node instead: the `flow.*` actions invoke **child
intents** through the engine, so the untaken branch is never part of the caller's
plan at all. `flow.branch`, `flow.foreach`, `flow.race` and the rest are ordinary
nodes whose work happens to be "run another intent".

**Tier 2 — the process.** A durable state machine in [`ref/process`](../ref/process),
persisted in rows: runs, step states, timers, event subscriptions, join state,
leases, human tasks. A run advances by claiming a lease, executing its ready
frames — each step invokes a Tier-1 intent — resolving its outgoing edges and
persisting the new cursor under an optimistic revision guard. Then it either parks
(on a timer, an event, a manual gate or a human task) or completes. It survives
process restarts, and several replicas can advance different runs of the same
process without ever running one step twice.

| | Tier 1 (intent) | Tier 2 (process) |
|---|---|---|
| Lifetime | One request | Days or months |
| State | In memory | Rows |
| Control flow | Every node runs; `flow.*` calls child intents | Edges traverse; steps are visited |
| Waiting | No | Timers, events, manual gates, human tasks |
| Failure | The request fails | Retry with backoff, then compensation |
| Concurrency | Goroutines in one invocation | Leases across replicas |
| Unit of work | A node's action | A step, which runs an intent |

The rule for deciding where something belongs: **if it can wait, it is a process
step.** A node family that parks is rejected at compile time inside a request
intent, with a message pointing at the `process.*` and `task.*` actions you use to
start and inspect a run from a request instead.

---

## The document

```
name / environment / version

secret     — credentials resolved before anything else compiles
resource   — connections and services (the driver SPI plugs in here)
shape      — reusable structural schemas
role       — RBAC roles, with inheritance
tenant     — tenant declarations for multi-tenant scoping
intent     — a request-scoped fact DAG          (Tier 1)
  node     — one unit of work inside it
process    — a durable state machine            (Tier 2)
  step     — one durable unit, running an intent
  edge     — how the run moves between steps
route      — the HTTP projection, with its guards
worker     — a queue consumer running an intent or starting a process
schedule   — cron/every, over the queue
trigger    — a signed webhook or event entry point
```

Compilation happens in a fixed order, and each stage can only depend on earlier
ones: secrets → shapes → roles → whole-document validation → resources (in
dependency order) → processes → intents → engine compile → routes → workers →
schedules → triggers. Processes compile before intents because a step validates
its intent by name while a `process.start` node needs a live engine; nothing
dispatches during compilation either way.

A partially built generation is never served: if any stage fails, everything opened
so far is closed in reverse order and `LoadFile` returns the error.

---

## Tier 1: intents

```
intent "order.place" {
  description "Price the basket, record the order, start its fulfilment"
  response "response"          # which fact becomes the HTTP body
  input_schema "OrderRequest"  # a shape, validated before any node runs
  timeout 15s
  max_db_queries 4

  node "user" {
    family auth
    uses "auth.require_session"
    resource "sessions"
    kind decision              # a deny here blocks every effect below
    provides [user_id]
  }
  node "insert" {
    family database
    uses "database.query"
    resource "db"
    kind effect
    requires [input, user_id]
    provides [rows]
    config {
      statement "INSERT INTO orders (...) VALUES ($1,$2) RETURNING id,total_cents"
      args [user_id, total]
    }
  }
  node "response" {
    family response
    uses "collect"
    requires [rows]
    provides [response]
    output_data { omit [internal_ref] }
  }
}
```

A node's `kind` is its speculation class and its place in the effect barrier:

| `kind` | Meaning |
|---|---|
| `pure` | No I/O. Freely speculated. |
| `read` | Reads state. Runs before identity is known only if safe to do so. |
| `decision` | Records an allow or a deny. Deny is dominant: one deny blocks every effect node in the plan. |
| `effect` | Mutates state. Runs only after every decision has allowed. |
| `async_effect` | Mutates state outside the request's critical path. |
| `stream` | Produces a streamed response. |

`family` is the node's taxonomy entry — what a visual builder shows — and `uses` is
the action that actually runs. A family implies a default action, so the two agree
unless you deliberately override.

**Control flow.** `flow.branch` evaluates ordered cases and invokes the first
matching child intent; `flow.switch` matches a value; `flow.foreach` and
`flow.parallel_map` run a child intent per item with bounded concurrency;
`flow.parallel`, `flow.race` and `flow.quorum` run named children concurrently;
`flow.retry`, `flow.timeout` and `flow.fallback` wrap one child in a policy;
`flow.loop_until` repeats one; `flow.terminate` ends the invocation immediately.

```
node "branch" {
  family branch
  uses "flow.branch"
  requires [input]
  provides [decision]
  config {
    cases [
      { name "large" condition "input.amount > 100" intent "order.large" }
      { name "small" condition "input.amount <= 100" intent "order.small" }
    ]
  }
}
```

Every child intent is resolved at compile time, so a typo fails the deployment
rather than the one request that took that branch. Recursion depth is capped at 10
and a single invocation may make at most 1000 child calls — a runaway graph is a
refusal, not an outage.

---

## Tier 2: processes

```
process "order.fulfil" {
  store "runs"                 # where runs, steps, timers and tasks live
  queue "jobs"                 # how the run wakes itself up
  start "reserve"
  version 1
  migration_policy pin         # pin | migrate | fail, for in-flight runs
  timeout 72h
  retention 720h
  idempotency "id"             # field of the start input that dedupes runs
  retry { max_attempts 3 strategy exponential initial_delay 500ms max_delay 30s jitter true }
  sla  { target 4h breach 24h on_breach escalate escalate "admin" }

  step "reserve" {
    intent "inventory.reserve"
    compensate "inventory.release"
    lock "locks"                            # no two replicas reserve one order
    lock_key "'order:' + run.input.id"
    lock_ttl 30s
  }

  step "approve" {
    family approval
    task {
      title "Approve order {{ run.input.id }}"
      role "approver"
      actions [approve, reject]
      form_schema "ApprovalDecision"
      due 48h
      reminder 12h
      escalate "admin"
      forbid_principals ["run.input.customer_id"]   # four eyes
    }
  }

  step "charge" {
    intent "payment.charge"
    compensate "payment.refund"
    retry { max_attempts 5 strategy exponential_jitter initial_delay 1s max_delay 60s }
  }

  edge "large-order" { kind branch  from "reserve" to "approve" condition "result.input.total_cents > 50000" }
  edge "after-charge" { kind fanout from "charge" targets [fulfil, notify] }
  edge "join"        { kind fanin  sources [fulfil, notify] to "invoice" strategy all }
  edge "charge-failed" { kind compensate from "charge" to "invoice" }
}
```

**How a run advances.** `Advance` claims the run's lease, runs every ready frame,
resolves the outgoing edges of each finished step, writes the new cursor with an
optimistic revision check, and then parks or completes. A second replica that tries
to advance the same run at the same moment finds the lease held and does nothing; a
replica that dies mid-step loses its lease after the TTL and another picks the run
up from its persisted cursor. Two advances therefore never produce two executions
of one step, and no step's result is ever lost to a crash.

**Human tasks.** A `task` block turns a step into a work item: a rendered title and
instructions, a role or queue to route it to, a declared action set the outgoing
edges match on, a form schema validated on completion, a due date with reminder and
escalation timers, and `forbid_principals` for separation of duties. Completing a
task resumes the run from exactly where it parked.

**Compensation.** When a run fails terminally, every completed step that declared a
`compensate` intent is compensated in reverse completion order. Each compensation
is recorded as its own step state with its own retry policy, and progress is
persisted — a crash during rollback resumes the rollback rather than restarting it.
A compensation that itself fails is loud: the run ends in a state that says so
rather than quietly reporting success.

**Events and timers.** A `wait_event` edge parks the run on a named event with a
correlation key; `Signal` wakes every run whose subscription matches (an empty
correlation is refused, because waking every waiting run is almost never intended).
Timers are claimed atomically, so a due timer fires exactly once across replicas
even though every replica is ticking.

---

## Routes and guards

```
route "order.place" {
  method POST
  path "/orders"
  intent "order.place"
  auth "identity"
  session "sessions"
  status 201
  max_body_bytes 65536
  timeout 15s
  authz { permissions [order:create] }
  rate_limit { limiter "limits" limit 30 window 1m }
  idempotency { header "Idempotency-Key" ttl 24h }
  audit { action "order.place" include_request true redact [items] }
  response { omit [internal_ref] }
}
```

Guards run before dispatch, in one fixed order, and every one fails closed:

1. **CORS** preflight
2. **Body limit** — refused, never truncated
3. **Tenant resolution** — an unresolvable tenant is a refusal, not an unscoped query
4. **Authentication** — a missing credential is a failure unless the route declared `allow_anonymous`
5. **Authorization** — deny-dominant: `deny_roles` → `roles` → `permissions` → `condition`
6. **Rate limit** — an unreachable limiter refuses the request
7. **Idempotency** — a replayed key returns the stored response without re-running the intent
8. Dispatch
9. Response shaping
10. Audit (hash-chained, with a mandatory redaction set)

Authentication precedes authorization because a gate needs an identity; the rate
limit follows both so a throttle can be keyed on the principal; idempotency is last
before dispatch so a replay that would have been rejected still is.

An `authz` block that declares no rule is a **compile error**. The safe reading of
an empty gate is "deny", and a compile error says so better than a silent runtime
denial would.

Route modes: `sync` (default), `async` (enqueue and return a job id), `process`
(start a durable run), `stream` (SSE).

---

## Workers, schedules, triggers

```
worker "notifications" {
  queue "jobs"
  job_type "order.notification"
  intent "notification.deliver"
  max_attempts 5
}

schedule "nightly-reconciliation" {
  cron "0 2 * * *"
  timezone "Europe/London"
  queue "jobs"
  intent "ops.reconcile"
  jitter 5m
}

trigger "payment-webhook" {
  kind webhook
  path "/webhooks/payment"
  secret "webhook_secret"
  signature_header "X-Payment-Signature"
  timestamp_header "X-Payment-Timestamp"
  tolerance 5m
  correlation_path "order_id"
  event "payment.settled"
}
```

A webhook trigger's signature is an HMAC over `timestamp + "." + raw body`,
compared in constant time, verified **before** any parsing — a signature over
re-serialised JSON verifies something the sender never signed. Outside the
tolerance window a replayed request is refused. A webhook trigger with no secret
fails to compile.

---

## The expression language

One expression language, everywhere: edge conditions, a step's `skip_when`, authz
conditions, decision-table rules, data transforms, lock keys, task titles. Compiled
once at load time, evaluated against a fixed environment.

In a request node:

| Name | What it holds |
|---|---|
| *fact names* | Each required fact, by its own name |
| `facts` | All of them as a map |
| `config` | The node's own config |
| `input` | The decoded request body |
| `principal` | `id`, `username`, `email`, `tenant_id`, `roles`, `scopes`, `claims` |
| `session` | The session's values |
| `tenant` | The resolved tenant id |
| `now` | RFC3339 timestamp |

In a process edge, step or task:

| Name | What it holds |
|---|---|
| `run` | `id`, `process`, `status`, `input`, `tenant_id`, `principal`, `created_at`, `steps` |
| `step` | `name`, `from`, `attempt`, `key` |
| `input` | The step's input |
| `result` | The step's result — its input before it has run, so one guard reads the same on the way in and out |
| `results` | Every completed step's result, by step name |
| `tenant`, `principal`, `now` | As above |

Expressions may call the hash, encoding and time helper families. They cannot reach
the network, the filesystem or a subprocess: an expression is a predicate over data
already in hand, and anything that reaches outside belongs in a node where it is
budgeted, audited and subject to the effect barrier.

Templates use `{{ }}` over the same environment.

---

## The data pipeline

A `data` block shapes a payload wherever one crosses a boundary: an intent's
`input_data`/`output_data`, a node's, a process edge's, a route's `response`, an
audit record. Stages run in a fixed order so the same spec always produces the same
result regardless of how it was written:

1. `source` — narrow to a sub-path
2. `defaults` — fill absent keys (`""` and `0` are values, not absences)
3. `extract` — build keys from expressions
4. `set` — literal or `{{ }}` templated values
5. `transform` blocks — per-path operations, in declaration order
6. `append` / `prepend`
7. `rename`
8. `coerce` — `string`, `int`, `float`, `bool`, `time`, `duration`, `json`
9. `flatten`
10. `pick` / `omit`
11. `redact` / `mask` (`mask_keep` trailing characters survive)
12. `filter` blocks — accept or reject the whole payload
13. `max_bytes` / `max_depth` — exceeding either **fails**; nothing is silently truncated
14. `schema` — structural validation, last, on the final shape

`strict true` turns a reference to a missing path into a refusal instead of a null.
A zero `data` block compiles to nothing at all.

---

## Resource catalog

32 kinds ship in the box. Everything except the object store is built
on `database/sql`, fh's own primitives and the standard library, so a deployment
needs one piece of infrastructure — a database — and the cache, sessions, queue,
process state, locks and rate limits are all tables in it.

| Kind | What it is | Provides | Config keys |
|---|---|---|---|
| `auth.api_key` | Static API keys for service-to-service calls. Compared in constant time. | Authenticator | `key` `principal_id` `roles` `scopes` `keys` `header` |
| `auth.basic` | HTTP basic credentials verified against bcrypt hashes. For operator and machine access, not end users. | Authenticator | `users` `realm` |
| `auth.chain` | Tries several authenticators in order and uses the first that succeeds — session cookie for browsers, bearer token for APIs. | Authenticator | `authenticators` |
| `auth.jwt` | Verifies bearer JWTs against a configured secret or public key. The algorithm comes from the key, never the token. | Authenticator, JWTIssuer | `algorithm` `secret` `public_key` `public_key_file` `private_key_file` `issuer` `audience` `skew` `ttl` `allow_missing_expiry` `subject_claim` `roles_claim` `scopes_claim` `tenant_claim` `email_claim` `username_claim` |
| `auth.oidc` | Verifies bearer tokens against an OIDC provider's JWKS, discovered and cached with a refresh on unknown key ids. | Authenticator | `issuer` `jwks_url` `audience` `refresh_interval` `timeout` `skew` `roles_claim` `scopes_claim` `tenant_claim` `email_claim` `username_claim` |
| `auth.session` | Resolves the caller from a signed session cookie, so a browser and an API client can share one identity model through auth.chain. | Authenticator | `session` `subject_claim` `roles_claim` `tenant_claim` `scopes_claim` `username_claim` `email_claim` |
| `authz.rbac` | Role-based authorization over the document's role blocks, with inheritance flattened once at load time. | Authorizer | `superuser_roles` `default_roles` |
| `cache.file` | On-disk cache surviving a restart. Shared only between processes on the same filesystem. | Cache, Locker, RateLimiter, CircuitBreaker | `dir` `gc_interval` |
| `cache.memory` | In-process cache. Fast and free, but private to one replica — never use it for anything two replicas must agree on. | Cache, Locker, RateLimiter, CircuitBreaker | `max_entries` `gc_interval` |
| `cache.sql` | Shared cache in a SQL table. Correct across replicas without adding a second datastore. | Cache, CachePrefix, Locker, RateLimiter | `database` `table` `migrate` `gc_interval` |
| `circuit_breaker.store` | Failure-counting breaker over any cache resource, with a half-open probe after the reset window. | CircuitBreaker | `cache` `prefix` `failure_threshold` `reset_after` |
| `database.sql` | SQL database over database/sql. Works with any driver the application imports — PostgreSQL, MySQL, SQLite. | Database | `driver` `dsn` `read_replica_dsn` `max_open_connections` `max_idle_connections` `connection_max_lifetime` `connection_max_idle_time` `ping` `migrations` `allowed_statements` |
| `lock.memory` | Leased mutual exclusion within one replica. Correct only for single-process deployments. | Locker | `max_entries` |
| `lock.store` | Leased mutual exclusion over any cache resource. Name a shared cache (cache.sql) to coordinate across replicas. | Locker | `cache` `prefix` `ttl` |
| `outbox.memory` | In-process transactional outbox, for development and tests. | Outbox | `max_messages` |
| `queue.file` | File-backed durable queue. Survives a restart; visible only to processes sharing the filesystem. | JobQueue, QueueDelay, QueueConsume | `dir` `workers` `max_attempts` `poll_interval` `backoff` `concurrency_limit_by_key` |
| `queue.sql` | SQL broker table claimed with row-level leases. One queue shared correctly by every replica. | JobQueue, QueueDelay, QueueConsume | `database` `table` `migrate` `workers` `max_attempts` `poll_interval` `backoff` `visibility_timeout` `retention` |
| `ratelimit.memory` | Fixed-window rate limiter within one replica. N replicas allow N times the configured limit. | RateLimiter | `max_entries` |
| `ratelimit.store` | Fixed-window rate limiter over any cache resource. Name a shared cache for a deployment-wide limit. | RateLimiter | `cache` `prefix` |
| `search.sql` | Keyword search over a SQL table. Honest about being keyword search, not relevance ranking. | SearchIndex | `database` `table` `migrate` |
| `secret.env` | Resolves secrets from the process environment. | SecretStore | `prefix` |
| `secret.file` | Resolves secrets from a directory of files, the shape a Kubernetes secret mount and Docker secrets both take. | SecretStore | `dir` |
| `service.http` | Outbound HTTP client restricted to an allowlist of hosts, with retries, timeouts and optional request signing. | HTTPService | `base_url` `allowed_hosts` `timeout` `max_response_bytes` `headers` `retry_attempts` `retry_backoff` `allow_private_networks` `sign_secret` `sign_header` `client_cert_file` `client_key_file` `ca_file` `insecure_skip_verify` |
| `service.llm` | Chat and embedding calls to an OpenAI-shaped or Anthropic-shaped HTTP API. | LLM, Embedder | `api` `base_url` `api_key` `model` `embedding_model` `allowed_hosts` `timeout` `max_response_bytes` `max_tokens` `anthropic_version` |
| `service.smtp` | Transactional mail over SMTP with STARTTLS. | Mailer | `host` `port` `username` `password` `from` `timeout` `tls` `insecure_skip_verify` |
| `session.file` | On-disk sessions surviving a restart, shared only between processes on one filesystem. | Session | `secret` `previous_secrets` `cookie` `max_age` `secure` `max_entries` `gc_interval` `dir` |
| `session.memory` | In-process sessions. Lost on restart and invisible to other replicas — development only. | Session | `secret` `previous_secrets` `cookie` `max_age` `secure` `max_entries` `gc_interval` |
| `session.sql` | Sessions in a SQL table. Correct across replicas; the right choice behind a load balancer. | Session | `secret` `previous_secrets` `cookie` `max_age` `secure` `max_entries` `gc_interval` `database` `table` `migrate` |
| `storage.fs` | Object storage in a directory. Keys are validated so nothing can be written outside the root. | ObjectStore | `dir` `max_object_bytes` `file_mode` |
| `storage.sql` | Object storage in a SQL table, so blobs are covered by the same backup and replication as the data referencing them. | ObjectStore | `database` `table` `migrate` `max_object_bytes` |
| `store.memory` | Durable process state in memory. Lost on restart and invisible to other replicas — tests and development only. | ProcessStore | — |
| `store.sql` | Durable process state in SQL tables. Correct across replicas; the right choice for production. | ProcessStore | `database` `table_prefix` |

A resource may depend on another by name (`database "db"`, `cache "cache"`,
`session "sessions"`); the compiler opens them in topological order and rejects a
cycle.

---

## Node families

82 families, each with an action behind it. The family is the taxonomy a
builder shows; `uses` is what runs.

| Family | Node type | What it is | Default action |
|---|---|---|---|
| compute | `action` | Generic unit of work | — |
| compute | `constant` | Publish a fixed value | `constant` |
| compute | `custom` | Host-registered action | — |
| compute | `script` | Evaluate a configured expression | `expression` |
| compute | `template` | Render a text template | `data.template` |
| compute | `transform` | Reshape a payload | `data.transform` |
| compute | `validate` | Structural or rule validation | `validate.schema` |
| coordination | `circuit_breaker` | Guard an unreliable dependency | `circuit_breaker.guard` |
| coordination | `idempotency` | Deduplicate a repeated operation | `idempotency.guard` |
| coordination | `lock` | Acquire or release a lease | `lock.acquire` |
| coordination | `rate_limit` | Consume a rate-limit token | `rate_limit.check` |
| data | `cache` | Cache read, write or invalidation | `cache.get` |
| data | `crud` | Generated create/read/update/delete | `database.crud` |
| data | `database` | SQL query or statement | `database.query` |
| data | `db` | SQL query or statement | `database.query` |
| data | `file` | Read or write a file | `storage.get` |
| data | `search` | Full-text search or indexing | `search.query` |
| data | `storage` | Object storage operation | `storage.put` |
| decision | `condition` | Boolean guard | `decision.expression` |
| decision | `decision` | Policy decision that can deny | `decision.expression` |
| decision | `decision_matrix` | Decision table over rows and thresholds | `decision.table` |
| decision | `rules` | Rule set evaluation | `decision.table` |
| flow | `batch` | Process a collection in groups | `flow.foreach` |
| flow | `branch` | First matching case runs a child intent | `flow.branch` |
| flow | `fallback` | Try alternatives in order | `flow.fallback` |
| flow | `foreach` | Run a child intent per item | `flow.foreach` |
| flow | `iterator` | Run a child intent per item | `flow.foreach` |
| flow | `join` | Merge parallel results | `collect` |
| flow | `loop` | Repeat until a condition holds | `flow.loop_until` |
| flow | `parallel` | Run named child intents concurrently | `flow.parallel` |
| flow | `parallel_map` | Concurrent map over a collection | `flow.parallel_map` |
| flow | `pipeline` | Invoke child intents in sequence | `flow.pipeline` |
| flow | `quorum` | N of M child intents must succeed | `flow.quorum` |
| flow | `race` | First successful child intent wins | `flow.race` |
| flow | `retry` | Retry a child intent with backoff | `flow.retry` |
| flow | `subflow` | Invoke one child intent | `flow.subflow` |
| flow | `switch` | Value-matched dispatch to a child intent | `flow.switch` |
| flow | `timeout` | Bound a child intent's duration | `flow.timeout` |
| identity | `auth` | Authenticate or manage credentials | `auth.authenticate` |
| identity | `authz` | Authorization decision | `decision.authz` |
| identity | `session` | Read or write session state | `session.get` |
| integration | `connector` | Host-registered connector | — |
| integration | `graphql` | GraphQL query or mutation | `service.graphql` |
| integration | `grpc` | gRPC-over-JSON call | `service.grpc_json` |
| integration | `http` | Outbound HTTP call | `service.http` |
| integration | `service` | Call a configured service | `service.http` |
| integration | `tool` | Invoke a registered tool | `service.http` |
| integration | `websocket` | WebSocket message | `service.http` |
| intelligence | `classifier` | Classify a payload | `service.llm_chat` |
| intelligence | `embedding` | Compute embeddings | `service.llm_embed` |
| intelligence | `llm` | Language model completion | `service.llm_chat` |
| intelligence | `rag` | Retrieve grounding context | `service.rag_retrieve` |
| messaging | `email` | Send mail | `service.smtp` |
| messaging | `event` | Emit a domain event | `queue.publish` |
| messaging | `inbox` | Inbound deduplication | `queue.inbox_dedupe` |
| messaging | `notification` | Send a notification | `notify.send` |
| messaging | `outbox` | Transactional outbox publish | `queue.outbox_publish` |
| messaging | `queue` | Publish a durable job | `queue.publish` |
| messaging | `stream` | Emit incremental results | `stream.emit` |
| messaging | `webhook` | Deliver a signed webhook | `service.webhook_send` |
| messaging | `worker` | Queue consumer entry point | `collect` |
| observability | `audit` | Append a hash-chained audit record | `audit.record` |
| observability | `log` | Record a structured log line | `audit.record` |
| observability | `metric` | Emit a metric | `metric.emit` |
| observability | `trace` | Annotate the current trace | `trace.span` |
| process | `approval` | A decision somebody must make | `task.complete` |
| process | `compensation` | Undo a committed step | — |
| process | `delay` | Park for a duration | — |
| process | `escalation` | Reassign or raise overdue work | `task.reassign` |
| process | `external_task` | Park until an external worker reports | — |
| process | `form` | Input collected from a person | `task.complete` |
| process | `human_task` | A work item somebody must complete | `task.list` |
| process | `manual_review` | Work held for manual review | `task.list` |
| process | `process` | Start or signal a durable run | `process.start` |
| process | `subprocess` | Run a child process | — |
| process | `timer` | Park until an instant | — |
| process | `wait` | Park until an external event | — |
| process | `wait_event` | Park until a named event | — |
| process | `workflow` | Start an external orchestrator run | `workflow.start` |
| terminal | `noop` | Do nothing | `collect` |
| terminal | `response` | Assemble the response | `collect` |
| terminal | `terminal` | End the graph | `flow.terminate` |

---

## Edge types

31 edge types, in the durable tier only — a request graph has no edges
because every node in it runs. Every edge evaluates its `condition` before it
traverses, whatever its kind.

| Kind | Family | Semantics | Keys |
|---|---|---|---|
| `branch` | sequence | Traverse only when the condition holds | `from` `to` `condition` `data` |
| `conditional_fork` | sequence | Traverse every target whose condition holds | `from` `targets` `condition` `data` |
| `priority` | sequence | Lowest priority value among siblings wins | `from` `to` `priority` `condition` |
| `simple` | sequence | Traverse to the target | `from` `to` `condition` `data` |
| `switch` | sequence | Value-matched branch | `from` `to` `condition` `data` |
| `threshold` | sequence | Route by numeric band | `from` `threshold` `data` |
| `weighted` | sequence | Weighted random choice among siblings | `from` `to` `weight` `condition` |
| `dynamic_fanout` | concurrency | Targets chosen at run time | `from` `targets_path` `condition` `data` |
| `fanin` | concurrency | Wait for sources, then continue | `sources` `to` `strategy` `quorum` `data` |
| `fanout` | concurrency | Traverse to every target | `from` `targets` `condition` `data` |
| `join` | concurrency | Wait for sources, then continue | `sources` `to` `strategy` `quorum` `data` |
| `parallel` | concurrency | Run targets concurrently | `from` `targets` `max_concurrency` `fail_fast` `continue_on_error` |
| `quorum` | concurrency | Continue once enough sources finish | `sources` `to` `quorum` `strategy` `data` |
| `race` | concurrency | First successful target wins | `from` `targets` `cancel_losers` `timeout` |
| `batch_iterator` | iteration | Run the target once per batch | `from` `to` `items_path` `batch_size` `max_concurrency` |
| `iterator` | iteration | Run the target once per item | `from` `to` `items_path` `max_concurrency` `continue_on_error` |
| `loop_until` | iteration | Repeat the target until the condition holds | `from` `to` `condition` `max_concurrency` |
| `delayed` | suspension | Park for a duration, then continue | `from` `to` `timeout` `data` |
| `escalation` | suspension | Raise or reassign overdue work | `from` `to` `timeout` `escalate` `notify` |
| `manual` | suspension | Park until an operator advances the run | `from` `to` `condition` |
| `wait_event` | suspension | Park until a named event arrives | `from` `to` `event` `correlation` `timeout` `on_timeout` |
| `compensate` | reliability | Run compensation after a failure | `from` `to` `condition` |
| `error` | reliability | Traverse when the source fails | `from` `to` `condition` `data` |
| `fallback` | reliability | Alternative path after a failure | `from` `to` `condition` `data` |
| `rate_limited` | reliability | Park until the rate limit admits the run | `from` `to` `rate_limit` `limit` `window` |
| `retry` | reliability | Retry the target with backoff | `from` `to` `attempts` `timeout` |
| `timeout` | reliability | Bound the target's duration | `from` `to` `timeout` `on_timeout` |
| `filter` | shaping | Traverse only when the payload passes the filter | `from` `to` `data` |
| `stream_pipe` | shaping | Stream the payload to the target | `from` `to` `data` |
| `transform` | shaping | Reshape the payload in transit | `from` `to` `data` `extract` |
| `cancel` | termination | Cancel the run | `from` `condition` |

Notes that matter in production:

- `delayed` and `rate_limited` park the run **durably**. Neither sleeps a goroutine,
  so a restart during the wait costs nothing.
- `weighted` draws once per resolution, so sibling weights compose into one choice
  rather than each edge flipping its own coin.
- `fanin`, `join` and `quorum` record arrivals idempotently: a source that is
  advanced twice cannot satisfy a join twice.
- `loop_until` is bounded by the process's visit cap, so a condition that never
  holds ends the run with a clear error instead of spinning.

---

## Actions

109 actions. Each declares its config fields, which is what the catalog
serves to a builder and what the compiler validates a node against.

| Family | Actions |
|---|---|
| auth | `auth.api_key_issue` `auth.authenticate` `auth.jwt_issue` `auth.login` `auth.logout` `auth.password_hash` `auth.password_verify` `auth.require_session` `auth.reset_token` `auth.token_hash` `auth.totp_verify` |
| cache | `cache.delete` `cache.get` `cache.invalidate_prefix` `cache.set` |
| compute | `collect` `constant` `expression` |
| coordination | `circuit_breaker.guard` `circuit_breaker.record` `idempotency.guard` `idempotency.record` `lock.acquire` `lock.release` `rate_limit.check` |
| data | `data.aggregate` `data.count` `data.filter` `data.first` `data.get` `data.group` `data.json_decode` `data.json_encode` `data.map` `data.merge` `data.pick` `data.sort` `data.template` `data.transform` `request.param` |
| database | `database.cached_query` `database.crud` `database.exec` `database.query` `database.transaction` |
| decision | `allow` `decision.authz` `decision.expression` `decision.rate_limit` `decision.table` `decision.tenant` `deny` |
| flow | `flow.branch` `flow.fallback` `flow.foreach` `flow.loop_until` `flow.parallel` `flow.parallel_map` `flow.pipeline` `flow.quorum` `flow.race` `flow.retry` `flow.subflow` `flow.switch` `flow.terminate` `flow.timeout` |
| intelligence | `service.llm_chat` `service.llm_embed` |
| notification | `notify.send` |
| observability | `audit.record` `metric.emit` `trace.span` |
| outbox | `queue.inbox_dedupe` `queue.outbox_publish` |
| process | `process.advance_manual` `process.cancel` `process.list` `process.signal` `process.start` `process.status` `task.claim` `task.complete` `task.get` `task.list` `task.reassign` `task.release` |
| queue | `queue.publish` `queue.publish_delayed` |
| search | `search.delete` `search.index` `search.query` |
| service | `service.graphql` `service.http` `service.smtp` |
| session | `session.all` `session.delete` `session.flash` `session.get` `session.set` |
| storage | `file.csv_decode` `file.csv_encode` `storage.delete` `storage.get` `storage.list` `storage.presign` `storage.put` |
| validate | `validate.expression` `validate.required` `validate.schema` |

`Registry.Catalog()` returns all of this as JSON — node families, edge types,
resource kinds and actions with their config fields — which is how a visual editor
learns what it may offer without hard-coding a list that drifts.

---

## The driver SPI

[`ref/platform/spi`](../ref/platform/spi) is a dependency-free package of adapter
contracts: `Cache`, `Locker`, `RateLimiter`, `CircuitBreaker`, `JobQueue`,
`ObjectStore`, `Mailer`, `Notifier`, `LLM`, `Embedder`, `SearchIndex`,
`Authenticator`, `Authorizer`, `SecretStore`. A Redis, Kafka or S3 adapter imports
only that package, and registering one needs no change to `ref/platform`.

A complete Redis cache adapter:

```go
package redisadapter

import (
    "context"
    "io"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/oarkflow/fh/ref/platform"
)

type cache struct{ client *redis.Client }

func (c *cache) Get(key string) ([]byte, bool, error) {
    return c.GetContext(context.Background(), key)
}

func (c *cache) GetContext(ctx context.Context, key string) ([]byte, bool, error) {
    value, err := c.client.Get(ctx, key).Bytes()
    if err == redis.Nil {
        return nil, false, nil // a miss is not an error
    }
    if err != nil {
        return nil, false, err
    }
    return value, true, nil
}

func (c *cache) Set(key string, value []byte, ttl time.Duration) error {
    return c.SetContext(context.Background(), key, value, ttl)
}

func (c *cache) SetContext(ctx context.Context, key string, value []byte, ttl time.Duration) error {
    return c.client.Set(ctx, key, value, ttl).Err()
}

func (c *cache) Delete(key string) error { return c.DeleteContext(context.Background(), key) }

func (c *cache) DeleteContext(ctx context.Context, key string) error {
    return c.client.Del(ctx, key).Err()
}

func (c *cache) Close() error { return c.client.Close() }

func init() {
    platform.RegisterResourceDriver("cache.redis",
        platform.ResourceFactoryFunc(func(ctx context.Context, spec platform.ResourceSpec) (platform.Resource, io.Closer, error) {
            opts, err := redis.ParseURL(spec.Config["url"].(string))
            if err != nil {
                return nil, nil, err
            }
            client := redis.NewClient(opts)
            if err := client.Ping(ctx).Err(); err != nil {
                return nil, nil, err // fail the deployment, not the first request
            }
            handle := &cache{client: client}
            return handle, handle, nil
        }),
        platform.ResourceKindInfo{
            Family:   "cache",
            Summary:  "Redis cache",
            Provides: []string{"Cache"},
            Config:   []platform.ConfigField{{Name: "url", Type: "string", Required: true}},
        })
}
```

Import the package from your `main.go` and `kind "cache.redis"` becomes available to
every document that host serves. Three contracts worth knowing about while writing
an adapter:

- Implementing `CacheContext` as well as `Cache` is what lets request cancellation
  and deadlines reach the backend. Any adapter over a network service should.
- Implement `CachePrefix` only if you really can enumerate keys. A document using
  `cache.invalidate_prefix` against a provider that does not implement it fails to
  compile — which is the point. Nothing is registered under a name whose semantics
  it cannot deliver.
- `Locker.Acquire` must never block indefinitely, and its lease is mandatory: a
  crashed holder must not be able to wedge a key forever.

`RegisterActionDriver` does the same for reusable domain actions. Both panic on a
duplicate name, because two providers silently competing for one name is an
ambiguity no deployment should start with.

---

## Authoring constraints in BCL

BCL v0.0.31 has parsing limits that fail **silently** — the document parses, the key
binds to nothing, and the setting you wrote does not exist. They are documented in
[`bclcompat.go`](../ref/platform/bclcompat.go) and asserted by tests, and the
platform's own naming works around every one of them:

| Constraint | Consequence | What to write instead |
|---|---|---|
| `when` and `const` derail the parse | Everything after them is lost | `condition`, `only_if` |
| `type`, `map`, `import`, `include` never bind | The key silently disappears | `family`, `kind`, `extract` |
| `schema` and `field` are reserved block names | The block is not recognised | `shape` and `prop` blocks; `shape`/`value_field` config keys |
| `time.Duration` fields decode to `0` | A timeout silently becomes "no timeout" | Duration strings: `timeout "30s"`, and the platform parses them |
| A bare identifier needs a leading comma in its tag | The value is empty | Handled by the platform's specs; write `method POST` or `method "POST"` freely |
| One key per line; `from`/`to` never bind inline | An edge quietly loses an endpoint | Put each key on its own line |
| Repeated named blocks inside `config { }` are mangled | Cases collapse or vanish | A list of objects: `cases [ { … } { … } ]` |

A test asserts that no catalog entry advertises a key BCL cannot bind, so a builder
generating documents from the catalog cannot be led into this either.

---

## Operating it

**Leases.** Every run advance holds a lease with a TTL. A lease is exclusive even
against its own owner, so a duplicate advance is a no-op rather than a second
execution. `RecoverStalled` reclaims runs whose holder died; the platform runs it
every minute on every replica.

**Timers.** Claimed atomically, so a due timer fires once across the deployment.
Every replica ticks once a second, claiming up to 50 timers per tick.

**Retention.** `retention` on a process bounds how long terminal runs and their step
history survive. `Purge` enforces it, and runs alongside recovery.

**Idempotency.** `idempotency` on a process names a field of the start input: a
repeated start returns the existing run rather than creating a second. On a route
it names a header and a TTL, and a replay returns the stored response without
re-running the intent.

**Replay and intervention.** `ProcessEngines()` exposes each engine, so an operator
surface can `Snapshot` a run (its steps, timers, tasks and errors), `Signal` an
event, `AdvanceManual` a parked manual gate, `Cancel` a run, or `ListTasks` and
`ReassignTask` a stuck approval. The example exposes reads of this through a route
gated on `process:read`.

**Multi-replica.** Run several replicas with different `ReplicaID`s. They share the
queue (`FOR UPDATE SKIP LOCKED`), the timers and the leases; each advances different
runs and none runs a step twice. The memory and file backends are explicitly
single-node — they say so in the catalog, and `store.memory` is for tests and
development.

**Failure posture.** Locks, rate limiters and idempotency stores fail **closed**: if
the backend is unreachable the request is refused. Circuit-breaker state reads fail
**open**, deliberately, because a breaker whose bookkeeping is down should not itself
become the outage; that one exception is documented where it happens.

---

## The trust boundary

`Registry` is the boundary. BCL selects among registered capabilities and
configures them; it can never introduce native code, reach an unregistered kind, or
widen what a capability may do. Everything a document can cause to happen was
registered by the host program in Go.

The document is also not a place secrets live. `secret` blocks describe where a
value comes from; `Platform.Document` — what an introspection route would serve —
carries the resolved resource configuration with every credential-shaped key
redacted, and the secret values stripped.

Other standing decisions worth knowing:

- Authentication failures are indistinguishable to the caller. Wrong password,
  unknown user, expired token and malformed token all produce the same result,
  because telling them apart is an enumeration oracle.
- A JWT's algorithm comes from the configured key, never from the token's header.
- `service.http` requires a host allowlist, checks the resolved address against
  private ranges unless told otherwise, and re-checks after every redirect.
- `storage.fs` refuses path escapes and symlinks and writes through an atomic
  rename.
- SQL identifiers in generated statements are validated against a strict pattern;
  a `database.crud` node can only touch the tables and columns it declares.
- Audit records are hash-chained, and a fixed set of field names is redacted no
  matter what the document says.

---

## What it deliberately does not do

- **No node skipping in a request graph.** Every node in a compiled plan runs.
  Conditionals live inside a node (`flow.*`) or in the durable tier.
- **No expression that reaches outside.** No network, no filesystem, no subprocess
  from an expression — that is what nodes are for.
- **Nothing registered under a name it cannot honour.** Redis, Kafka, S3 and SES are
  documented SPI adapters, not stubs shipped under those names.
- **No cross-node transaction.** One `database.transaction` node can hold several
  statements; two effect nodes cannot share a transaction, because the scheduler
  makes no ordering promise between independent effects.
- **No silent degradation.** A limit exceeded is a refusal, a missing secret is a
  startup failure, an unreachable limiter is a refused request. If the platform
  cannot do what the document asked, it says so.

---

## Further reading

- [`examples/ref-platform`](../examples/ref-platform) — the order-fulfilment
  application, with a curl walkthrough of the whole lifecycle including the approval
- [`examples/ref-platform-todo`](../examples/ref-platform-todo) — the same platform
  at a tenth of the size
- [`ref/platform/bclcompat.go`](../ref/platform/bclcompat.go) — the BCL constraints,
  with the tests that assert them
- [`ref/process`](../ref/process) — the durable engine, independent of BCL
- [`examples/ref-app`](../examples/ref-app) — the same class of application built
  directly on REF in Go, with its own PostgreSQL schema, transactional outbox and
  metrics: read it to see what the platform is generating on your behalf
