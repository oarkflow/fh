# ref-app — a complete application on the Runtime Execution Fabric

This is REF used directly, in Go, with real infrastructure behind every capability:
PostgreSQL, a file-backed cache and durable queue, bcrypt, HMAC bearer tokens, SMTP
or webhook delivery, a transactional outbox, Prometheus metrics and a database audit
trail. Orders are rows, stock is a checked constraint, and the notification promise
commits inside the same transaction as the order it promises to confirm.

It is the code-first counterpart to [`../ref-platform`](../ref-platform), which
builds the same kind of application from a BCL document with no Go at all. Read this
one to understand what REF *is*; read that one to see it driven by configuration.

The only stand-in is the default notifier, which prints instead of sending — and it
says so, in the startup log and in `/healthz`.

---

## Contents

- [What REF actually does](#what-ref-actually-does)
- [The application](#the-application)
- [How a request runs](#how-a-request-runs)
- [Capabilities, and the rule that governs them](#capabilities-and-the-rule-that-governs-them)
- [Effects: the two-phase commit](#effects-the-two-phase-commit)
- [Running it](#running-it)
- [The API](#the-api)
- [A walkthrough](#a-walkthrough)
- [Transports](#transports)
- [Operating it](#operating-it)
- [Testing](#testing)
- [Reading the code](#reading-the-code)
- [Known sharp edges](#known-sharp-edges)

---

## What REF actually does

A conventional web application is a chain: `request → middleware → middleware →
handler → response`. Each link runs in the order you wrote it, and each one may do
anything — read, write, call out, decide.

REF replaces that with a compiled graph of **facts**:

- A **capability** produces a fact (`auth.principal`, `tenant.context`, an
  authorization verdict). It declares what it requires and what it provides.
- An **intent** is one business operation. It declares the facts it needs, and a
  compiled plan is derived from them — nothing says "run this second".
- Everything runs **as soon as its inputs are ready**, concurrently, and reads
  marked safe may run *speculatively* before policy has decided.
- A **decision** is deny-dominant: one deny anywhere blocks every effect in the
  plan, not just the node that asked.
- An intent never writes. It returns an **effect plan**, which the kernel commits
  past the **effect barrier** — after every decision has allowed.

The payoff is that the security model is a *compile-time* property. When
`order.create` says it requires an authorization verdict, the engine walks that fact
back to the capability that produces it and schedules it. Nobody can forget to call
the policy, and a test can read the compiled plan back and assert the policy is in
it — which is exactly what `main_test.go` does.

## The application

An order service for a multi-tenant shop.

| Intent | What it does | Facts it requires |
|---|---|---|
| `auth.register` | Create an account in a tenant, issue its first token | public tenant, address rate limit |
| `auth.login` | Exchange credentials for a bearer token | public tenant, address rate limit |
| `catalog.list` | The tenant's products, cache-aside, anonymous | catalogue (which requires the public tenant), address rate limit |
| `order.create` | Reserve stock, place the order, promise a confirmation | principal, tenant, **authorization**, principal rate limit |
| `order.get` | One order, scoped by policy | principal, tenant, authorization, principal rate limit |
| `order.list` | The orders the policy allows | principal, tenant, authorization, principal rate limit |
| `order.cancel` | Cancel and restock, notify | principal, tenant, authorization, principal rate limit |

Behind them: PostgreSQL (`schema.go`, `store.go`), a file-backed cache used by the
catalogue and the rate limiter, a durable queue, the outbox deliverer, bcrypt +
HS256 tokens, Prometheus metrics, and an audit trail.

## How a request runs

`POST /orders` with a bearer token:

```
                     Invocation (body, token, headers, remote IP)
                                      │
      ┌───────────────────────────────┼───────────────────────────────┐
      ▼                               ▼                               ▼
  app.auth                     order.create.decode            (nothing else can
  verify HS256, load the       parse and validate the          start: everything
  account, its roles and       JSON body                       downstream needs
  its disabled flag            [pure, speculated]              the principal)
  [decision]
      │
      ├──────────────► app.tenant ──────────┐
      │                the account's tenant │
      │                [decision]           │
      │                                     ▼
      ├──────────────► app.ratelimit.principal        app.policy.orders
      │                fixed window in the shared     roles → permissions,
      │                cache [decision]               scope, tenant + region
      │                                               constraints [decision]
      └───────────────────────┬───────────────────────────────┘
                              ▼
                   ═══ EFFECT BARRIER ═══
             (no effect runs until every decision above allowed)
                              ▼
                     order.create.operation
             pure business logic → returns an EffectPlan
                              ▼
        ┌─────────────────────┴───────────────────────┐
        ▼                                             ▼
  LocalTransactional (one *sql.Tx)           DurableDelivery + FireAndForget
  1. reserve stock                           send the confirmation now;
  2. insert the order                        the outbox row is the retry
  3. write the outbox row                    Telemetry, best effort
        └── commit ──────────────────────────────────┘
```

Two properties are worth pausing on:

**The decode node is speculated.** It is pure, so REF runs it while authentication
and the policy are still deciding. If the policy denies, the parsed body is
discarded and nothing was risked. That is where the latency win comes from, and it
is only safe because a pure node cannot mutate anything.

**The tenant is not the header.** `X-Tenant-ID` is a *request*. For an authenticated
caller the tenant comes from their account, and a header that disagrees is refused —
otherwise changing a header would change whose data you read.

## Capabilities, and the rule that governs them

> A capability reaches a plan only by producing a fact the intent requires, and it
> may only read facts it declares.

Both halves of that rule are load-bearing, and this example used to break both.

**The first half.** The original version of this example registered a policy
capability that produced nothing. It looked right, it was in the README's diagram —
and it never ran, because nothing depended on it. Every policy here now publishes a
typed verdict fact (`AuthorizationKey`, `PrincipalRateKey`, …) and every governed
intent lists it in `Spec.Requires`. `TestGovernedIntentsCompileTheirPolicies` reads
the compiled plans back and fails if a policy stops being a dependency.

**The second half.** Reading a fact you did not declare is a race, not a shortcut.
There is no edge to order the two nodes, so you get the value sometimes. That is why
there are two rate limiters and two tenant capabilities rather than one of each
with an `if`:

| Capability | Requires | Keys on / resolves from |
|---|---|---|
| `app.ratelimit.address` | — | the remote address |
| `app.ratelimit.principal` | `auth.principal` | the account |
| `app.tenant.public` | — | the `X-Tenant-ID` header, validated |
| `app.tenant` | `auth.principal` | the account's own tenant |

Each intent picks the pair that matches what it knows at that point, and the choice
is visible in its `Spec`.

**Constraints travel.** `app.policy.orders` records not just "allow" but
constraints: `tenant_id=acme`, `region=eu-central-1`. REF intersects those across
every decision in the execution and a contradiction between any two is itself a
deny. `order.create` then reads the tenant back out of
`nc.Decisions().Constraints()` and builds its writes from it — so the tenant
boundary is enforced by the policy rather than by each query's author remembering
to add a `WHERE`.

## Effects: the two-phase commit

An intent returns a plan; `effectstore.go` is where that plan meets PostgreSQL.

```
Begin(executionID)        → BEGIN on a real connection
Record(txID, effect)      → note the durable effects
  LocalTx effects run     → each writes through *that* transaction
Commit(txID)              → journal row + domain rows + outbox row, all at once
ScheduleDelivery(txID)    → wake the delivery worker
  Durable effects run     → send now; the outbox row is the safety net
  FireAndForget effects   → metrics, best effort
```

Each effect finds the shared transaction by execution id. The consequence is that
`order.create`'s three local effects are genuinely atomic: reserve, insert, and the
promise to notify either all happen or none do. There is no window where an order
exists without its confirmation queued, or a confirmation is queued for an order
that rolled back.

Delivery is **at-least-once**. The worker claims a row, sends, then marks it; a
crash between the send and the mark means one duplicate, which is the honest trade —
marking first would lose notifications instead. Failures back off exponentially and
dead-letter after `REF_APP_OUTBOX_ATTEMPTS`, keeping their last error.

One gap is worth knowing about: REF's `EffectStore` interface has no "abort" call,
so when a local effect fails, nothing tells the store to roll back. The host does it
explicitly on a failed dispatch (`AbortExecution`), and a sweeper rolls back
anything older than 30 seconds as a backstop. Both are in `effectstore.go`, commented
where they are.

## Running it

**You need:** PostgreSQL. Nothing else.

```sh
createdb refapp
export DATABASE_URL="postgres://localhost/refapp?sslmode=disable"
export JWT_SECRET="$(openssl rand -hex 32)"

go run ./examples/ref-app
```

It listens on `:8090`, creates its schema, and seeds two tenants (`acme` and a
suspended one), three products and their stock.

### Environment

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `DATABASE_URL` | **yes** | — | PostgreSQL DSN |
| `JWT_SECRET` | **yes** | — | HS256 signing key, **32 bytes or more** |
| `REF_APP_ADDR` | no | `:8090` | Listen address |
| `DATABASE_DRIVER` | no | `pgx` | Any `database/sql` driver the binary imports |
| `REF_APP_CACHE_DIR` | no | `.data/ref-app/cache` | Catalogue cache and rate-limit windows |
| `REF_APP_QUEUE_DIR` | no | `.data/ref-app/queue` | Durable queue |
| `REF_APP_TOKEN_TTL` | no | `1h` | Bearer token lifetime |
| `REF_APP_RATE_LIMIT` / `REF_APP_RATE_WINDOW` | no | `60` / `1m` | The fixed window |
| `REF_APP_CATALOG_TTL` | no | `30s` | Catalogue cache TTL |
| `REF_APP_NOTIFIER` | no | `stdout` | `stdout`, `smtp` or `webhook` |
| `SMTP_HOST`, `SMTP_PORT`, `SMTP_FROM`, `SMTP_USER`, `SMTP_PASSWORD` | for smtp | `localhost:1025` | Mail |
| `REF_APP_WEBHOOK_URL`, `REF_APP_WEBHOOK_HOSTS` | for webhook | — | Endpoint and its host allowlist |
| `REF_APP_OUTBOX_ATTEMPTS` / `REF_APP_OUTBOX_INTERVAL` | no | `8` / `2s` | Delivery retries and poll period |

Configuration is validated once, at startup, and reports **every** problem at once
rather than one per restart. A short signing key, a webhook host outside its own
allowlist, an unreachable database — all are startup failures.

## The API

| Method | Path | Auth | Notes |
|---|---|---|---|
| POST | `/auth/register` | `X-Tenant-ID` | Returns a bearer token |
| POST | `/auth/login` | `X-Tenant-ID` | Returns a bearer token |
| GET | `/catalog` | `X-Tenant-ID` | Anonymous, cached, `Vary: X-Tenant-ID` |
| POST | `/orders` | Bearer | `sku`, `quantity`, optional `idempotency_key` |
| GET | `/orders` | Bearer | `?limit=` |
| GET | `/orders/:id` | Bearer | |
| POST | `/orders/:id/cancel` | Bearer | optional `reason` |
| GET | `/healthz` | — | Database, outbox, dropped observations |
| GET | `/metrics` | — | Prometheus text; `/metrics.json` for humans |
| GET | `/ref/intents` | — | What this engine can do |
| GET | `/ref/inspect/:intent` | — | The compiled plan |
| GET | `/ref/diagram/:intent` | — | The plan as Mermaid |

Failures are one envelope, with the status derived from the failure's category —
`{"error":{"code":"INSUFFICIENT_STOCK","message":"only 5 units of LAPTOP-X are available"}}` —
so an intent never names an HTTP status and the same failure travels unchanged over
the CLI and the queue.

## A walkthrough

```sh
B=http://localhost:8090

# 1. Register. The tenant comes from the header here, because there is no account yet.
TOKEN=$(curl -sS -X POST $B/auth/register \
  -H 'Content-Type: application/json' -H 'X-Tenant-ID: acme' \
  -d '{"email":"ada@example.com","name":"Ada","password":"correct horse battery"}' \
  | jq -r .token)

# 2. The catalogue is anonymous. Run it twice: the second is served from the cache.
curl -sS $B/catalog -H 'X-Tenant-ID: acme' | jq .
curl -sS $B/catalog -H 'X-Tenant-ID: acme' | jq '.cached'

# 3. Place an order. No tenant header: the token already says which tenant.
curl -sS -X POST $B/orders -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"sku":"WIDGET-100","quantity":2}' | jq .

# 4. The order, the stock movement and the notification committed together.
psql "$DATABASE_URL" -c "SELECT status, payload->'data'->>'order_id' FROM outbox ORDER BY id DESC LIMIT 1"
psql "$DATABASE_URL" -c "SELECT sku, available, reserved FROM inventory WHERE sku='WIDGET-100'"

# 5. Retry safely. The same idempotency key returns the same order, replayed: true,
#    and reserves nothing a second time.
curl -sS -X POST $B/orders -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"sku":"CABLE-1","quantity":1,"idempotency_key":"abc-123"}' | jq '.order.order_id, .replayed'
curl -sS -X POST $B/orders -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"sku":"CABLE-1","quantity":1,"idempotency_key":"abc-123"}' | jq '.order.order_id, .replayed'

# 6. An oversell is refused by the database's own predicate, not by a check that
#    might be stale: only five Laptop X exist.
curl -sS -X POST $B/orders -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"sku":"LAPTOP-X","quantity":99}' | jq .

# 7. Cancel: the status transition and the restock are one transaction.
ORDER=$(curl -sS $B/orders -H "Authorization: Bearer $TOKEN" | jq -r '.orders[0].order_id')
curl -sS -X POST $B/orders/$ORDER/cancel -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"reason":"changed my mind"}' | jq .
curl -sS -X POST $B/orders/$ORDER/cancel -H "Authorization: Bearer $TOKEN" | jq .   # 409

# 8. A suspended tenant is refused before anything runs.
curl -sS $B/catalog -H 'X-Tenant-ID: suspended-co' | jq .

# 9. Look at what actually ran.
curl -sS $B/ref/diagram/order.create
curl -sS $B/metrics | grep refapp_decisions_total
```

## Transports

The same intent, the same policy, the same effects, reached three ways — with no
branch anywhere in the business logic about which one it was:

```sh
# HTTP
curl -X POST $B/orders -H "Authorization: Bearer $TOKEN" -d '{"sku":"CABLE-1","quantity":1}'

# CLI
go run ./examples/ref-app -intent order.create -token "$TOKEN" \
  -input '{"sku":"CABLE-1","quantity":1}'
```

```go
// Queue: MountQueue registers order.create as a job type. Publishing one runs the
// same plan, with the job's headers carrying the identity.
deps.Queue.Enqueue("order.create",
    map[string]any{"sku": "CABLE-1", "quantity": 1},
    map[string]string{"authorization": token})
```

`transport.go` is where each of those becomes an `Invocation`. It is host code on
purpose — building one is about ten lines, and doing it here is what lets the HTTP
layer contribute path parameters and a guaranteed-unique execution id that the
generic `ref/transport/http` adapter does not provide.

## Operating it

**Health.** `/healthz` pings the database, counts the outbox by status, and reports
dropped observations. A dead-lettered notification makes it `degraded` — the service
is serving, but somebody should look.

**Metrics.** `/metrics` is Prometheus text: nodes started and failed, executions and
failures by intent, an execution-latency histogram, policy decisions by outcome, and
the application counters emitted by fire-and-forget effects (`orders.placed`,
`notifications.delivered`, `catalog.cache_hit`).

**Audit.** Every execution outcome and every policy verdict lands in `audit_log`,
batched through a bounded channel. If that channel saturates, events are dropped and
counted — visible in `/healthz`. For a regulated trail that is the wrong trade, and
the fix is to move the observer to the `Critical` tier, where a failure to record is
a failure to serve; `audit.go` says so where it matters.

**Recovery.** On start, `Effects.Recover` finds transactions that committed but were
never scheduled — the process died in between — and schedules their deliveries
before serving anything new.

**Scaling out.** The database and the outbox are shared, and the outbox claim uses
`FOR UPDATE SKIP LOCKED`, so several instances can run the deliverer without sending
anything twice. The cache and the queue are file-backed, so they are per-host: two
instances get two caches (harmless, each expires on its own TTL) and two queues
(only used to wake the deliverer, which also polls). For a real multi-host
deployment, point the cache at a shared store through the same `kv.Store` interface.

## Testing

```sh
go test ./examples/ref-app/                 # plan shape, tokens, passwords, limiter, config
REF_APP_TEST_DSN="postgres://localhost/refapp_test?sslmode=disable" \
  go test ./examples/ref-app/               # …plus the full lifecycle
```

The first command needs nothing. It compiles the plans and asserts the security
model: every governed intent runs authentication, tenancy, the policy and the
limiter; the catalogue runs none of the first three; login and register are limited
by address and require no identity. It also covers token forgery (`alg: none`, a
swapped payload, another issuer's key), the constant-work password comparison, the
fixed window and its expiry, and the configuration rules.

With a DSN it additionally runs the lifecycle against real PostgreSQL: place an
order and assert the outbox row committed with it, retry with an idempotency key and
assert one order, oversell and assert a 409, cancel twice and assert the second is a
conflict, order anonymously and assert a 401, read a suspended tenant's catalogue and
assert a refusal.

## Reading the code

| File | What is in it |
|---|---|
| `main.go` | Wiring, startup order, shutdown, the banner |
| `config.go` | Environment, validated all at once |
| `deps.go` | Opening every resource; registering capabilities and intents; compiling |
| `capabilities.go` | Auth, tenancy (×2), rate limits (×2), policy, catalogue cache |
| `intents_auth.go` | `auth.register`, `auth.login`, and the failure constructors |
| `intents_order.go` | `catalog.list`, `order.create`, `order.get`, `order.list`, `order.cancel` |
| `effects.go` | The effect types — the only code that mutates anything |
| `effectstore.go` | REF's two-phase commit over a real `*sql.Tx` |
| `outbox.go` | Claim, deliver, back off, dead-letter |
| `store.go` | Queries and transactional writes |
| `schema.go` | The schema, re-runnable |
| `security.go` | bcrypt, HS256, identifiers |
| `notifier.go` | stdout / SMTP / webhook |
| `metrics.go`, `audit.go` | Observers |
| `transport.go` | HTTP, CLI and queue invocations; failure projection |

## Known sharp edges

Things this example ran into that are worth knowing before you build on REF:

1. **A capability that provides nothing never runs.** Plans are built backwards from
   required facts. Give every policy a verdict fact.
2. **Reading an undeclared fact is a race.** Declare it, or split the capability.
3. **The kernel emits `NodeStarted`, `NodeFinished` and `ExecutionFinished` only.**
   `Observer.DecisionMade` and `Observer.EffectCommitted` exist on the interface but
   are not called today, so this application records decisions explicitly
   (`Metrics.RecordDecision`, `AuditObserver.RecordDecision`) rather than shipping an
   audit table that would silently stay empty.
4. **`CompositeObserver` forwards `ExecutionFinished` to critical and async observers
   only** — a `Lossy` observer receives node events but never an execution finish, so
   latency histograms stay empty. Both observers here are `Async`.
5. **`EffectStore` has no abort.** Roll back explicitly when a dispatch fails, and
   keep a sweeper for the paths that do not.
6. **The stock generic adapters leave the invocation id empty.** Anything that keys
   per-execution state — an effect transaction, for instance — needs one, so build
   the invocation yourself.
