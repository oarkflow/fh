# Orders — a production-shaped no-code application

Everything this application does is declared in [`app.bcl`](./app.bcl). `main.go`
loads that document, mounts the routes it declares and listens; it contains no
business logic, no handler and no SQL. The schema, the identity model, the RBAC
rules, the caching, the job queue, the durable fulfilment process with its human
approval gate and its saga rollback are all configuration.

What it exercises, end to end:

| Concern | Where it lives |
|---|---|
| Registration, login, logout, bearer tokens | `intent "auth.*"` over `session.sql` + `auth.jwt` |
| Catalogue reads through a shared cache | `intent "catalogue.list"` over `cache.sql` |
| Order placement with pricing and a total | `intent "order.place"` |
| Stock reservation under a cross-replica lock | `process` step `reserve` + `lock.store` |
| A human approval above £500, with four-eyes | `process` step `approve` (`forbid_principals`) |
| Payment with retry and a saga refund | `process` step `charge` + `compensate "payment.refund"` |
| Concurrent fulfilment and notification, joined | `fanout` → `fanin` edges |
| Invoice written to object storage | `storage.fs` |
| Background notifications | `worker "notifications"` over `queue.sql` |
| Nightly reconciliation | `schedule "nightly-reconciliation"` |
| Signed payment webhook | `trigger "payment-webhook"` |

## What you need

One piece of infrastructure: **PostgreSQL**. The cache, the sessions, the job
queue, the process state, the locks and the rate limits are all tables in it, so
they are backed up, replicated and correct across replicas by construction.

## Environment

| Variable | Required | Meaning |
|---|---|---|
| `DATABASE_URL` | yes | PostgreSQL DSN. The document creates its own tables on start. |
| `SESSION_SECRET` | yes | Session cookie signing key, 32 bytes or more. |
| `JWT_SECRET` | yes | HMAC key for the bearer tokens `/auth/token` issues, 32 bytes or more. |
| `PAYMENT_API_KEY` | yes | Credential sent to the payment service. |
| `PAYMENT_WEBHOOK_SECRET` | yes | HMAC key the payment webhook's signature is verified against. |
| `APP_ENV` | no | `development` by default. |
| `DATABASE_DRIVER` | no | `pgx` by default; anything `database/sql` can open and `main.go` imports. |
| `PAYMENT_BASE_URL`, `PAYMENT_HOST` | no | The payment service and its host allowlist. Requests to any other host are refused. |
| `PAYMENT_ALLOW_PRIVATE` | no | `false`. Set `true` only to point at a local mock — it disables the SSRF guard. |
| `SMTP_HOST`, `SMTP_PORT`, `SMTP_FROM`, `SMTP_TLS` | no | Mail. Defaults suit a local MailHog/Mailpit on `localhost:1025`. |
| `INVOICE_DIR` | no | Where invoices are written. `.data/orders/invoices`. |
| `REPLICA_ID` | no | Identifies this process in process leases. Random if unset. |

## Run it

```sh
createdb orders
export DATABASE_URL="postgres://postgres:postgres@localhost/orders?sslmode=disable"
export SESSION_SECRET="$(openssl rand -hex 32)"
export JWT_SECRET="$(openssl rand -hex 32)"
export PAYMENT_API_KEY="test-payment-key"
export PAYMENT_WEBHOOK_SECRET="$(openssl rand -hex 32)"
export PAYMENT_BASE_URL="http://127.0.0.1:9099"
export PAYMENT_HOST="127.0.0.1"
export PAYMENT_ALLOW_PRIVATE=true   # only because the gateway here is a local mock

go run ./examples/ref-platform
```

It listens on `:8089` (`-addr` to change it, `-config` to point at another
document). A missing secret, an unreachable database, a misspelled action or a
process step whose intent does not exist fails the *startup*, not the first
request that needed it.

Run a second replica with a different `-addr` and `REPLICA_ID` whenever you want
to watch the leases work: both advance runs, neither runs a step twice.

## The whole lifecycle in curl

```sh
BASE=http://localhost:8089
J=/tmp/orders.cookies     # the session cookie jar
curl() { command curl -sS -c $J -b $J -H 'Content-Type: application/json' "$@"; }
```

**1. Register a customer.** The `Registration` shape enforces the email format
and a 12-character minimum before any node runs; the password is bcrypt-hashed in
a node, never in the document.

```sh
curl -X POST $BASE/auth/register -d '{
  "email": "ada@example.com", "name": "Ada Lovelace", "password": "correct horse battery"
}'
```

**2. Stock the catalogue.** This needs `catalogue:write`, which only `admin`
holds, so as a customer it answers 403 — the deny is the point. Grant it
directly for the walkthrough:

```sh
psql "$DATABASE_URL" -c "UPDATE users SET roles='admin' WHERE email='ada@example.com'"
curl -X POST $BASE/auth/login -d '{"email":"ada@example.com","password":"correct horse battery"}'

curl -X PUT $BASE/catalogue -d '{
  "sku": "LAPTOP-1", "name": "Laptop", "price_cents": 120000, "stock": 10
}'
curl -X PUT $BASE/catalogue -d '{
  "sku": "CABLE-1", "name": "USB-C cable", "price_cents": 1200, "stock": 500
}'
```

**3. Read the catalogue twice.** The first read comes from PostgreSQL, the second
from the shared cache. `GET /catalogue` needs no session at all.

```sh
curl $BASE/catalogue
curl $BASE/catalogue
```

**4. Place a small order.** £12 is under the £500 approval threshold, so the
`branch` edge routes it straight to the charge and the run finishes on its own.

```sh
curl -X POST $BASE/orders -d '{"items":[{"sku":"CABLE-1","quantity":1}],"currency":"GBP"}'
curl $BASE/orders
```

The response carries the order and its `run_id`. The run starts detached — the
request returns as soon as the run exists, and the queue advances it — so poll
the order to watch it move:

```sh
curl $BASE/orders/<order id>
```

**5. Place a large order.** £1,200 is over the threshold, so the run reserves the
stock and then *parks* on a human task. Nothing is charged yet.

```sh
curl -X POST $BASE/orders -d '{"items":[{"sku":"LAPTOP-1","quantity":1}],"currency":"GBP"}'
```

**6. Try to approve your own order.** `forbid_principals ["run.input.customer_id"]`
is the four-eyes control: the approval task does not appear in the queue of the
person who placed the order.

```sh
curl $BASE/tasks          # empty for Ada, who placed it
```

Register a second person and give them `approver`:

```sh
rm -f $J
curl -X POST $BASE/auth/register -d '{
  "email": "grace@example.com", "name": "Grace Hopper", "password": "another long secret"
}'
psql "$DATABASE_URL" -c "UPDATE users SET roles='approver' WHERE email='grace@example.com'"
curl -X POST $BASE/auth/login -d '{"email":"grace@example.com","password":"another long secret"}'

curl $BASE/tasks          # one task, its title rendered from the run's own data
```

**7. Approve it.** The `ApprovalDecision` shape rejects anything but `approve` or
`reject`, and the outgoing `branch` edges match on the action, so an undeclared
action cannot silently take a path.

```sh
curl -X POST $BASE/tasks/<task id>/decide -d '{"action":"approve","note":"within budget"}'
```

The run continues from its persisted cursor: `record_approval` → `charge` →
`fulfil` and `notify` concurrently → `fanin` → `invoice`. Watch it as an
operator (`process:read`, which `admin` holds):

```sh
curl $BASE/ops/runs
```

**8. Watch the rollback.** Point `PAYMENT_BASE_URL` at something that refuses the
charge and place another large order. After the charge exhausts its five attempts,
the `compensate` edge runs the compensations in reverse completion order —
`payment.refund`, then `inventory.release` — each recorded as its own step, and
the order ends up back with its stock released rather than half-charged.

**9. Settle it from the outside.** The payment webhook is a signed trigger: the
signature is verified against `PAYMENT_WEBHOOK_SECRET` over the raw body with a
five-minute timestamp tolerance, and the `order_id` in the body correlates the
event to the waiting run. An unsigned or replayed request is refused before it
reaches any intent.

```sh
BODY='{"order_id":"<order id>","status":"settled"}'
TS=$(date +%s)
SIG=$(printf '%s.%s' "$TS" "$BODY" | openssl dgst -sha256 -hmac "$PAYMENT_WEBHOOK_SECRET" -hex | awk '{print $2}')
command curl -sS -X POST $BASE/webhooks/payment \
  -H 'Content-Type: application/json' \
  -H "X-Payment-Timestamp: $TS" \
  -H "X-Payment-Signature: $SIG" \
  -d "$BODY"
```

**10. Bearer tokens.** Anything a browser does with a session cookie an API
client can do with a token, because the route's `auth "identity"` is a chain that
accepts either.

```sh
TOKEN=$(curl -X POST $BASE/auth/token | jq -r .token)
command curl -sS $BASE/orders -H "Authorization: Bearer $TOKEN"
```

## Where to look next

- [`app.bcl`](./app.bcl) reads top to bottom: secrets, connections, shapes, roles,
  the request graphs, the durable process, then how the outside world reaches them.
- [`../../docs/ref-no-code-platform.md`](../../docs/ref-no-code-platform.md) explains
  the two tiers, the full node and edge catalogs, every resource kind's config keys,
  and how to plug Redis or Kafka in through the driver SPI.
- [`../ref-platform-todo`](../ref-platform-todo) is the same platform at a tenth of
  the size, if this one is too much at once.
