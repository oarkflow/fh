# Todo — the small way into the platform

A complete, authenticated, cached todo API whose entire implementation is
[`app.bcl`](./app.bcl) — 311 lines of configuration. `main.go` opens the document,
mounts what it declares and listens; it contains no handler, no SQL and no
business logic.

This is the onboarding example. It uses only the request tier, so you can read the
whole thing in one sitting. When you want the durable tier — human approvals,
timers, saga rollback, cross-replica locks — read
[`../ref-platform`](../ref-platform) next.

What it actually demonstrates:

| Concern | Where |
|---|---|
| Registration, login, logout, change password | `intent "auth.*"` over `session.file` |
| Password reset with a one-time token | `auth.forgot_password` → queue → worker → HTTP |
| Todo CRUD scoped to the session's owner | `intent "todos.*"` |
| Cache-aside reads with write invalidation | `database.cached_query` + `cache.delete` |
| Background delivery with retries | `worker "password-reset-delivery"` over `queue.file` |
| Schema creation on startup | `migrations` on the `database` resource |
| Outbound HTTP with a host allowlist and an SSRF guard | `service.http` |

---

## How it works

### One request, end to end

`GET /todos` is a good one to trace, because it touches most of the machinery:

```
route "todos.list"            # method, path, which intent, which session
  └─ session "sessions"       # the signed cookie is loaded before anything else
     └─ intent "todos.list"   # a fact DAG, compiled once at startup
        ├─ node "user"     uses auth.require_session   kind decision  provides [user_id]
        ├─ node "todos"    uses database.cached_query  kind read      requires [user_id]
        └─ node "response" uses collect                              requires [todos]
```

The pieces that matter:

- **Facts, not steps.** A node declares the facts it `requires` and `provides`.
  The compiler builds the DAG from those declarations — nothing in the document
  says "run this second". `node "todos"` runs after `node "user"` because it needs
  `user_id`, and for no other reason.
- **Every node runs.** There is no skipping and no branching inside a request
  graph. Execution is ordered by facts and gated by decisions.
- **A decision is dominant.** `node "user"` is `kind decision`. If the session is
  missing it records a deny, and a single deny anywhere blocks *every* `effect`
  node in the plan. This is why `todos.delete` cannot delete anything for an
  anonymous caller even though the delete node never mentions authentication.
- **`kind` is the effect barrier.** `pure` → `read` → `decision` → `effect`.
  Reads may be speculated ahead of the decision when they are marked safe to be
  (`speculation pre_auth_safe` on the login lookup); effects never are.
- **The response is a fact.** `response "response"` on the intent names which fact
  becomes the body, and `collect` gathers the facts its node required into an
  object.

### What compiles at startup, and what does not

`platform.LoadFile` does all of this before the listener opens:

1. Resolves secrets (`env.required(...)` — a missing one is a startup failure).
2. Opens the resources in dependency order, runs the `migrations`, and pings the
   database.
3. Compiles every intent into an immutable plan: prepares expressions, resolves
   each node's action and resource, and **proves** the graph — every required
   fact has exactly one producer, no fact has two, there are no cycles.
4. Mounts the routes and starts the queue consumer.

So a typo in an action name, a fact nobody produces, two nodes claiming one fact,
a route pointing at an intent that does not exist, or an unreachable database are
all failures at `go run`, with a message naming the intent and the node. They are
never a 500 on the first request that happened to need them.

At request time nothing is parsed or compiled. A request evaluates a plan that
already exists.

---

## What you need

- **PostgreSQL.** The document creates its own tables on startup.
- **A writable directory** for the file-backed cache, sessions and queue
  (`.data/todo/…` by default).
- Nothing else. No Redis, no broker.

### Environment

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `DATABASE_URL` | **yes** | — | PostgreSQL DSN |
| `SESSION_SECRET` | **yes** | — | Session cookie signing key, **32 bytes or more** |
| `APP_ENV` | no | `development` | Recorded on the document |
| `TODO_CACHE_DIR` | no | `.data/todo/cache` | Cache files |
| `TODO_SESSION_DIR` | no | `.data/todo/sessions` | Session files |
| `TODO_QUEUE_DIR` | no | `.data/todo/queue` | Queue files |
| `TODO_NOTIFICATION_URL` | no | `http://127.0.0.1:8099/password-reset` | Where reset tokens are delivered |
| `TODO_NOTIFICATION_HOST` | no | `127.0.0.1` | Host allowlist for that call. A URL outside it is refused. |
| `TODO_NOTIFICATION_ALLOW_PRIVATE` | no | `false` | Permit loopback/RFC1918 delivery. **Needed for the local walkthrough below**, and only for it. |
| `TODO_NOTIFICATION_AUTH` | no | empty | `Authorization` header sent with the delivery |

## Run it

```sh
createdb todo
export DATABASE_URL="postgres://localhost/todo?sslmode=disable"
export SESSION_SECRET="$(openssl rand -hex 32)"

go run ./examples/ref-platform-todo     # from the repository root
```

It listens on `:8089`. The document's path is relative, so run it from the
repository root (or edit the path in `main.go`).

---

## The API

Everything is JSON in and JSON out. "Session" means the `todo_sid` cookie the
login endpoints set.

| Method | Path | Session | Body | Success |
|---|---|---|---|---|
| POST | `/auth/register` | — | `email`, `name`, `password` | 201, sets the cookie |
| POST | `/auth/login` | — | `email`, `password` | 200, rotates the cookie |
| POST | `/auth/logout` | required | — | 200 |
| POST | `/auth/change-password` | required | `current_password`, `new_password` | 200 |
| POST | `/auth/forgot-password` | — | `email` | 200, queues the delivery |
| POST | `/auth/reset-password` | — | `token`, `new_password` | 200 |
| GET | `/todos` | required | — | 200, cached for 30s |
| GET | `/todos/:id` | required | — | 200, cached for 30s |
| POST | `/todos` | required | `title`, optional `description` | 201 |
| PUT | `/todos/:id` | required | `title`, optional `description`, `completed` | 200 |
| DELETE | `/todos/:id` | required | — | 200 |

## What to expect from a response

**Success bodies are keyed by fact name.** `collect` gathers the facts its node
required, so the shape follows the document rather than a hand-written struct:

```jsonc
// GET /todos
{ "todos": [ { "id": 1, "title": "Buy milk", "completed": false, ... } ] }

// POST /todos  — the invalidation is a fact too, so it appears
{ "todo": { "id": 2, "title": "Walk the dog", ... }, "cache_invalidated": true }
```

If you want a bare value instead of the one-key object, add `config { unwrap true }`
to a `collect` node that requires exactly one fact.

**Failures are one envelope, with the status derived from the failure's category:**

```json
{ "error": { "code": "NOT_FOUND", "message": "todo not found" } }
```

| Category | Status | Typical cause here |
|---|---|---|
| invalid input | 422 | `validate.required` found a missing field |
| auth | 401 | no session, or wrong credentials |
| permission | 403 | a decision node denied |
| not found | 404 | `require_affected` matched no row (`todo not found`) |
| conflict | 409 | duplicate email |
| timeout | 504 | the intent's `timeout` elapsed |
| unavailable | 503 | the database or the notification host is unreachable |

Authentication failures are deliberately indistinguishable: an unknown email and a
wrong password produce the same 401, because telling them apart is an enumeration
oracle.

---

## Walkthrough

```sh
BASE=http://localhost:8089
J=/tmp/todo.cookies
curl() { command curl -sS -c $J -b $J -H 'Content-Type: application/json' "$@"; }
```

**Register** (the cookie is set here, so you are logged in already):

```sh
curl -X POST $BASE/auth/register -d '{
  "email":"ada@example.com","name":"Ada","password":"correct horse battery"
}'
```

**Create, list, read, update, delete:**

```sh
curl -X POST $BASE/todos -d '{"title":"Buy milk","description":"2 litres"}'
curl $BASE/todos
curl $BASE/todos/1
curl -X PUT $BASE/todos/1 -d '{"title":"Buy oat milk","completed":true}'
curl -X DELETE $BASE/todos/1
```

**Watch the cache work.** `GET /todos` is a `database.cached_query` with a 30s TTL
keyed on the user. Run it twice and the second read never reaches PostgreSQL; then
create a todo and run it again — the create node's `cache.delete` invalidated the
key, so the list is fresh immediately rather than up to 30 seconds stale:

```sh
curl $BASE/todos          # from PostgreSQL
curl $BASE/todos          # from .data/todo/cache
curl -X POST $BASE/todos -d '{"title":"Third"}'
curl $BASE/todos          # fresh: the write invalidated the list key
```

You can see the effect on disk: `ls .data/todo/cache` before and after.

**Anonymous access is refused by the decision node, not by a route guard:**

```sh
rm -f $J
curl $BASE/todos                     # 401
curl -X DELETE $BASE/todos/2         # 401 — nothing was deleted
```

### The password-reset flow

This is the one flow that crosses out of the request. `auth.forgot_password`
creates a one-time token, stores only its **hash**, and queues the delivery; the
response deliberately does not contain the token. A worker then posts it to the
notification service.

Start a receiver in another terminal:

```sh
python3 - <<'PY'
from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        print(self.rfile.read(int(self.headers['Content-Length'])).decode())
        self.send_response(200); self.end_headers()
HTTPServer(('127.0.0.1', 8099), H).serve_forever()
PY
```

Then, with the server started with `TODO_NOTIFICATION_ALLOW_PRIVATE=true` (the
SSRF guard refuses loopback destinations otherwise):

```sh
curl -X POST $BASE/auth/forgot-password -d '{"email":"ada@example.com"}'
```

The receiver prints the payload, including `reset_token.token`. Use it once:

```sh
curl -X POST $BASE/auth/reset-password -d '{"token":"<token>","new_password":"a new long secret"}'
curl -X POST $BASE/auth/reset-password -d '{"token":"<token>","new_password":"again"}'   # 404
```

The second attempt fails because the token is consumed and the password updated in
**one statement** — a `DELETE … RETURNING` feeding the `UPDATE` — so two
simultaneous redemptions cannot both succeed.

**When delivery fails** (no receiver, or the guard refuses the host) the job is
retried up to `max_attempts 5` with a 1s backoff and then moves to
`.data/todo/queue/failed/`, with the last error recorded in the job file. The HTTP
request that queued it still returned 200: queuing succeeded, and that is what it
reported.

---

## On disk

```
.data/todo/
├── cache/      # cached query results, garbage-collected every minute
├── sessions/   # server-side session state; the cookie holds only a signed id
└── queue/
    ├── pending/ processing/ done/
    └── failed/  # jobs that exhausted max_attempts, with last_error
```

Deleting `.data/todo` logs everybody out and empties the cache and the queue. It
does not touch the database.

---

## Changing it

Three recipes that cover most of what you will want to do. None of them involve
writing Go.

**Add a field to a todo.** Add a migration to the `database` resource, then add the
column to the statements that read and write it:

```
migrations [
  ...,
  "ALTER TABLE todos ADD COLUMN IF NOT EXISTS due_at TIMESTAMPTZ"
]
```

```
# in todos.create
statement "INSERT INTO todos (user_id,title,description,due_at) VALUES ($1,$2,COALESCE($3,''),$4) RETURNING id,title,description,due_at,completed,created_at,updated_at"
args [user_id, input.title, input.description, input.due_at]
```

Migrations are `IF NOT EXISTS`-shaped on purpose: they run on every start, so they
must be safe to re-run.

**Add an endpoint.** An intent plus a route. This one counts what is outstanding:

```
intent "todos.count" {
  response "response"
  timeout 3s
  node "user" { uses "auth.require_session" resource "sessions" kind decision provides [user_id] }
  node "rows" {
    uses "database.query" resource "database" kind read requires [user_id] provides [rows]
    config {
      statement "SELECT COUNT(*) AS outstanding FROM todos WHERE user_id=$1 AND completed=FALSE"
      args [user_id]
    }
  }
  node "count" { uses "data.first" requires [rows] provides [count] config { source_fact "rows" } }
  node "response" { uses "collect" requires [count] provides [response] config { unwrap true } }
}

route "todos.count" { method GET path "/todos/count" intent "todos.count" session "sessions" }
```

Restart. If you misspell `data.frst` or forget that `count` needs `rows`, the
startup tells you which node and why.

**Harden a route.** The guards in the [platform
reference](../../docs/ref-no-code-platform.md#routes-and-guards) are available here
too — declare a limiter resource and add, say:

```
route "auth.login" {
  method POST
  path "/auth/login"
  intent "auth.login"
  session "sessions"
  cache_control "no-store"
  rate_limit { limiter "limits" limit 10 window 15m }
  route_audit { action "auth.login" }
}
```

Two authoring rules worth knowing before you edit anything, because breaking them
fails *silently* in BCL v0.0.31 (all of them are listed in
[`bclcompat.go`](../../ref/platform/bclcompat.go)):

- **Never use `when` as a key** — it derails the parse and everything after it is
  lost. Guards are spelled `condition`.
- **One key per line inside a nested block.** A single-line `config { … }` with
  several nested blocks in it can bind partially. Simple one-line blocks of plain
  key/value pairs — as the `route` blocks here are — are fine.

---

## Troubleshooting

| Message at startup | What it means |
|---|---|
| `required env "DATABASE_URL" is not set` | Export it. Secrets resolve before anything else compiles. |
| `session secret must contain at least 32 bytes` | `SESSION_SECRET` is too short. |
| `dial tcp … connection refused` | PostgreSQL is not reachable; `ping true` makes this a startup failure on purpose. |
| `intent "x" node "y" requires fact "z", which no node provides` | A typo in a `requires`, or you removed the node that provided it. |
| `fact "z" is provided by both "a" and "b"` | Two nodes claim one fact. Rename one. |
| `node "y" uses unregistered action "…"` | Misspelled `uses`. `Registry.Catalog()` lists every action. |
| `resource "r" is not declared` | A node's `resource` names something no `resource` block defines. |

At runtime:

| Symptom | Cause |
|---|---|
| Every todo request returns 401 | The cookie is not being sent. `curl -c/-b` with a jar, and note that login *rotates* the id. |
| A list is stale for up to 30s | You changed rows outside the API, so nothing invalidated the cache key. |
| Reset emails never arrive | No receiver, a host outside `TODO_NOTIFICATION_HOST`, or `TODO_NOTIFICATION_ALLOW_PRIVATE` unset for a loopback URL. Check `.data/todo/queue/failed/`. |

---

## What this example is not

Deliberate omissions, so the document stays readable. Each one is shown in
[`../ref-platform`](../ref-platform):

- **No durable tier.** No process, no human approval, no timer, no compensation.
  Everything here finishes inside one request.
- **No RBAC.** Ownership is enforced by `user_id=$2` in the SQL, not by roles and
  permissions.
- **Single node only.** `cache.file`, `session.file` and `queue.file` are visible
  only to processes sharing the filesystem. Two replicas behind a load balancer
  need the `.sql` variants — which is a change to five lines of `app.bcl`, not to
  any code.
- **No rate limits, idempotency keys, tenancy or audit** on the routes.

## Further reading

- [`app.bcl`](./app.bcl) — the whole application
- [platform reference](../../docs/ref-no-code-platform.md) — both tiers, the full
  resource/node/edge/action catalogs, the driver SPI, the operational surface
- [`../ref-platform`](../ref-platform) — the order-fulfilment example, with the
  durable tier
- [`../ref-app`](../ref-app) — the same kind of application written directly against
  REF in Go, if you want to see what the BCL compiler is doing for you
