# fh Comprehensive Example

Single-server example demonstrating principal, tenant, compliance, authz from contrib, TCPGuard security, sessions, CSRF, webhooks, file upload, WebSocket, typed handlers, redirects, durable queues, and structured error handling.

## Quick Start

```bash
cd examples/complete
go run .
# Server starts on :3000; TCPGuard management on :18183; press Ctrl+C for graceful shutdown.
```

The server logs all requests in JSON format. Compliance evidence is available at `/_fh/compliance`.

## Endpoints

### GET /health

Returns service health. Public — no auth required.

```bash
curl http://localhost:3000/health
```

```json
{"status":"ok","time":"2026-06-23T17:00:00Z"}
```

---

### GET /

Welcome page listing all available endpoints.

```bash
curl http://localhost:3000/
```

```json
{"service":"fh comprehensive example","endpoints":["GET  /health","GET  /static/*","GET  /ws",...]}
```

---

### GET /static/\*

Serves static files from the `public/` directory.

```bash
curl http://localhost:3000/static/hello.txt
```

```
Hello, this is a static file served by fh!
```

---

### POST /typed/hello

Typed handler (from the `modern` example). Parses JSON body and returns a typed response. No auth required.

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"name":"Alice"}' \
  http://localhost:3000/typed/hello
```

```json
{"greeting":"Hello, Alice!"}
```

Without a name:

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{}' \
  http://localhost:3000/typed/hello
```

```json
{"greeting":"Hello, World!"}
```

---

### GET /redirect

301 redirect to an external URL.

```bash
curl -v http://localhost:3000/redirect
```

```
< HTTP/1.1 301 Moved Permanently
< Location: https://example.com
```

---

### GET /api/v1/users

List all users. Requires authz headers.

```bash
curl -H "X-Subject-ID: alice" -H "X-Roles: admin" \
  -H "X-Tenant-ID: acme-corp" \
  http://localhost:3000/api/v1/users
```

```json
{"users":[{"id":"usr_1","name":"Alice"},{"id":"usr_2","name":"Bob"}]}
```

---

### GET /api/v1/users/:id

Get a user by ID. Owner-scoped.

```bash
curl -H "X-Subject-ID: alice" -H "X-Roles: admin" \
  -H "X-Tenant-ID: acme-corp" \
  http://localhost:3000/api/v1/users/usr_123
```

```json
{"user":"usr_123","decision":{"allowed":true,...}}
```

---

### POST /api/v1/users

Create a user. Parses JSON body.

```bash
curl -X POST -H "X-Subject-ID: alice" -H "X-Roles: admin" \
  -H "X-Tenant-ID: acme-corp" \
  -H "Content-Type: application/json" \
  -d '{"name":"Charlie"}' \
  http://localhost:3000/api/v1/users
```

```json
{"id":"usr_new","name":"Charlie"}
```

---

### PUT /api/v1/users/:id

Update a user.

```bash
curl -X PUT -H "X-Subject-ID: alice" -H "X-Roles: admin" \
  -H "X-Tenant-ID: acme-corp" \
  http://localhost:3000/api/v1/users/usr_123
```

```json
{"updated":"usr_123"}
```

---

### DELETE /api/v1/users/:id

Delete a user.

```bash
curl -X DELETE -H "X-Subject-ID: alice" -H "X-Roles: admin" \
  -H "X-Tenant-ID: acme-corp" \
  http://localhost:3000/api/v1/users/usr_123
```

```json
{"deleted":"usr_123"}
```

---

### GET /api/documents

List documents scoped to the current tenant.

```bash
curl -H "X-Subject-ID: alice" -H "X-Roles: admin" \
  -H "X-Tenant-ID: acme-corp" \
  http://localhost:3000/api/documents
```

```json
{"documents":["doc_1","doc_2"],"tenant":"acme-corp"}
```

---

### POST /api/documents/sensitive

Create a sensitive document. Requires idempotency key.

```bash
curl -X POST -H "X-Subject-ID: alice" -H "X-Roles: admin" \
  -H "X-Tenant-ID: acme-corp" \
  -H "Idempotency-Key: idem-001" \
  http://localhost:3000/api/documents/sensitive
```

```json
{"status":"sensitive document created"}
```

---

### GET /api/admin/settings

Admin-only configuration. Requires the `admin` role.

```bash
curl -H "X-Subject-ID: alice" -H "X-Roles: admin" \
  -H "X-Tenant-ID: acme-corp" \
  http://localhost:3000/api/admin/settings
```

```json
{"settings":"admin configuration","decision":{"allowed":true,...}}
```

---

### GET /api/compliance/report

Compliance dashboard. Requires `compliance-officer` role.

```bash
curl -H "X-Subject-ID: bob" -H "X-Roles: compliance-officer" \
  -H "X-Tenant-ID: acme-corp" \
  http://localhost:3000/api/compliance/report
```

```json
{"report":{"generated_at":"...","profile":"enterprise",...}}
```

---

### GET /api/search?q=...

Search endpoint demonstrating query parameter parsing.

```bash
curl "http://localhost:3000/api/search?q=laptops"
```

```json
{"query":"laptops","results":["result_1","result_2"]}
```

---

### POST /webhooks/payments

Webhook receiver with HMAC signature validation and replay protection.

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-Signature: $(echo -n '{"event":"payment.completed"}' | openssl dgst -sha256 -hmac 'webhook-secret' | sed 's/.* //')" \
  -H "X-Nonce: nonce-001" \
  -d '{"event":"payment.completed"}' \
  http://localhost:3000/webhooks/payments
```

```json
{"status":"webhook received"}
```

Replaying the same X-Nonce within 10 minutes is rejected.

---

### GET /account & POST /account/login

Session & CSRF demo. Login then check session.

```bash
# Step 1: GET /account to obtain CSRF token + session cookie
curl -c cookies.txt http://localhost:3000/account

# Step 2: POST /account/login with CSRF token and cookie
CSRF=$(curl -s http://localhost:3000/account | python3 -c "import sys,json; print(json.load(sys.stdin)['csrf_token'])")
curl -b cookies.txt -H "X-CSRF-Token: $CSRF" -X POST http://localhost:3000/account/login

# Step 3: GET /account again to see logged-in session
curl -b cookies.txt http://localhost:3000/account
```

```
{"status":"signed_in"}
{"user":"demo-user","csrf_token":"..."}
```

---

### POST /upload

File upload handler. Accepts multipart form data.

```bash
curl -X POST -F "file=@README.md" http://localhost:3000/upload
```

```json
{"filename":"README.md","size":1234,"status":"uploaded"}
```

---

### GET /ws

WebSocket echo endpoint.

```bash
# Using wscat (npm install -g wscat)
wscat -c ws://localhost:3000/ws
Connected (press CTRL+C to quit)
> Hello
< Hello
```

---

### GET /error/validation

Structured RFC 9457 validation error.

```bash
curl http://localhost:3000/error/validation
```

```json
{"code":"VALIDATION_FAILED","detail":"Validation failed","errors":[{"field":"email","message":"invalid email format"},{"field":"age","message":"must be at least 18"}],"kind":"validation","status":422}
```

---

### GET /error/internal

Structured RFC 9457 internal error.

```bash
curl http://localhost:3000/error/internal
```

```json
{"code":"INTERNAL_ERROR","detail":"something went wrong","kind":"internal","status":500}
```

---

### GET /_fh/compliance

Full compliance report (controls, findings, routes, safe config).

```bash
curl http://localhost:3000/_fh/compliance
```

```json
{"generated_at":"...","profile":"enterprise","controls":[...],"findings":null,...}
```

---

### GET /_fh/compliance/controls

Listing of compliance controls mapped to security standards.

```bash
curl http://localhost:3000/_fh/compliance/controls
```

---

### GET /_fh/compliance/findings

Security findings from runtime validation.

```bash
curl http://localhost:3000/_fh/compliance/findings
```

---

### GET /_fh/config/safe

Redacted safe configuration summary.

```bash
curl http://localhost:3000/_fh/config/safe
```

---

### GET /_fh/live

Liveness probe.

```bash
curl http://localhost:3000/_fh/live
```

```json
{"status":"alive"}
```

---

### GET /_fh/ready

Readiness probe (returns 503 during drain).

```bash
curl http://localhost:3000/_fh/ready
```

```json
{"status":"ready"}
```

---

### GET /_fh/routes

Registered route table with metadata.

```bash
curl http://localhost:3000/_fh/routes
```

---

### GET /_fh/runtime

Runtime diagnostics.

```bash
curl http://localhost:3000/_fh/runtime
```

---

### GET /_fh/queue/stats

Durable queue statistics.

```bash
curl http://localhost:3000/_fh/queue/stats
```

```json
{"Pending":0,"Processing":0,"Done":3,"Failed":0}
```

---

### POST /queue/email

Enqueue an email for durable background processing.

```bash
curl -X POST http://localhost:3000/queue/email
```

```json
{"job_id":"...","status":"queued"}
```

---

## TCPGuard Demo Endpoints

TCPGuard is a real-time security policy engine. The following endpoints demonstrate its capabilities.

### GET /public

No auth required. A clean request is allowed.

```bash
curl http://localhost:3000/public
```

```json
{"message":"clean request allowed","ok":true,"risk":"0"}
```

### POST /api/v1/account/login

Login endpoint monitored by TCPGuard for credential-stuffing and ATO abuse.

```bash
curl -X POST http://localhost:3000/api/v1/account/login
```

```json
{"message":"login accepted","ok":true}
```

Repeat above a few times to trigger rate-limit / abuse rules.

### POST /api/v1/transfers

Signed transfer endpoint. TCPGuard enforces HMAC signing and checks for replay attacks (nonce + timestamp).

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"amount":100}' \
  http://localhost:3000/api/v1/transfers
```

```json
{"message":"signed transfer accepted","ok":true}
```

### POST /admin/users

Admin route with TCPGuard approval workflow (requires security-admin or platform-owner approval).

```bash
curl -X POST -H "X-User-ID: alice" -H "X-User-Role: admin" \
  -H "X-Tenant-ID: demo-bank" \
  http://localhost:3000/admin/users
```

TCPGuard creates a pending approval request:

```json
{"effect":"challenge","approvals":[{"status":"pending","approvers":["security-admin","platform-owner"],...}],...}
```

Approve via the management API:

```bash
curl -H "X-API-Key: dev-management-key" \
  -X POST http://127.0.0.1:18183/approvals/approve?approval_id=<id>
```

### GET /geo-restricted

Geo-restricted endpoint. Requests from outside Nepal are blocked.

```bash
curl http://localhost:3000/geo-restricted
```

When blocked:

```json
{"effect":"block","explanation":"Blocked ... because rule geo-country-restriction matched...",...}
```

### Management Admin Server

TCPGuard exposes a management API on `:18183`:

```bash
curl -H "X-API-Key: dev-management-key" http://127.0.0.1:18183/health
curl -H "X-API-Key: dev-management-key" http://127.0.0.1:18183/incidents
curl -H "X-API-Key: dev-management-key" http://127.0.0.1:18183/audit
```

---

## TCPGuard Request Headers

| Header | Purpose | Example |
|---|---|---|
| `X-User-ID` | TCPGuard identity | `alice` |
| `X-User-Role` | TCPGuard role | `admin` |
| `X-Tenant-ID` | Tenant context | `demo-bank` |
| `X-Session-ID` | Session tracking | `sess-001` |
| `X-Device-ID` | Device fingerprinting | `device-mac-123` |
| `X-Country` | Country code for geo rules | `NP` |
| `X-Business-Amount` | Transaction amount | `5000` |
| `X-Business-Action` | Business action type | `payment` |
| `X-Outside-Hours` | Flag for out-of-hours | `true` |

---

## AuthZ Request Headers (oarkflow)

| Header | Purpose | Example |
|---|---|---|
| `X-Subject-ID` | User identity | `alice` |
| `X-Roles` | Comma-separated roles | `admin` |
| `X-Tenant-ID` | Tenant scope | `acme-corp` |
| `X-Scopes` | OAuth-style scopes | `users:read,documents:write` |
| `Idempotency-Key` | Request deduplication | `idem-001` |
| `X-Nonce` | Replay prevention | `nonce-001` |
| `X-Signature` | HMAC webhook signature | `sha256=...` |

## Middleware Stack Order

1. `recover` — panic recovery
2. `requestid` — request tracing
3. `security` — security headers
4. `bodylimit` — max body size (1 MiB)
5. `timeout` — per-request deadline (30 s)
6. `cors` — cross-origin resource sharing
7. `logger` — JSON request logging
8. `ratelimiter` — rate limit (120/min)
9. `session` — cookie sessions
10. `tenant` — tenant resolution (`X-Tenant-ID`)
11. `principal` — identity extraction (`X-Subject-ID`, `X-Roles`)
12. `authz` — oarkflow AuthZ from contrib (skip paths for public routes)
13. `tcpguard` — TCPGuard policy engine (protects `/public`, `/admin/*`, `/api/v1/*`, `/api/users/*`)
14. Application routes

## Graceful Shutdown

Press `Ctrl+C` to trigger graceful shutdown. In-flight requests complete before the process exits. The shutdown timeout is 30 seconds.
