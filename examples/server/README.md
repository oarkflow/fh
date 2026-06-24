# TCPGuard fh server example

This is a complete `github.com/oarkflow/fh` server wired to TCPGuard as an enterprise anomaly-detection and business-rule enforcement layer. It demonstrates global rules, endpoint rules, correlation rules, BCL-configurable detector modules, authz, HMAC/replay protection, datasource lookups, audit envelopes, approvals, incidents, metrics, and management APIs.

## Run

```bash
cd examples/tcpguard-fh-server
go mod tidy
go run .
```

The example defaults to a compact production-style response contract. Use `TCPGUARD_ENV=development go run .` only when you need full local diagnostics such as matched rule IDs, evidence, and field-level details.

Application server: `http://127.0.0.1:18184`

Management server: `http://127.0.0.1:18185`

Default management API key: `dev-management-key`. Override with `TCPGUARD_MGMT_API_KEY`.


## Dependency integration

This example uses the real upstream TCPGuard engine, not a local reimplementation:

- `github.com/oarkflow/tcpguard v0.0.14` for policy loading, evaluation, rendering, audit, incidents, approvals, replay/HMAC, datasource lookups, management APIs, and metrics.
- `github.com/oarkflow/fh/contrib/mw/tcpguard` as the thin FH middleware adapter.

The adapter calls `Guard.EvaluateHTTPRequest` for each FH request and uses TCPGuard's own `HTTPRequestResult`, decision response, and public response policy.

## User-facing decision messages

TCPGuard renders safe, readable decision responses through `ResponseMessagePolicy`. This example intentionally also uses `WithResponseRenderer` so production APIs keep a stable, professional response envelope while still using the environment-aware public response builder internally. The FH adapter uses the same policy for metadata headers and `OnDecision` emits structured logs for enforced or non-info decisions by default.

Environment behavior:

- Unset `TCPGUARD_ENV` or set `TCPGUARD_ENV=production`: compact response contract. The public body contains only the safe decision code/message, concise reason, status, and request ID. It does **not** return duplicated outcome fields, risk scores, details arrays, action lists, rule internals, raw headers, tokens, signatures, nonces, body payloads, datasource values, or noisy debug metadata.
- `TCPGUARD_ENV=staging`: production-safe values with bounded diagnostics when TCPGuard policy allows them.
- `TCPGUARD_ENV=development` or `TCPGUARD_ENV=test`: local diagnostics such as matched rule IDs, public evidence, and field-level details. Sensitive fields such as authorization, cookie, token, secret, password, API key, signature, nonce, card, CVV, body, and payload are still redacted when detected.

Production denial body shape:

```json
{
  "code": "TCPGUARD_BLOCK_CRITICAL",
  "message": "Request blocked by security policy.",
  "reason": "request timestamp is outside allowed clock skew",
  "status": 403,
  "request_id": "req_..."
}
```

Development denial bodies add diagnostic fields such as `details[].message`, `details[].fields`, `matched_rules`, public evidence, and action errors where applicable. Keep development diagnostics off for normal demos and production.


## Response renderer and structured logs

This example keeps `WithResponseRenderer` enabled. The renderer does **not** expose raw TCPGuard internals directly. Instead, it wraps `tcpguard.PublicDecisionResponseRenderer(exampleResponsePolicy())`. That gives applications full control over their public response contract without leaking sensitive evidence or adding noisy debug metadata.

The FH middleware configures `OnDecision: logHTTPDecision`. By default, only enforced or non-info decisions are written as compact structured JSON log entries using `tcpguard.DecisionLogEntry(...)`. Set `TCPGUARD_LOG_ALLOWED=true` when you also want allow-decision logs. Production logs include the trigger/rule, concise reason, deduplicated finding summary, compact action summary, request ID, method/path, safe entity references, policy version, incident reference, and audit ID where available. They intentionally do **not** dump the full request object, business context, trace, audit envelope hashes, rate-counter evidence, or all raw decision internals into normal application logs. Set `TCPGUARD_ENV=development` or `policy.LogLevel = tcpguard.DecisionLogFull` when you need the full redacted diagnostic trace locally or in a trusted SIEM sink.

Production user response goal: minimal, understandable, supportable.

Production operator log goal: compact, searchable, debuggable, and safe.

Example production response fields:

```json
{
  "code": "TCPGUARD_BLOCK_CRITICAL",
  "message": "Request blocked by security policy.",
  "reason": "request timestamp is outside allowed clock skew",
  "status": 403,
  "request_id": "req_..."
}
```

Example compact production log fields include:

```json
{
  "event": "tcpguard.http.decision",
  "request_id": "req_...",
  "method": "POST",
  "path": "/api/v1/transfers",
  "effect": "block",
  "severity": "critical",
  "reason": "request timestamp is outside allowed clock skew",
  "triggered_rules": ["signed-transfer-replay-or-mitm"],
  "incident_created": true,
  "tenant": "demo-bank",
  "audit_id": "audit_1"
}
```

Allowed `info` decisions are not logged unless `TCPGUARD_LOG_ALLOWED=true`, which keeps normal demo output readable while preserving full audit/metrics inside TCPGuard.

## What is included

The server loads `tcpguard.bcl`, which includes rule files under `rules/*/*.bcl`.

| Area | Rules / features |
|---|---|
| Global protection | known bad IP feed, banned user cache lookup, tenant lockdown, per-IP rate limit |
| Endpoint protection | admin after-hours challenge, sensitive export challenge, dynamic user/order ownership checks |
| Extensible detectors | BCL-configured `rate`, `replay`, `header_anomaly`, `abuse`, and `dsl` detectors through the detector registry |
| Abuse/anomaly detection | credential stuffing, password spray, account enumeration, ATO signals, API key sharing, UA rotation, endpoint scanning, function abuse, export abuse, destructive admin abuse, payment velocity |
| Correlation | failed-login sequence followed by login-success to trigger account takeover chain handling |
| Business rules | high-value payment after-hours enforcement |
| Data lookups | CSV user directory, JSON tenant config, in-memory ban cache, SQL account status, HTTP external risk API |
| Security controls | HMAC signatures, nonce/timestamp replay checks, authz policy file, MFA/challenge/block/throttle actions |
| Enterprise operations | audit envelopes and verification, incidents, approval records, metrics snapshot, hot reload/simulate/explain management APIs |


## Route-level middleware design

The example registers TCPGuard as route-level FH middleware for every business endpoint:

```go
guardMiddleware := tcpguardfh.MiddlewareWithConfig(tcpguardfh.Config{
    Guard: guard,
    ResponsePolicy: exampleResponsePolicy(),
    OnDecision: logHTTPDecision,
})

app.Post("/api/v1/transfers", guardMiddleware, ok("signed transfer accepted"))
```

This keeps each endpoint in control of its own middleware chain. Operational demo endpoints such as `/_demo/metrics`, `/_demo/audit`, and `/_demo/sign` stay outside the business guard chain, while every business endpoint explicitly opts into TCPGuard and can add more route-specific middleware beside it.


## Verify the documented examples

Start the server in one terminal, then run the smoke verifier in another terminal:

```bash
cd examples/tcpguard-fh-server
./scripts/verify_examples.sh
```

The script validates the most important documented scenarios: clean allow, debug throttle, banned user, tenant lockdown, bad IP file intel, geo restriction, admin challenge, sensitive export, high-value payment block, dynamic route ownership, invalid transfer signature, and management health.

## Curl scenarios

### 1. Clean public request

Purpose: proves the FH adapter lets low-risk traffic pass with a compact application response and minimal TCPGuard metadata headers.

```bash
curl -i http://127.0.0.1:18184/public
```

Expected response:

- Status: `200 OK`
- Headers: `X-TCPGuard-Decision: allow`, `X-TCPGuard-Message: Request allowed.`, possibly `X-TCPGuard-Trace`
- Body shape:

```json
{"ok":true,"message":"clean request allowed"}
```

Response description: TCPGuard evaluated the request, found no abuse/business/security signal, and FH continued to the route handler.

### 2. Debug/probe query throttled by endpoint rule

Purpose: demonstrates DSL detector/rule handling for suspicious query parameters.

```bash
curl -i 'http://127.0.0.1:18184/public?debug=true'
```

Expected response:

- Status: usually `429 Too Many Requests` in enforce mode when `throttle` is selected
- Headers: `X-TCPGuard-Decision: throttle`, `X-TCPGuard-Severity`, `X-TCPGuard-Trace`, and `X-TCPGuard-Message`
- Production body includes only `code`, readable `message`, safe `reason`, `status`, and `request_id`

Response description: `debug-query-probe` / `debug_query_probe` identifies a probe-style query and applies the throttle response.


### 2b. Production-safe denial body vs development diagnostics

Purpose: shows the same blocked request rendered with safe production disclosure. Start the server with `TCPGUARD_ENV=production` or leave it unset.

```bash
curl -i -X POST \
  -H 'X-User-ID: banned-user' \
  http://127.0.0.1:18184/public
```

Expected response:

- Status: `403 Forbidden` or another enforced status depending on the selected effect
- Headers: `X-TCPGuard-Decision`, `X-TCPGuard-Severity`, `X-TCPGuard-Trace`, and `X-TCPGuard-Message`
- Body shape: `code`, `message`, `reason`, `status`, and `request_id`
- Production body does **not** expose raw tokens, signatures, cookies, authorization headers, body payloads, datasource values, risk scores, action lists, details arrays, or internal rule details. The application log for the same request contains a compact structured decision with triggered rules, deduplicated findings, compact action summary, request ID, audit/incident references, and hashed/redacted identifiers for debugging.

Response description: the user receives an understandable reason and a request ID for support, while sensitive security evidence remains in TCPGuard audit/incident records. Re-run with `TCPGUARD_ENV=development go run .` to include fuller local diagnostics.

### 3. Global per-IP rate rule after repeated requests

Purpose: verifies global rate counters and endpoint-independent abuse handling.

```bash
for i in 1 2 3 4 5; do curl -i -H 'X-Forwarded-For: 10.10.10.10' http://127.0.0.1:18184/public; done
```

Expected response:

- First few responses: `200 OK`
- Later responses: `429 Too Many Requests`
- Headers on throttled responses: `X-TCPGuard-Decision: throttle`, `X-TCPGuard-Severity: medium`
- Production body explains that the request rate limit was exceeded without exposing sensitive values; development body also references the `demo-rate-limit` rule

Response description: repeated requests from the same IP exceed the demo threshold and demonstrate global protection.

### 4. Credential stuffing / password spray

Purpose: sends explicit `auth.login_failed` events through the demo endpoint to trigger auth abuse rules.

```bash
for u in a b c d; do \
  curl -i -X POST \
    -H 'X-Forwarded-For: 203.0.113.25' \
    -H "X-User-ID: $u" \
    http://127.0.0.1:18184/_demo/auth/fail; \
done
```

Expected response:

- Early failures may be monitor/allow depending on counters
- Once thresholds are crossed: decision body has `code`, readable `message`, safe `reason`, `status`, and `request_id`
- Production response remains compact; production logs include the triggered rule, finding summary, compact action summary, incident/audit references, and safe hashes. Development responses include diagnostic details and matched rule IDs.

Response description: TCPGuard tracks failed-auth velocity by IP/user and distinct users to detect credential-stuffing behavior.

### 5. Correlated account takeover chain

Purpose: demonstrates sequence/correlation detection: three failed logins followed by a successful login.

```bash
for i in 1 2 3; do \
  curl -s -X POST -H 'X-User-ID: user-1' -H 'X-Forwarded-For: 198.51.100.45' http://127.0.0.1:18184/_demo/auth/fail >/dev/null; \
done
curl -i -X POST -H 'X-User-ID: user-1' -H 'X-Forwarded-For: 198.51.100.45' http://127.0.0.1:18184/_demo/auth/success
```

Expected response:

- Status: challenge-style response, commonly `403` depending renderer/effect mapping
- Production body contains a readable challenge/block message and request ID; development body includes `matched_rules` containing `account-takeover-correlation-chain` and trace contributors
- Production application logs include a compact rule/finding/action summary and incident/approval references

Response description: the rule sequence matches an ATO chain and escalates to MFA/SOC notification/incident action.

### 6. Account takeover signals from a new device/country

Purpose: tests behavioral signal detection without an auth-failure sequence.

```bash
curl -i -X POST \
  -H 'X-User-ID: user-1' \
  -H 'X-New-Device: true' \
  -H 'X-Previous-Country: US' \
  -H 'X-Country: NP' \
  http://127.0.0.1:18184/api/v1/account/login
```

Expected response:

- Status: challenge-style response
- Headers: `X-TCPGuard-Decision: challenge`, `X-TCPGuard-Severity: high`
- Compact body includes a safe challenge reason and request ID. Development mode includes `account-takeover-abuse` and finding `account_takeover_risk`.

Response description: TCPGuard combines new device, country change, session drift, and profile risk into an ATO score.

### 7. Known banned user through memory datasource

Purpose: demonstrates business/security lookup against a memory datasource.

```bash
curl -i -H 'X-User-ID: banned-user' http://127.0.0.1:18184/public
```

Expected response:

- Status: `403 Forbidden`
- Headers: `X-TCPGuard-Decision: block`, `X-TCPGuard-Severity: critical`
- Production body shows a safe block reason and request ID; development body includes matched rule `cache-banned-user`. Production logs include the matched rule and sanitized datasource evidence.

Response description: the `demo-cache` datasource contains `ban:user:banned-user`, causing a block.

### 8. Tenant lockdown through JSON datasource

Purpose: tests tenant-level enterprise lockdown.

```bash
curl -i -H 'X-Tenant-ID: locked-tenant' http://127.0.0.1:18184/public
```

Expected response:

- Status: `403 Forbidden`
- Compact body includes a safe block reason and request ID. Development mode includes matched rule `tenant-lockdown`.
- Headers include `X-TCPGuard-Decision: block`

Response description: tenant config is loaded from `data/tenants.json`; the locked tenant is denied globally.

### 9. Known bad IP through file intel feed

Purpose: verifies file-based IP intelligence enrichment.

```bash
curl -i -H 'X-Forwarded-For: 203.0.113.10' http://127.0.0.1:18184/public
```

Expected response:

- Status: `403 Forbidden`
- Compact body includes a safe block reason and request ID. Development mode includes matched rule `block-bad-ip`.
- Operator logs/audit provide the blacklisted IP/reputation evidence without exposing it in the public response.

Response description: the IP is loaded from `data/bad_ips.txt` and enriched before rule evaluation.

### 10. Admin after-hours rule

Purpose: tests endpoint-specific admin business rules, datasource enrichment, approval, and MFA challenge.

```bash
curl -i -X POST \
  -H 'X-User-ID: manager-1' \
  -H 'X-User-Role: admin' \
  -H 'X-Outside-Hours: true' \
  -H 'X-New-Device: true' \
  http://127.0.0.1:18184/admin/users
```

Expected response:

- Status: challenge-style response
- Headers: `X-TCPGuard-Decision: challenge`, `X-TCPGuard-Severity: high`
- Compact body includes a safe challenge reason and request ID. Development mode includes matched rule `admin-after-hours-department-check`.
- Body may include approval records

Response description: admin activity outside business hours from a risky context requires security review/MFA.

### 11. Sensitive report export

Purpose: demonstrates endpoint-level data exfiltration controls.

```bash
curl -i -X POST \
  -H 'X-User-ID: manager-1' \
  -H 'X-Sensitivity: high' \
  http://127.0.0.1:18184/api/v1/reports/export
```

Expected response:

- Status: challenge-style response
- Compact body includes a safe challenge reason and request ID. Development mode includes rule `sensitive-export`.
- Findings may include `sensitive_export` or export-related abuse signals

Response description: high-sensitivity exports are challenged even if the HTTP request is otherwise valid.

### 12. Repeated export abuse

Purpose: tests export velocity detection.

```bash
for i in 1 2 3; do \
  curl -i -X POST -H 'X-User-ID: analyst-1' http://127.0.0.1:18184/api/v1/reports/export; \
done
```

Expected response:

- Later responses contain elevated risk and export-abuse findings
- Depending threshold/action, decision may be challenge or throttle

Response description: repeated exports by the same entity are treated as possible data exfiltration.

### 13. Application attack probe

Purpose: checks path traversal / injection / SSRF-style abuse detection.

```bash
curl -i 'http://127.0.0.1:18184/public?file=../../etc/passwd'
```

Expected response:

- Status: `403 Forbidden`
- Compact body includes a safe throttle reason and request ID. Development mode includes rule `application-attack-probe`.
- Findings include application abuse such as path traversal probe

Response description: TCPGuard detects probe payloads before the route handler processes the request.

### 14. Function invocation abuse

Purpose: simulates excessive workflow/function invocation.

```bash
for i in 1 2 3 4; do \
  curl -i -X POST -H 'X-User-ID: function-user' http://127.0.0.1:18184/api/v1/functions/reconcile; \
done
```

Expected response:

- Later responses include function invocation velocity findings
- Decision may throttle/challenge depending cumulative risk

Response description: TCPGuard protects expensive internal functions from repeated invocation abuse.

### 15. High-value payment after hours

Purpose: tests business-specific fraud/approval rules.

```bash
curl -i -X POST \
  -H 'X-User-ID: manager-1' \
  -H 'X-User-Role: finance_approver' \
  -H 'X-Business-Amount: 1500000' \
  -H 'X-Outside-Hours: true' \
  http://127.0.0.1:18184/api/v1/payments/approve
```

Expected response:

- Status: `403 Forbidden`
- Headers: `X-TCPGuard-Decision: block`, `X-TCPGuard-Severity: critical`
- Compact body includes a safe block reason and request ID. Development mode includes matched rule `high-value-payment-after-hours`.

Response description: the derived event `business.high_value_payment` fires and blocks high-value after-hours approval.

### 16. Dynamic route ownership check

Purpose: verifies path-parameter extraction and endpoint-level authorization/business ownership checks.

```bash
curl -i -X PUT \
  -H 'X-User-ID: user-1' \
  http://127.0.0.1:18184/api/users/user-2/order/order-9
```

Expected response:

- Status: challenge-style response
- Compact body includes a safe challenge reason and request ID. Development mode includes rule `dynamic-order-change`.
- Evidence shows route params, especially `request.params.id = user-2`

Response description: user `user-1` is trying to modify `user-2`'s order, so TCPGuard challenges the request.

### 17. Invalid signed transfer

Purpose: verifies HMAC signature, timestamp, and nonce protection.

```bash
curl -i -X POST \
  -H 'X-User-ID: manager-1' \
  -H 'X-TCPGuard-Signature: bad-signature' \
  -H 'X-TCPGuard-Nonce: nonce-demo' \
  -H "X-TCPGuard-Timestamp: $(date +%s)" \
  http://127.0.0.1:18184/api/v1/transfers
```

Expected response:

- Status: `403 Forbidden`
- Headers: `X-TCPGuard-Decision: block`, `X-TCPGuard-Severity: critical`
- Compact body includes a safe block reason and request ID. Development mode includes rule `signed-transfer-replay-or-mitm`.
- Findings include `invalid_signature`

Response description: signed endpoints cannot be called with forged signatures.

### 18. Generate a valid transfer signature

Purpose: helper endpoint that signs the body exactly as the transfer endpoint expects.

```bash
curl -s -X POST --data '{"amount":100}' http://127.0.0.1:18184/_demo/sign
```

Expected response body:

```json
{"method":"POST","path":"/api/v1/transfers","signature":"<hex>","nonce":"nonce-...","timestamp":1760000000,"secret":"server-side only in real deployments"}
```

Response description: use `signature`, `nonce`, and `timestamp` with the exact same body in the next request.

### 19. Use the returned valid signature

Purpose: proves valid signed requests are allowed and replay-protected.

```bash
curl -i -X POST \
  -H 'Content-Type: application/json' \
  -H 'X-User-ID: manager-1' \
  -H 'X-TCPGuard-Signature: <signature>' \
  -H 'X-TCPGuard-Nonce: <nonce>' \
  -H 'X-TCPGuard-Timestamp: <timestamp>' \
  --data '{"amount":100}' \
  http://127.0.0.1:18184/api/v1/transfers
```

Expected response:

- First use: `200 OK`, body `{"ok":true,"message":"signed transfer accepted",...}`
- Reusing the same nonce: `403 Forbidden` with a compact replay/timestamp/signature reason. Development mode includes finding `nonce_reused` when emitted by TCPGuard.

Response description: signature validation passes once; nonce reuse is blocked as replay.

### 20. Audit chain, incidents, and metrics

Purpose: confirms operational visibility after running scenarios above.

```bash
curl -s http://127.0.0.1:18184/_demo/audit
curl -s http://127.0.0.1:18184/_demo/incidents
curl -s http://127.0.0.1:18184/_demo/metrics
```

Expected response:

- Audit: JSON object with `valid: true` and `envelopes`
- Incidents: JSON array of open incidents created by block/critical rules
- Metrics: JSON object with decision counts, detector counts, action counts, durations

Response description: audit envelopes are tamper-evident and metrics show enforcement activity.

### 21. Management API health and audit verification

Purpose: demonstrates secured management endpoints.

```bash
curl -i -H 'X-API-Key: dev-management-key' http://127.0.0.1:18185/health
curl -i -H 'X-API-Key: dev-management-key' http://127.0.0.1:18185/audit/verify
```

Expected response:

- Health: `200 OK` with `{"ok":true}`
- Audit verify: `200 OK` with `valid: true` unless audit data was tampered

Response description: management API requires the configured API key and provides operational control-plane checks.

### 22. Policy simulation/explanation through management API

Purpose: tests a request context without sending traffic through the application route.

```bash
curl -i -X POST \
  -H 'X-API-Key: dev-management-key' \
  -H 'Content-Type: application/json' \
  --data '{"event":{"type":"request.received"},"context":{"request":{"id":"sim-1","method":"GET","path":"/public","headers":{"User-Agent":"sqlmap"}},"network":{"ip":"203.0.113.10"},"security":{},"rate":{},"extra":{}}}' \
  http://127.0.0.1:18185/explain
```

Expected response:

- Status: `200 OK`
- Body includes compact simulation/explain output. Use development mode or the management API for deeper diagnostic fields such as matched rules, findings, evidence, audit hash, and policy metadata.

Response description: the management API explains why a synthetic request would be allowed, blocked, challenged, or throttled.

## CLI checks

Validate and lint the policy from the repository root:

```bash
go run ./cmd/tcpguard validate -dir ./examples/tcpguard-fh-server
go run ./cmd/tcpguard lint -dir ./examples/tcpguard-fh-server
go run ./cmd/tcpguard lint -strict -dir ./examples/tcpguard-fh-server
```

Expected response: JSON with policy validity and linter issues. Strict mode exits non-zero when warnings exist.

## Production notes

The demo uses an in-memory store for easy local execution. In production, use Redis or another shared store so abuse counters, nonce state, bans, audit state, approvals, incidents, entity profiles, and correlation windows are consistent across all application instances.

Use policy linting, simulation, audit verification, and staged reloads before deploying rule changes to production.
