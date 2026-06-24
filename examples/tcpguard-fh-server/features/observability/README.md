# Observability endpoints

## Endpoints

- `GET /_demo/metrics`
- `GET /_demo/incidents`
- `GET /_demo/audit`
- `GET /_demo/approvals`

## Purpose

Expose TCPGuard runtime state from the demo: metrics snapshots, incidents, tamper-evident audit envelopes, and pending approvals.

## Curl

```bash
curl -i http://127.0.0.1:18184/_demo/metrics
curl -i http://127.0.0.1:18184/_demo/incidents
curl -i http://127.0.0.1:18184/_demo/audit
curl -i http://127.0.0.1:18184/_demo/approvals
```

## Expected response

Each endpoint returns JSON. `/_demo/audit` verifies the audit chain and returns `{"valid": true, "envelopes": [...]}` when the chain is intact.
