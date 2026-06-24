# Payment approval protection

## Endpoint

`POST /api/v1/payments/approve`

## Purpose

Demonstrates business-rule enforcement for high-value payments, after-hours actions, user role, and risk score.

## Curl

```bash
curl -i -X POST \
  -H 'X-User-ID: manager-1' \
  -H 'X-User-Role: manager' \
  -H 'X-Business-Amount: 1500000' \
  -H 'X-Outside-Hours: true' \
  http://127.0.0.1:18184/api/v1/payments/approve
```

## Expected response

High-risk approvals can create incidents or approvals and return a challenge/block response. Clean allowed response:

```json
{"ok":true,"message":"payment approved"}
```
