# Signed transfer and replay protection

## Endpoint

`POST /api/v1/transfers`

## Purpose

Demonstrates TCPGuard HMAC signature validation, nonce replay detection, timestamp skew checks, and tamper protection.

## Create signature

```bash
BODY='{"to":"acct-2","amount":100}'
curl -s -X POST \
  -H 'X-Sign-Method: POST' \
  -H 'X-Sign-Path: /api/v1/transfers' \
  -d "$BODY" \
  http://127.0.0.1:18184/_demo/sign
```

Use the returned `signature`, `nonce`, and `timestamp`:

```bash
curl -i -X POST \
  -H 'X-TCPGuard-Signature: <signature>' \
  -H 'X-TCPGuard-Nonce: <nonce>' \
  -H 'X-TCPGuard-Timestamp: <timestamp>' \
  -d "$BODY" \
  http://127.0.0.1:18184/api/v1/transfers
```

## Expected response

Valid signed request:

```json
{"ok":true,"message":"signed transfer accepted"}
```

Reusing the same nonce or changing the body after signing should be blocked.
