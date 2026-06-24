# Report export protection

## Endpoint

`POST /api/v1/reports/export`

## Purpose

Demonstrates sensitive export detection, export velocity, body-size/sensitivity signals, and challenge/block decisions for possible data exfiltration.

## Curl

```bash
curl -i -X POST \
  -H 'X-User-ID: user-1' \
  -H 'X-Sensitivity: high' \
  -d '{"format":"csv","rows":50000}' \
  http://127.0.0.1:18184/api/v1/reports/export
```

## Expected response

Allowed exports return:

```json
{"ok":true,"message":"export started"}
```

Sensitive or repeated exports can return a TCPGuard challenge or block response.
