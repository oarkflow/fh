# Function invocation abuse protection

## Endpoint

`POST /api/v1/functions/reconcile`

## Purpose

Demonstrates protection for workflow/function endpoints. TCPGuard tracks repeated invocation, endpoint scanning, API-key sharing, and abuse detector signals.

## Curl

```bash
for i in 1 2 3 4; do \
  curl -i -X POST \
    -H 'X-User-ID: user-1' \
    http://127.0.0.1:18184/api/v1/functions/reconcile; \
done
```

## Expected response

Initial requests can be allowed. Repeated abuse can trigger throttle/challenge/block depending on configured thresholds.
