# Auth abuse event endpoints

## Endpoints

- `POST /_demo/auth/fail`
- `POST /_demo/auth/success`

## Purpose

These endpoints explicitly emit TCPGuard events for authentication telemetry. They demonstrate failed-login velocity, password spray, credential stuffing, and correlated account-takeover chains.

## Curl

```bash
for u in a b c d; do \
  curl -i -X POST \
    -H 'X-Forwarded-For: 203.0.113.25' \
    -H "X-User-ID: $u" \
    http://127.0.0.1:18184/_demo/auth/fail; \
done
```

## Expected response

Early events can monitor or allow. After thresholds are crossed, TCPGuard returns a challenge, throttle, or block response depending on the matched rule. The body remains safe in production and includes a `request_id` for support/debugging.
