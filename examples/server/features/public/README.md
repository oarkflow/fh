# Public endpoint protection

## Endpoint

`GET /public`

## Purpose

Demonstrates the route-level TCPGuard middleware path. Clean requests pass through to the endpoint, while global rules such as banned users, bad IPs, tenant lockdown, and rate limits can still enforce before the handler runs.

## Middleware

The endpoint explicitly registers `tcpguardmw.MiddlewareWithConfig(...)` in its own route handler chain. TCPGuard evaluates the full HTTP request context before the endpoint handler runs.

## Curl

```bash
curl -i http://127.0.0.1:18184/public
```

## Expected clean response

```json
{
  "ok": true,
  "message": "clean request allowed"
}
```

## Block example

```bash
curl -i -H 'X-User-ID: banned-user' http://127.0.0.1:18184/public
```

Expected status: `403 Forbidden` with `X-TCPGuard-Decision: block` and a safe TCPGuard response body.
