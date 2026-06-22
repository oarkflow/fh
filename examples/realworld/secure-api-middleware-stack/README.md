# Secure API Middleware Stack

This example is a route-oriented production stack, not a “turn everything on” toy. Each middleware sits at the boundary where it is useful: public caching, partner API contracts, browser CSRF/session protection, signed replay-safe webhooks, protected metrics, and a circuit-broken reverse proxy.

## Run

```bash
go run ./examples/realworld/secure-api-middleware-stack -addr :3000
```

Optional environment variables are `SESSION_SECRET`, `PARTNER_API_KEY`, `WEBHOOK_SECRET`, `ADMIN_USER`, and `ADMIN_PASSWORD`. Development defaults are included only so the example runs immediately.

## Try it

```bash
curl -i http://localhost:3000/legacy/catalog
curl -i -H 'Accept-Encoding: gzip' -H 'Accept-Version: 2026-01' http://localhost:3000/api/catalog
curl -i -H 'X-API-Key: partner-demo-key' -H 'Accept-Version: v2' \
  -H 'X-Partner-Request-ID: ship-42' -H 'Content-Type: application/json' \
  -d '{"order_id":"ord_42"}' http://localhost:3000/partner/shipments
curl -i -u admin:change-me http://localhost:3000/admin
curl -i http://localhost:3000/assets/app.txt
curl -i 'http://localhost:3000/_internal/metrics?format=prometheus'
```

For `/account/login`, first `GET /account`, then send the returned cookie and `csrf_token` as `X-CSRF-Token`. For webhook calls, sign `<unix timestamp>.<body>` with HMAC-SHA256 and `WEBHOOK_SECRET`, then send the digest as `X-Signature` and a unique `X-Nonce`.

The proxy route forwards `/gateway/*` to `-upstream` (default `http://127.0.0.1:4000`) and opens its circuit after three failures.
