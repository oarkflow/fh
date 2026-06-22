# Real-World Middleware Examples

These runnable applications give every first-class `fh/mw` package a concrete job. The examples favor realistic boundaries over isolated “hello middleware” snippets.

## Featured examples

| Example | Scenario |
|---|---|
| [`secure-api-middleware-stack`](secure-api-middleware-stack/) | Public catalog, partner API, browser account, signed webhook, operations endpoints, static assets, and a circuit-broken upstream gateway. |
| [`workflow-reliable-checkout`](workflow-reliable-checkout/) | Idempotent checkout with per-cart serialization, lifecycle hooks, sequential workflow steps, and durable fulfillment handoff. |

The directory also contains focused reliability examples for email, orders, payments, webhooks, imports, file processing, fanout, tenancy, administration, and gateways.

## Complete middleware coverage

| Middleware | Real-world use |
|---|---|
| `actor` | Serializes checkout attempts by cart in `workflow-reliable-checkout`. |
| `apikey` | Authenticates partner shipment clients in `secure-api-middleware-stack`. |
| `apiversion` | Enforces catalog and partner API versions and emits deprecation metadata. |
| `basicauth` | Protects the human operations endpoint. |
| `bodylimit` | Bounds request memory at the public edge. |
| `cache` | Caches only safe public catalog responses and varies by API version. |
| `circuitbreaker` | Stops calls to a repeatedly failing catalog upstream. |
| `compress` | Compresses sufficiently large edge responses. |
| `contract` | Enforces the partner shipment request contract. |
| `correlationid` | Propagates cross-service correlation IDs. |
| `cors` | Allows the known browser application origin. |
| `csrf` | Protects cookie-authenticated account mutations. |
| `earlydata` | Rejects replayable unsafe TLS early-data requests. |
| `idempotency` | Normalizes a mobile checkout retry token. |
| `ipwhitelist` | Restricts the metrics endpoint to loopback networks. |
| `lifecycle` | Logs checkout events and compensates inventory after a later failure. |
| `logger` | Produces structured edge access logs. |
| `metrics` | Measures requests and serves Prometheus/JSON metrics. |
| `policy` | Attaches catalog data-handling and version policy. |
| `proxy` | Forwards the gateway route to a configurable upstream. |
| `ratelimiter` | Limits public traffic while skipping health/static probes. |
| `recover` | Converts panics at the outer edge boundary. |
| `reliability` | Journals, deduplicates, replays, and queues checkout/restock mutations. |
| `replay` | Rejects reused webhook nonces. |
| `requestid` | Validates or creates edge request IDs. |
| `rewrite` | Preserves a legacy catalog URL through an internal rewrite. |
| `security` | Adds hardened browser response headers. |
| `session` | Maintains signed browser login state. |
| `signature` | Verifies timestamped payment webhook HMAC signatures. |
| `skip` | Exempts health/static traffic and private routes from inappropriate global policies. |
| `static` | Serves an asset with ETag and cache metadata. |
| `timeout` | Bounds end-to-end edge request duration. |
| `workflow` | Composes checkout validation, reservation, payment, and fulfillment handoff. |

## Run

From the repository root:

```bash
go run ./examples/realworld/secure-api-middleware-stack
go run ./examples/realworld/workflow-reliable-checkout
```

See each example README for requests to try and configuration details.
