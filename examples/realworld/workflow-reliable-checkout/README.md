# Workflow Reliable Checkout

This example composes five middleware packages around a realistic checkout:

- `idempotency` normalizes a mobile-specific retry token.
- `reliability` journals the mutation, rejects body drift, and replays completed responses.
- `actor` serializes requests for the same cart to prevent double reservation.
- `lifecycle` records start, failure, and completion hooks, and releases reserved stock when a later step fails.
- `workflow` validates, reserves inventory, authorizes payment, and atomically hands fulfillment to the durable queue.

It also includes a typed, asynchronous reliable endpoint for inventory restocks.

## Run

```bash
go run ./examples/realworld/workflow-reliable-checkout -addr :3000
```

## Try it

```bash
curl -i -H 'Content-Type: application/json' -H 'Idempotency-Key: checkout-001' \
  -d '{"cart_id":"cart-42","sku":"notebook","quantity":2,"payment_token":"tok_demo"}' \
  http://localhost:3000/checkouts

# Repeat exactly: the stored response is replayed without reserving stock twice.
curl -i -H 'Content-Type: application/json' -H 'Idempotency-Key: checkout-001' \
  -d '{"cart_id":"cart-42","sku":"notebook","quantity":2,"payment_token":"tok_demo"}' \
  http://localhost:3000/checkouts

curl -i -H 'Content-Type: application/json' -H 'Idempotency-Key: restock-001' \
  -d '{"sku":"notebook","quantity":10}' http://localhost:3000/inventory/restocks
curl -i http://localhost:3000/inventory
curl -i http://localhost:3000/queue/stats
```

Runtime state is stored under `.fh-data`. Use unique idempotency keys when starting a new operation.
