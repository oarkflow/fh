# Runtime Execution Fabric (REF) - Complete Example Application

This example demonstrates how to build applications using the **Runtime Execution Fabric (REF)** in `fh`.

Rather than chaining sequential HTTP middleware (`Request -> MW1 -> MW2 -> MW3 -> Handler -> Response`), REF compiles your application into an intent-driven execution graph where:
1. **Facts are resolved by readiness**: Capabilities run concurrently as soon as their required inputs are satisfied.
2. **Speculative execution is safe**: Pure parsers, tenant resolution, and token verification execute before policy gates, while business operations and effects strictly wait for authorization.
3. **Mutations only occur through an authorized effect runtime**: Business logic (`Intent.Run`) returns a declarative `EffectPlan`. Side effects are committed atomically through two-phase local transactions and durable transactional outboxes.
4. **Transport Neutrality**: The exact same business intent can be dispatched from HTTP, gRPC, WebSocket frames, message queues, or CLI commands.

---

## Architecture of this Example

```text
                           HTTP Request / CLI / WS
                                      │
                                      ▼
                           Invocation (Input)
                                      │
                                      ▼
                        ┌─────────────┴─────────────┐
                        │  Compilation / Lookup     │
                        └─────────────┬─────────────┘
                                      │
                 ┌────────────────────┼────────────────────┐
                 ▼                    ▼                    ▼
             app.auth             app.tenant         order.create.decode
        (Bearer -> Principal) (Headers -> TenantID)   (JSON -> InputFact)
                 │                    │                    │
                 └────────────────────┼────────────────────┘
                                      ▼
                               app.policy.order
                     (Constraint Algebra: DENY-Dominates)
                                      │
                              EFFECT BARRIER
                                      │
                                      ▼
                            order.create.operation
                           (Pure Business Contract)
                                      │
                                      ▼
                            Two-Phase Effect Plan
                 ┌────────────────────┴────────────────────┐
                 ▼                                         ▼
         LocalTransactional                         DurableDelivery
    (DB Writes: Inventory & Order)           (Transactional Outbox: Email)
```

---

## Running the Example

### 1. Start the Server

```bash
go run ./examples/ref-app
```

Output:
```text
=================================================================
 Runtime Execution Fabric (REF) - Example Server
=================================================================
 Endpoints:
   POST /orders                -> Place order (Intent: order.create)
   GET  /ref/inspect/order.create -> Execution Graph Plan Summary
   GET  /ref/diagram           -> Mermaid DAG Diagram
=================================================================
```

### 2. Place an Order (HTTP)

```bash
curl -X POST http://localhost:8088/orders \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer valid-token" \
  -H "X-Tenant-ID: acme-corp" \
  -d '{"sku":"WIDGET-100","quantity":2,"price":49.99}'
```

Response:
```json
{
  "order_id": "ord-847291",
  "sku": "WIDGET-100",
  "quantity": 2,
  "total": 99.98,
  "tenant_id": "acme-corp",
  "user_id": "usr-alice",
  "created_at": "2026-09-21T20:30:00Z"
}
```

Server logs will confirm the two-phase effect execution:
```text
[DB-TX] Debited 2 units of SKU WIDGET-100 for tenant acme-corp
[DB-TX] Inserted Order ord-847291 ($99.98) for tenant acme-corp
[OUTBOX-WORKER] Dispatching confirmation email for Order ord-847291 to User usr-alice
[TELEMETRY] Metric orders.created = 1.00
```

### 3. Inspect the Execution Graph (JSON)

```bash
curl http://localhost:8088/ref/inspect/order.create
```

Returns the compiled DAG topological analysis, stage waves, effect barrier index, and node dependencies.

### 4. View the Mermaid Diagram

```bash
curl http://localhost:8088/ref/diagram
```

Copy the Mermaid output into any markdown viewer or [mermaid.live](https://mermaid.live) to see the interactive execution graph.

### 5. Run via CLI

The same intent can be invoked from the command line without starting an HTTP server:

```bash
go run ./examples/ref-app cli
```

Output:
```text
Running REF CLI Dispatch Demonstration...
CLI Response: {"order_id":"ord-123456","sku":"LAPTOP-X","quantity":1,"total":1299,"tenant_id":"default-org","user_id":"usr-guest","created_at":"..."}
```

---

## Code Breakdown

- [`intents.go`](./intents.go): The typed business intent `CreateOrderIntent` declaring required facts and returning an `Outcome` with an `EffectPlan`.
- [`capabilities.go`](./capabilities.go): Reusable fact producers (`auth`, `tenant`) and policy decision nodes using constraint algebra and obligations.
- [`effects.go`](./effects.go): Mutations classified into `LocalTransactional` (atomic DB writes with rollback compensation), `DurableDelivery` (transactional outbox with retry), and `FireAndForget` (telemetry).
- [`main.go`](./main.go): Application bootstrap, mounting HTTP routes, CLI handlers, and debug introspection.
