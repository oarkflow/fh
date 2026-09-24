# Comprehensive REF + fh Integration Example

This example demonstrates a **Real-World E-Commerce Checkout API** built using the `fh` web framework and the `ref` execution engine.

It showcases the strict interface-driven decoupling between the frameworks, while heavily utilizing every advanced feature the `ref` architecture provides.

## Features Demonstrated

1. **Capabilities & Graph Execution**: 
   An isolated `CheckInventory` capability evaluates stock logic concurrently based on graph dependencies.
2. **Sagas (Distributed Transactions)**:
   The checkout process uses the `saga` orchestrator to contact Payment and Shipping services. If payment fails, the Saga automatically triggers the compensation (Rollback) functions for all succeeded steps.
3. **CQRS & Event Sourcing**:
   The engine uses a Tier-0 Lock-Free `EventBus`. Observers listen for `OrderPlacedEvent` and `OrderFailedEvent` to simulate asynchronous Read-Model projections.
4. **Execution Budgets & Load Shedding**:
   The Intent is configured with strict timeouts and memory limits (`2s`, `5MB`). If it exceeds these, the engine aborts the execution automatically.
5. **Decoupled Architecture (`refAdapter`)**:
   `fh` maintains zero direct dependency on `ref`. The application injects an `any`-based adapter into the framework.

## How to Run

1. Start the server:
   ```bash
   go run main.go
   ```

2. Open a new terminal and try the following scenarios:

   **Scenario 1: Successful Checkout**
   ```bash
   curl -X POST -H "Content-Type: application/json" -d '{"user_id":"tony","item_id":"book","qty":1}' http://localhost:8080/api/checkout
   ```
   *Notice the CQRS projection and Saga execution steps in the server logs.*

   **Scenario 2: Capability Failure (Out of Stock)**
   ```bash
   curl -X POST -H "Content-Type: application/json" -d '{"user_id":"tony","item_id":"book","qty":99}' http://localhost:8080/api/checkout
   ```
   *Notice the business error returned, and the `OrderFailedEvent` broadcast via CQRS.*

   **Scenario 3: Saga Rollback (Card Declined)**
   ```bash
   curl -X POST -H "Content-Type: application/json" -d '{"user_id":"poor-user","item_id":"book","qty":1}' http://localhost:8080/api/checkout
   ```
   *Notice the Saga starting, the payment failing, and the automatic ROLLBACK functions executing to revert the state.*
