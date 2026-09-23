# Runtime Execution Fabric (REF)

The **Runtime Execution Fabric (REF)** is a compiled, capability-driven, transport-neutral application execution runtime built into `fh`.

Rather than processing requests through linear, sequential middleware pipelines (`Request -> MW1 -> MW2 -> Handler`), REF compiles application business logic into a **dependency-readiness directed acyclic graph (DAG)** with typed facts, algebraic policy evaluation, and controlled two-phase effect lifecycles.

---

## The Problem: The 6 Fatal Flaws of Traditional HTTP Pipelines

Traditional HTTP servers (including Gin, Echo, Fiber, FastHTTP, and conventional `fh`) rely on a 20-year-old architectural pattern: **the linear middleware chain**.

```text
Request ──▶ Middleware 1 ──▶ Middleware 2 ──▶ Middleware 3 ──▶ Handler ──▶ Response
```

This model introduces six severe architectural bottlenecks in production systems:

| # | Traditional Pipeline Flaw | Production Failure / Bottleneck |
|---|---|---|
| **1** | **Pipeline Tyranny (Serial Latency)** | Independent operations (Auth, Tenant, Rate Limit, Body Parse) must run one after another, summing their latencies ($3\text{ms} + 4\text{ms} + 3\text{ms} + 2\text{ms} = 12\text{ms}$). |
| **2** | **The Shared Mutable Context ("String-Soup")** | Middleware pass data via untyped string-keyed maps (`c.Locals("tenant_id", val)`). Typo bugs fail silently at runtime; zero compile-time safety. |
| **3** | **Protocol Entanglement (Transport Lock-In)** | Business handlers are hardcoded to `fh.Ctx`, HTTP headers, and HTTP status codes. Logic cannot be reused over gRPC, WebSockets, or Queues. |
| **4** | **The Uncontrolled Mutation Trap** | Handlers execute direct database writes or external API calls mid-flight. A crash halfway through leaks corrupted, unrecoverable state. |
| **5** | **The Speculation Dilemma** | Pipelines cannot pre-fetch data or decode payloads concurrently with authorization policies without custom goroutine gymnastics. |
| **6** | **Fragile Security & Black-Box Evaluation** | Policies evaluate serially; first allow calls `c.Next()`; conflicting tenant contexts overwrite each other silently; zero dataflow visibility. |

---

## How REF Solves Every Flaw: Direct Mechanics & Code

### 1. Fixing Serial Latency: Dependency-Readiness DAG Scheduling

#### In Traditional Servers:
Middleware are chained in a sequential slice. Even though authentication, tenant resolution, and JSON body decoding do not depend on each other, they are forced to run serially:
```go
// Conventional: Total latency = 4ms (Auth) + 3ms (Tenant) + 2ms (Decode) = 9ms
app.Use(AuthMiddleware)       // 4ms
app.Use(TenantMiddleware)     // 3ms
app.Use(BodyParserMiddleware) // 2ms
```

#### How REF Fixes It:
In REF, capabilities declare their exact inputs (`Requires: []fact.AnyKey`) and outputs (`Provides: []fact.AnyKey`). During startup, the compiler builds a directed acyclic graph (DAG) and computes dependency counters:

```text
Traditional Pipeline:  [ Auth (4ms) ] ──▶ [ Tenant (3ms) ] ──▶ [ Decode (2ms) ] ──▶ Total: 9ms

REF Readiness DAG:    ┌─▶ [ Auth (4ms) ] ──────────┐
                      ├─▶ [ Tenant (3ms) ] ────────┼──▶ Operation ──▶ Total: 4ms
                      └─▶ [ Decode (2ms) ] ────────┘
```

At runtime, the [`Scheduler`](file:///Users/sujit/Sites/fh/ref/execution/scheduler.go) evaluates **dependency readiness, not stage order**:
- All nodes with `0` initial dependencies execute **concurrently** in parallel worker waves.
- When a capability finishes, it atomically decrements the dependency counter of downstream nodes (`remaining.Add(-1)`).
- As soon as a downstream node hits `0`, it is launched immediately.
- **Result:** Latency drops from the sum of all middleware to the **critical path length**.

---

### 2. Fixing Context String-Soup: Typed Fact Keys & Dense Plan Slots

#### In Traditional Servers:
Data is passed between middleware using untyped strings:
```go
// Traditional: No compile-time check, runtime type assertion panics
c.Locals("tenant_id", "acme-corp")
...
tenantID := c.Locals("tenantId").(string) // Bug: typo "tenantId" fails at runtime!
```

#### How REF Fixes It:
REF introduces **strongly typed Fact Keys** with compile-time type parameters and stable definition IDs:

```go
// 1. Declare typed fact key at package level
var TenantKey = fact.NewKey[string]("app.tenant_id")
var PrincipalKey = fact.NewKey[PrincipalFact]("auth.principal")

// 2. Publish with compile-time type safety (zero string hashing)
ref.Publish(nc, TenantKey, "acme-corp") // Type-checked by Go compiler!

// 3. Require returns typed T directly, no type assertions needed
tenantID, err := ref.Require(nc, TenantKey) // Returns string, not any!
```

#### Runtime Architecture:
- At compile time, every unique fact key in an intent is assigned a contiguous index: `PlanSlot(0..N-1)`.
- At runtime, [`fact.Store`](file:///Users/sujit/Sites/fh/ref/fact/store.go) is a pooled array of slots (`slots []any`) with atomic release-acquire synchronization flags (`flags []atomic.Uint32`).
- Reading a fact is an array index access: $O(1)$, zero string hashing, zero locks, zero allocations.

---

### 3. Fixing Protocol Lock-In: Pure Intent Contracts & Transport Projections

#### In Traditional Servers:
Business handlers are coupled directly to the HTTP protocol:
```go
// Traditional: Tied to HTTP status codes, headers, and fh.Ctx
func HandleCreateOrder(c fh.Ctx) error {
    token := c.Get("Authorization")
    var in OrderInput
    if err := c.BodyParser(&in); err != nil { return c.Status(400).SendString("Bad JSON") }
    order := domain.CreateOrder(token, in)
    return c.Status(201).JSON(order)
}
```
If you need to call this same business logic from gRPC, a WebSocket frame, or a Kafka queue worker, you have to rewrite the handler or build fake HTTP requests.

#### How REF Fixes It:
In REF, business logic is written as a pure, transport-neutral [`Intent[I, O]`](file:///Users/sujit/Sites/fh/ref/intent/intent.go):

```go
type CreateOrderIntent struct{}

func (CreateOrderIntent) Name() ref.IntentName { return "order.create" }

func (CreateOrderIntent) Spec() ref.Spec {
    return ref.Spec{
        Requires: []fact.AnyKey{capability.PrincipalKey.Any(), TenantKey.Any()},
    }
}

// Pure business logic: No HTTP headers, no status codes, no fh.Ctx!
func (CreateOrderIntent) Run(nc *ref.NodeContext, in OrderInput) (ref.Outcome[OrderOutput], error) {
    user, _ := ref.Require(nc, capability.PrincipalKey)
    tenant, _ := ref.Require(nc, TenantKey)

    return ref.Outcome[OrderOutput]{
        Value: OrderOutput{OrderID: "ord-100", Total: in.Price * float64(in.Quantity)},
    }, nil
}
```

#### Transport Projections:
The exact same intent can be mounted across any protocol without changing a single line of business logic:
```go
// 1. Mount to HTTP
app.Post("/orders", refHttp.Adapter(engine, "order.create"))

// 2. Mount to gRPC
grpcServer.RegisterService(&OrderServiceDesc, refGrpc.UnaryHandler(engine, "order.create"))

// 3. Mount to WebSocket frames
wsHandler := refWs.Handler(engine)

// 4. Mount to RabbitMQ / Kafka Queue Consumer
queueConsumer := refQueue.Consumer(engine, "order.create")

// 5. Mount to Command-Line CLI
cliCmd := refCli.Command(engine, "order.create")
```

---

### 4. Fixing Uncontrolled Mutations: Two-Phase Crash-Safe Transactional Outbox

#### In Traditional Servers:
Handlers perform direct, uncoordinated mutations mid-flight:
```go
// Traditional: Direct I/O inside handler leads to corrupted partial state on crash
func HandleOrder(c fh.Ctx) error {
    db.Exec("UPDATE inventory SET qty = qty - 1") // Mutates DB!
    // If process dies here or network fails:
    stripe.Charge(...)            // Lost!
    mailer.SendConfirmation(...)  // Lost!
    return c.SendString("OK")
}
```

#### How REF Fixes It:
Operations in REF **never perform direct mutations**. Instead, `Intent.Run` returns an [`EffectPlan`](file:///Users/sujit/Sites/fh/ref/effect/effect.go#L51) describing mutations across three delivery tiers:
- `LocalTransactional`: Atomic database writes.
- `DurableDelivery`: External webhooks, emails, payment calls.
- `FireAndForget`: Best-effort metrics/telemetry.

```go
func (CreateOrderIntent) Run(nc *ref.NodeContext, in OrderInput) (ref.Outcome[OrderOutput], error) {
    return ref.Outcome[OrderOutput]{
        Value: OrderOutput{OrderID: "ord-100"},
        Effects: ref.EffectPlan{
            LocalTx: []ref.Effect{
                &DebitInventoryEffect{SKU: in.SKU, Qty: in.Quantity},
                &InsertOrderEffect{OrderID: "ord-100"},
            },
            Durable: []ref.Effect{
                &SendEmailEffect{OrderID: "ord-100", UserID: user.ID}, // Transactional Outbox!
            },
            FireAndForget: []ref.Effect{
                &EmitMetricEffect{Metric: "orders.count", Value: 1},
            },
        },
    }, nil
}
```

#### Crash-Safe Outbox Guarantee:
In [`effect.CommitPlan`](file:///Users/sujit/Sites/fh/ref/effect/effect.go#L90), durable outbox records are inserted into the **same database transaction** *before* local writes and commit:
```text
1. BEGIN DB TX
2.   Insert Durable Outbox Records into DB (Email, Webhook)
3.   Execute Local Transactional Writes in DB (Inventory, Order)
4. COMMIT DB TX ──▶ (Atomically commits writes AND outbox records together!)
5. Schedule delivery worker
```
If the server crashes after step 4, **no state is lost**: the background outbox worker reloads the unfulfilled records from the database on startup and delivers them with retry and idempotency.

---

### 5. Fixing the Speculation Dilemma: Compiler-Enforced Speculation Classes

#### In Traditional Servers:
If authorization takes 15ms (e.g. querying OPA or checking a permissions database), request body decoding and read-only pre-fetching are stalled until authorization finishes, wasting precious CPU idle time.

#### How REF Fixes It:
REF allows nodes to declare their [`SpeculationClass`](file:///Users/sujit/Sites/fh/ref/graph/node.go#L43-L50):
- `PreAuthSafe`: Safe before authentication (e.g., pure JSON payload decoding, token extraction).
- `PostIdentitySafe`: Safe once principal identity is known, but before permission checks complete.
- `PostPolicySafe`: Strictly requires all policy decisions to have passed.
- `NoSpeculation`: Default. Waits for complete authorization.

#### Startup Invariant Enforcement:
REF does not trust speculation as a runtime hint. During `engine.Compile()`, [`enforceSpeculationInvariants`](file:///Users/sujit/Sites/fh/ref/runtime/engine.go#L249) verifies that:
- Any node marked `PostIdentitySafe` has a verified transitive path from an identity-producing node.
- Any node marked `PostPolicySafe` has a verified transitive path from all decision nodes.
- **If an unsafe configuration is detected, compilation fails at startup.**

---

### 6. Fixing Fragile Security: Algebraic Policy Gates & Contradiction Detection

#### In Traditional Servers:
Middleware evaluate security serially. Conflicting tenant headers or permissions can overwrite context keys silently:
```go
// Traditional: Last writer wins, confused deputy risk
c.Locals("tenant", "org-A") // Set by header
...
c.Locals("tenant", "org-B") // Overwritten by nested middleware!
```

#### How REF Fixes It:
In REF, security policies are first-class [`DecisionNode`](file:///Users/sujit/Sites/fh/ref/graph/node.go#L13)s evaluated through [`DecisionSet`](file:///Users/sujit/Sites/fh/ref/execution/decision.go):
1. **DENY Dominates:** Any single `RecordDeny` halts execution immediately.
2. **All Policies Must Pass:** `VerdictAllow` is returned **only when `completed >= required && !denied`**. Partial allows never unblock the Effect Barrier.
3. **Constraint Intersection:** Access constraints (allowed regions, tenant IDs, field projections) are algebraically intersected.
4. **Contradiction Detection:** If Policy 1 requires `tenant = "org-A"` and Policy 2 requires `tenant = "org-B"`, REF detects the contradiction and **automatically converts the verdict to `VerdictDeny`**.

---

## Performance and benchmark report

Do not treat the earlier direct-dispatch microbenchmark as an HTTP comparison. REF adds transport projection, input decoding, scheduling, fact handling, and effect lifecycle work. The cost depends on the graph: small CPU-only handlers currently favor a direct FH handler; independent latency-bound reads can overlap in REF. See the [reproducible FH-versus-REF parity report](./BENCHMARK_REPORT.md) for measured latency, allocations, loopback load results, commands, and limitations.

---

## Core Packages

| Package | Role | Key Types |
|---|---|---|
| [`ref/invocation`](./invocation/) | Transport-neutral, deeply immutable input | `Invocation`, `Input`, `PrincipalHint`, `HTTPMeta`, `GRPCMeta` |
| [`ref/fact`](./fact/) | Two-level typed fact identity & zero-allocation dense store | `Key[T]`, `DefinitionID`, `PlanSlot`, `Store` |
| [`ref/graph`](./graph/) | Descriptive execution graph & topological planner | `Node`, `NodeKind`, `SpeculationClass`, `Plan`, `Compile` |
| [`ref/execution`](./execution/) | Concurrency-safe readiness scheduler & decision algebra | `Scheduler`, `NodeContext`, `DecisionSet`, `Budget`, `Verdict` |
| [`ref/capability`](./capability/) | Reusable fact producers & security policies | `Registry`, `AuthCapability`, `TenantCapability`, `RateLimitCapability` |
| [`ref/effect`](./effect/) | Two-phase effect runtime & transactional outbox | `Effect`, `CompensatingEffect`, `EffectPlan`, `CommitPlan`, `Runner` |
| [`ref/intent`](./intent/) | Strongly-typed domain contracts | `Intent[I, O]`, `Outcome[T]`, `Failure`, `Category`, `Register` |
| [`ref/runtime`](./runtime/) | Top-level coordinator & compiler | `Engine`, `Dispatch`, `Compile`, `Plan` |
| [`ref/transport`](./transport/) | Transport adapters & projections | `http.Adapter`, `grpc.UnaryHandler`, `websocket.Handler`, `queue.Consumer`, `cli.Command` |
| [`ref/debug`](./debug/) | Graph introspection & live visualizers | `InspectPlan`, `ToMermaid`, `ToDOT`, `HTTPHandler` |
| [`ref/testing`](./testing/) | Transport-independent intent testing harness | `TestBuilder`, `Given`, `AssertFact`, `AssertOutcome` |

---

## Contract-led HTTP modernization

REF can serve as an incremental modernization layer: keep existing endpoints live, describe replacement routes as contracts, compare read-only candidate results, and shift traffic by a stable canary percentage.

Route declarations support named query, header, cookie, and path parameters through repeated parameter blocks. Intent input and output shapes are the source of truth for validation and documentation. After loading a platform, generate artifacts directly from the compiled document:

```go
if err := platform.WriteOpenAPI(file); err != nil { return err }
clientSource := platform.TypeScriptClient()
```

OpenAPI() returns the OpenAPI 3.1 document as data, TypeScriptTypes() and TypeScriptClient() generate client artifacts, and GoContractSmokeTest("contracttest") emits a deployed-route smoke test scaffold that fails on server errors (it does not replace schema-aware assertions). CompareContracts(old, next) reports removed routes, incompatible shape changes, and newly required inputs. MountArtifacts(app, "/openapi.json", "/contracts.ts") is optional; expose those endpoints only where the contract is meant to be public. The existing ref/debug package renders execution plans as Mermaid and Graphviz DOT.

For a framework-native rollout, CanaryFH(legacy, candidate, percent) deterministically routes a percentage of requests to the new handler. ShadowFH(legacy, http.EnginePreview(engine, intent, route), ...) serves the legacy response and, after a successful legacy response, compares status plus a digest of the candidate response. The preview refuses plans with explicit effect nodes and does not commit returned effect plans. **Only use it with trusted read-only Go capabilities:** Go code can still perform side effects from a node declared pure or read, so the runtime cannot prove those callbacks harmless. Shadow comparison records hashes rather than response bodies.

This gives teams a route-by-route migration path. It does not automatically translate arbitrary legacy handlers, prove third-party callbacks pure, or make an in-memory effect store durable; those concerns remain explicit at the integration boundary.
## Documentation & Examples

- [**docs/runtime-execution-fabric.md**](../docs/runtime-execution-fabric.md): Detailed architectural whitepaper explaining the mathematical and systems foundations of REF.
- [**examples/ref-app/**](../examples/ref-app/): Complete runnable application with HTTP, CLI, and Mermaid diagram endpoints.
