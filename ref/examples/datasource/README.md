# REF DataSource Subsystem Showcase

This example demonstrates how to use the `ref/source` package to resolve common data fetching bottlenecks (such as database query amplification, redundant API calls, and thundering herds) natively within the REF execution engine.

## Problem Context

When a complex execution graph contains multiple nodes that fetch data independently, or when high concurrency causes multiple identical requests to be processed at the same time, systems often suffer from:

1. **N+1 Query Problems:** 5 nodes requesting 5 different users trigger 5 separate `SELECT` queries to the database.
2. **Redundant Fetches:** The exact same configuration or tenant setting is fetched repeatedly from a remote API.
3. **Thundering Herds:** A cache expires, and 100 simultaneous incoming requests all attempt to fetch the same data from the database at the exact same moment.

## How REF Solves This

The `ref/source` package introduces a universal optimization layer for any external I/O (Databases, REST APIs, Object Storage, Caches, etc.). By attaching a `source.Spec` to a capability, REF automatically applies the following optimizations:

### 1. DataLoader (Batching)
The `source.Loader` collapses multiple independent key requests made within the same invocation into a single batched fetch. 
* **Showcased in Scenario 1:** 5 concurrent user requests are captured within a 10ms window and dispatched as a single database query `SELECT ... WHERE id IN (...)`.

### 2. Multi-Level Scoped Caching
The `source.ProcessCache` provides an L1 memory cache. The cache keys strictly enforce authorization scopes (`TenantID` and `PrincipalID`) to prevent cross-tenant data leaks by design.
* **Showcased in Scenario 2:** A subsequent request for the same tenant's settings avoids the API entirely and is served from the L1 cache.

### 3. In-flight Request Coalescing
The `source.Coalescer` deduplicates identical active requests. If 100 requests arrive for the exact same key simultaneously, only the first request actually executes the fetch. The other 99 requests wait for and share the same result.
* **Showcased in Scenario 3:** 100 concurrent requests simulate a Thundering Herd. The Coalescer intercepts them, resulting in exactly **1 API call**.

## Running the Example

Run the showcase to see the metrics and bottleneck resolutions in action:

```bash
cd ref/examples/datasource
go run main.go
```

### Expected Output

```text
=== REF DataSource Subsystem Showcase ===
Showcasing: DataLoader (Batching), L1 Caching, and Coalescing

--- Scenario 1: First Request (Cold Cache, 5 concurrent user loads) ---
[API] Fetching settings for tenant: tenant-acme
[DB] Executing query for 5 users: [105 101 102 103 104]
Database Queries Executed: 1 (Expected: 1, due to DataLoader batching!)
API Calls Executed: 1 (Expected: 1)

--- Scenario 2: Second Request (Hot Cache for Tenant API) ---
[DB] Executing query for 2 users: [202 201]
Database Queries Executed: 2 (Expected: 2, new users fetched in 1 batch)
API Calls Executed: 1 (Expected: 1, Served from ProcessCache!)

--- Scenario 3: Thundering Herd (100 simultaneous requests for same settings) ---
[API] Fetching settings for tenant: tenant-stark
[DB] Executing query for 1 users: [999]
API Calls Executed for 100 concurrent requests: 1 (Expected: 1, due to Coalescer!)

--- Source Metrics Summary ---
Source: users-db     | Ops: 102 | Cache Hits:  0 | Avg Latency: 60ms
Source: tenant-api   | Ops: 93  | Cache Hits:  1 | Avg Latency: 100ms
```

## Key Code Highlights

* `source.NewBatchCapability`: Wraps a function that accepts `[]K` and returns `map[K]V`, automatically providing DataLoader batching.
* `source.NewFetchCapability`: Wraps a single-item fetch function with optional Cache and Coalescer integration.
* `source.MetricsCollector`: Integrates with REF's observer pattern to track `QueueWait`, `ExecTime`, `DecodeTime`, cache hits, and batch sizes transparently.
