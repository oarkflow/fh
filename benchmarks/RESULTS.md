# fh Benchmark Results — Go HTTP Frameworks

**Date:** 2026-06-23  
**Tool:** [bombardier](https://github.com/codesenberg/bombardier)  
**Configuration:** 100 concurrent connections, 5-second duration per scenario  
**Hardware:** Linux x86_64, Go 1.26.4

## Overall Winner

| Category | Winner | RPS | Margin |
|----------|--------|-----|--------|
| **Plaintext** | `fasthttp` / `fiber` | ~320K RPS | Near tie (top 3 within 3%) |
| **JSON** | `fasthttp` | 328,336 RPS | 9% ahead of #2 |
| **Route Params** | `fasthttp` | 324,733 RPS | 4% ahead of #2 |
| **Query String** | `fasthttp` | 288,359 RPS | Tie with `fh` |
| **POST Echo** | `fasthttp` | 314,692 RPS | 40% ahead of #2 (raw bytes) |
| **Users Array** | `net/http` | 213,619 RPS | 25% ahead of #2 |

**Grand Champion: `fasthttp`** (wins 4 of 6 scenarios)  
**Best All-Rounder: `fh`** (top 3 in every scenario, zero-dependency codebase)

---

## Detailed Results

### 1. Plaintext — `GET /plaintext`

Returning the static string `"Hello, World!"`.

| Rank | Server | RPS | Avg Lat | P50 | P95 | P99 | vs #1 |
|------|--------|-----|---------|-----|-----|-----|-------|
| 🥇 | fiber | 320,119 | 0.310ms | 0.241ms | 0.774ms | 1.362ms | — |
| 🥈 | fasthttp | 317,561 | 0.313ms | 0.242ms | 0.789ms | 1.448ms | -0.8% |
| 🥉 | **fh** | **312,216** | **0.318ms** | **0.240ms** | **0.826ms** | **1.536ms** | -2.5% |
| 4 | net/http | 218,426 | 0.456ms | 0.305ms | 1.418ms | 2.495ms | -31.8% |
| 5 | gin | 191,973 | 0.519ms | 0.327ms | 1.706ms | 2.991ms | -40.0% |

> **Analysis:** Top 3 are within 3% — effectively a tie. `fh` has the lowest P50 latency.

### 2. JSON — `GET /json`

Returning `{"message":"Hello, World!"}` as JSON.

| Rank | Server | RPS | Avg Lat | P50 | P95 | P99 | vs #1 |
|------|--------|-----|---------|-----|-----|-----|-------|
| 🥇 | fasthttp | 328,336 | 0.303ms | 0.242ms | 0.739ms | 1.297ms | — |
| 🥈 | **fh** | **301,640** | **0.329ms** | **0.236ms** | **0.870ms** | **1.892ms** | -8.1% |
| 🥉 | fiber | 281,498 | 0.353ms | 0.258ms | 0.938ms | 1.945ms | -14.3% |
| 4 | net/http | 188,887 | 0.528ms | 0.348ms | 1.714ms | 2.613ms | -42.5% |
| 5 | gin | 182,300 | 0.546ms | 0.350ms | 1.828ms | 2.969ms | -44.5% |

> **Analysis:** fasthttp leads with raw JSON string building. `fh` with `encoding/json` serialization performs well at #2.

### 3. Route Params — `GET /users/42`

Extracting a route parameter and returning `{"name":"User 42"}`.

| Rank | Server | RPS | Avg Lat | P50 | P95 | P99 | vs #1 |
|------|--------|-----|---------|-----|-----|-----|-------|
| 🥇 | fasthttp | 324,733 | 0.306ms | 0.242ms | 0.756ms | 1.331ms | — |
| 🥈 | **fh** | **310,690** | **0.320ms** | **0.246ms** | **0.805ms** | **1.495ms** | -4.3% |
| 🥉 | fiber | 303,494 | 0.327ms | 0.244ms | 0.827ms | 1.556ms | -6.5% |
| 4 | net/http | 198,286 | 0.502ms | 0.334ms | 1.627ms | 2.548ms | -38.9% |
| 5 | gin | 195,042 | 0.510ms | 0.336ms | 1.640ms | 2.780ms | -39.9% |

> **Analysis:** fasthttp's raw string slicing is fastest. `fh`'s radix tree router keeps it at #2.

### 4. Query String — `GET /search?q=benchmark`

Parsing a query parameter and returning `{"query":"benchmark"}`.

| Rank | Server | RPS | Avg Lat | P50 | P95 | P99 | vs #1 |
|------|--------|-----|---------|-----|-----|-----|-------|
| 🥇 | fasthttp | 288,359 | 0.344ms | 0.267ms | 0.885ms | 1.535ms | — |
| 🥇 | **fh** | **288,171** | **0.345ms** | **0.248ms** | **0.914ms** | **1.963ms** | -0.1% |
| 🥉 | fiber | 260,362 | 0.381ms | 0.282ms | 1.024ms | 2.097ms | -9.7% |
| 4 | net/http | 170,917 | 0.583ms | 0.378ms | 1.879ms | 2.887ms | -40.7% |
| 5 | gin | 168,033 | 0.593ms | 0.381ms | 1.979ms | 3.188ms | -41.7% |

> **Analysis:** fasthttp and `fh` are effectively tied (0.1% difference). Both outperform the rest by ~10%.

### 5. POST Echo — `POST /echo`

Parsing a JSON body `{"message":"Hello, World!"}` and echoing it back.

| Rank | Server | RPS | Avg Lat | P50 | P95 | P99 | vs #1 |
|------|--------|-----|---------|-----|-----|-----|-------|
| 🥇 | fasthttp | 314,692 | 0.315ms | 0.249ms | 0.771ms | 1.393ms | — |
| 🥈 | fiber | 225,092 | 0.442ms | 0.315ms | 1.245ms | 2.496ms | -28.5% |
| 🥉 | **fh** | **215,255** | **0.462ms** | **0.297ms** | **1.460ms** | **3.004ms** | -31.6% |
| 4 | gin | 147,599 | 0.675ms | 0.428ms | 2.258ms | 3.566ms | -53.1% |
| 5 | net/http | 141,789 | 0.703ms | 0.435ms | 2.268ms | 3.389ms | -54.9% |

> **Analysis:** fasthttp's raw `PostBody()` pass-through is unmatched here. `fh` with JSON decoding/encoding is #3 but competitive.

### 6. Users Array — `GET /users`

Serializing a JSON array of 100 user objects.

| Rank | Server | RPS | Avg Lat | P50 | P95 | P99 | vs #1 |
|------|--------|-----|---------|-----|-----|-----|-------|
| 🥇 | net/http | 213,619 | 0.466ms | 0.315ms | 1.442ms | 2.556ms | — |
| 🥈 | **fh** | **170,549** | **0.584ms** | **0.367ms** | **1.978ms** | **3.490ms** | -20.2% |
| 🥉 | fiber | 164,149 | 0.607ms | 0.369ms | 2.079ms | 3.407ms | -23.2% |
| 4 | gin | 134,046 | 0.743ms | 0.478ms | 2.430ms | 3.621ms | -37.2% |
| 5 | fasthttp | 16,554 | 6.039ms | 4.681ms | 18.425ms | 27.498ms | -92.3% |

> **Analysis:** net/http wins due to `json.Encoder` streaming directly to `http.ResponseWriter`. fasthttp's string-concatenation JSON builder causes severe slowdown. `fh` places #2 with proper JSON encoding.

---

## Head-to-Head: fh vs Fiber

| Scenario | fh (RPS) | Fiber (RPS) | Winner |
|----------|----------|-------------|--------|
| Plaintext | 312,216 | 320,119 | Fiber (+2.5%) |
| JSON | 301,640 | 281,498 | **fh** (+7.2%) |
| Params | 310,690 | 303,494 | **fh** (+2.4%) |
| Query | 288,171 | 260,362 | **fh** (+10.7%) |
| Echo POST | 215,255 | 225,092 | Fiber (+4.6%) |
| Users Array | 170,549 | 164,149 | **fh** (+3.9%) |

**fh leads 4–2 vs Fiber**, despite being a zero-dependency framework.

## Head-to-Head: fh vs Gin

| Scenario | fh (RPS) | Gin (RPS) | Winner |
|----------|----------|-----------|--------|
| Plaintext | 312,216 | 191,973 | **fh** (+62.6%) |
| JSON | 301,640 | 182,300 | **fh** (+65.5%) |
| Params | 310,690 | 195,042 | **fh** (+59.3%) |
| Query | 288,171 | 168,033 | **fh** (+71.5%) |
| Echo POST | 215,255 | 147,599 | **fh** (+45.8%) |
| Users Array | 170,549 | 134,046 | **fh** (+27.2%) |

**fh sweeps 6–0 vs Gin** with substantial margins.

---

## Scoring Summary

| Server | Plaintext | JSON | Params | Query | Echo | Users | Avg Rank |
|--------|-----------|------|--------|-------|------|-------|----------|
| **fasthttp** | 🥈 (#2) | 🥇 (#1) | 🥇 (#1) | 🥇 (#1) | 🥇 (#1) | 5th | **1.8** |
| **fh** | 🥉 (#3) | 🥈 (#2) | 🥈 (#2) | 🥇 (#1) | 🥉 (#3) | 🥈 (#2) | **2.2** |
| **fiber** | 🥇 (#1) | 🥉 (#3) | 🥉 (#3) | 🥉 (#3) | 🥈 (#2) | 🥉 (#3) | **2.5** |
| **net/http** | 4th | 4th | 4th | 4th | 5th | 🥇 (#1) | **3.7** |
| **gin** | 5th | 5th | 5th | 5th | 4th | 4th | **4.7** |

---

## Key Takeaways

1. **fh** is the best zero-dependency Go HTTP framework — competitive with fasthttp/fiber while having zero external imports.
2. **fasthttp** is fastest for raw throughput but suffers on complex serialization (users array was 10× slower due to string concatenation).
3. **fiber** is competitive across all scenarios but never #1 on any except plaintext.
4. **net/http** is surprisingly strong on the users array endpoint thanks to streaming JSON encoder.
5. **gin** is consistently the slowest of the Go frameworks tested but still fast enough for most applications.

---

*Generated by fh Benchmarks runner. Results saved to `results/bench_20260623_113217.json`.*
