# fh apples-to-apples benchmark results

**Date:** 2026-07-13

**Configuration:** Linux x86_64, 100 persistent connections, 3 seconds per sample, 3 rotating rounds, median RPS reported

**Compared servers:** fh, Fiber, and fasthttp

These results use ordinary framework handlers. JSON endpoints parse and/or serialize JSON on every request; no cached payloads, static response helpers, or benchmark-only response APIs are used. All servers use the same HTTP/1 throughput profile, and a preflight gate verifies byte-identical bodies and equivalent media types before measurement.

The full run completed with zero request errors for every supported route. Fiber does not register the non-standard `QUERY` method and is reported as unsupported for that row.

| Scenario | Method | fh RPS | Next RPS | Next server | fh lead |
|---|---:|---:|---:|---|---:|
| Plaintext | GET | 341,509 | 296,046 | fasthttp | 15.4% |
| JSON | GET | 283,358 | 270,044 | fasthttp | 4.9% |
| Params | GET | 289,109 | 287,479 | fasthttp | 0.6% |
| Query string | GET | 315,699 | 280,182 | fasthttp | 12.7% |
| Echo | POST | 277,828 | 234,435 | fasthttp | 18.5% |
| Users array | GET | 203,916 | 175,154 | fasthttp | 16.4% |
| Method GET | GET | 370,718 | 349,322 | fasthttp | 6.1% |
| Method HEAD | HEAD | 395,469 | 380,197 | fasthttp | 4.0% |
| Method POST | POST | 385,531 | 354,980 | fasthttp | 8.6% |
| Method PUT | PUT | 378,477 | 330,438 | Fiber | 14.5% |
| Method PATCH | PATCH | 371,501 | 323,531 | fasthttp | 14.8% |
| Method DELETE | DELETE | 418,203 | 344,335 | fasthttp | 21.5% |
| Method OPTIONS | OPTIONS | 419,643 | 374,103 | fasthttp | 12.2% |
| Method CONNECT | CONNECT | 381,751 | 377,741 | fasthttp | 1.1% |
| Method TRACE | TRACE | 372,097 | 350,284 | Fiber | 6.2% |
| Method QUERY | QUERY | 371,434 | 329,940 | fasthttp | 12.6% |

fh ranked first in all 16 scenarios on this machine. The narrow Params and CONNECT margins should be treated as close results, not a universal performance guarantee; hardware, OS scheduling, Go versions, and workload shape can change rankings.

Raw runner output: `results/bench_20260713_143411.json`.
