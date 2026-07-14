# Benchmark Results

## Overview

This document summarizes the supplied `fh` cross-language benchmark run comparing:

- `fh`
- `Fiber`
- `fasthttp`

The benchmark suite used `bombardier` and validated that successful responses from all servers matched before measurements were collected.

## Test Configuration

| Setting | Value |
|---|---:|
| Concurrent connections | 100 |
| Duration per test | 10 seconds |
| Rounds | 3 |
| Servers | 3 |
| Benchmark client | `bombardier` |
| Response validation | Successful responses matched |
| Errors for supported tests | 0 |

> The source log does not include CPU model, operating system, Go version, compiler flags, process affinity, power settings, or memory/allocation measurements. Results should therefore be treated as a performance snapshot of the recorded environment, not as a universal ranking.

## Executive Summary

`fh` delivered the highest reported throughput in **15 of 16 scenarios**. The only throughput loss was parameterized routing, where `fh` was approximately **1.56%** behind Fiber.

Across the six application-oriented tests—plaintext, JSON, path parameters, query parsing, echo, and users—`fh` averaged approximately **1.078 million requests/second**, compared with **1.046 million** for fasthttp and **1.030 million** for Fiber.

Across the HTTP method-routing tests, `fh` averaged approximately **1.474 million requests/second**, around **6.4% faster than fasthttp** and **8.4% faster than Fiber** using the reported values. Fiber did not successfully serve the custom `QUERY` method and recorded 100 errors for that scenario.

The strongest `fh` advantages appeared in the more involved application tests:

- **Users:** 17.59% more throughput than the next-fastest server.
- **Echo:** 6.74% more throughput than the next-fastest server.
- **Query:** 0.99% more throughput than the next-fastest server.
- **Plaintext:** 1.98% more throughput than the next-fastest server.

## Application Workloads

| Scenario | Winner | fh RPS | Fiber RPS | fasthttp RPS | fh vs. best alternative | fh avg latency |
|---|---|---:|---:|---:|---:|---:|
| Plaintext `GET` | **fh** | 1,208,429 | 1,184,937 | 1,179,073 | **+1.98%** | 0.082 ms |
| JSON `GET` | **fh** | 1,178,284 | 1,163,668 | 1,168,060 | **+0.88%** | 0.084 ms |
| Params `GET` | Fiber | 1,188,403 | 1,207,180 | 1,204,856 | **−1.56%** | 0.083 ms |
| Query `GET` | **fh** | 1,170,520 | 1,139,252 | 1,159,043 | **+0.99%** | 0.084 ms |
| Echo `POST` | **fh** | 1,122,423 | 990,237 | 1,051,504 | **+6.74%** | 0.088 ms |
| Users `GET` | **fh** | 602,399 | 493,697 | 512,271 | **+17.59%** | 0.165 ms |

### Application-workload averages

| Server | Mean throughput | Relative to fh |
|---|---:|---:|
| **fh** | **1,078,410 req/s** | — |
| fasthttp | 1,045,801 req/s | fh **+3.12%** |
| Fiber | 1,029,828 req/s | fh **+4.72%** |

These averages are simple arithmetic means across workloads with substantially different response behavior. They are useful as a compact overview but should not replace per-scenario analysis.

## HTTP Method Routing

| Method | Winner | fh RPS | Fiber RPS | fasthttp RPS | fh vs. best alternative | fh avg latency |
|---|---|---:|---:|---:|---:|---:|
| `GET` | **fh** | 1,486,912 | 1,367,223 | 1,397,772 | **+6.38%** | 0.065 ms |
| `HEAD` | **fh** | 1,489,491 | 1,374,546 | 1,396,705 | **+6.64%** | 0.066 ms |
| `POST` | **fh** | 1,452,052 | 1,362,292 | 1,379,656 | **+5.25%** | 0.067 ms |
| `PUT` | **fh** | 1,451,121 | 1,344,193 | 1,378,033 | **+5.30%** | 0.067 ms |
| `PATCH` | **fh** | 1,456,290 | 1,350,687 | 1,362,820 | **+6.86%** | 0.067 ms |
| `DELETE` | **fh** | 1,457,527 | 1,349,877 | 1,375,570 | **+5.96%** | 0.067 ms |
| `OPTIONS` | **fh** | 1,478,288 | 1,359,063 | 1,398,542 | **+5.70%** | 0.066 ms |
| `CONNECT` | **fh** | 1,490,333 | 1,374,378 | 1,383,020 | **+7.76%** | 0.066 ms |
| `TRACE` | **fh** | 1,489,653 | 1,354,933 | 1,393,178 | **+6.92%** | 0.066 ms |
| `QUERY` | **fh** | 1,486,588 | 0 | 1,383,780 | **+7.43%** | 0.066 ms |

### Method-routing averages

| Server | Mean throughput | Relative to fh | Notes |
|---|---:|---:|---|
| **fh** | **1,473,826 req/s** | — | All methods completed without errors |
| fasthttp | 1,384,908 req/s | fh **+6.42%** | All methods completed without errors |
| Fiber | 1,359,688 req/s | fh **+8.39%** | Excludes unsupported `QUERY` result from the mean |

## Latency Comparison

### Application workloads

| Scenario | fh avg | fh P50 | fh P95 | fh P99 | Lowest reported P99 |
|---|---:|---:|---:|---:|---|
| Plaintext | 0.082 ms | 0.069 ms | 0.184 ms | 0.254 ms | **fh** |
| JSON | 0.084 ms | 0.068 ms | 0.198 ms | 0.284 ms | fasthttp, 0.282 ms |
| Params | 0.083 ms | 0.068 ms | 0.192 ms | 0.267 ms | Fiber, 0.254 ms |
| Query | 0.084 ms | 0.068 ms | 0.199 ms | 0.287 ms | **fh**, 0.287 ms |
| Echo | 0.088 ms | 0.072 ms | 0.198 ms | 0.312 ms | **fh** |
| Users | 0.165 ms | 0.132 ms | 0.412 ms | 0.558 ms | **fh** |

`fh` had the lowest P99 latency in four of the six application workloads. JSON and parameter routing were the exceptions.

### Method routing

For all supported method-routing cases, `fh` reported average latency between **0.065 ms and 0.067 ms**. Its P99 latency ranged from **0.182 ms to 0.191 ms**, lower than both alternatives in every method-routing comparison.

## Detailed Reported Results

### Plaintext `GET`

| Server | RPS | Avg | P50 | P95 | P99 | Errors |
|---|---:|---:|---:|---:|---:|---:|
| **fh** | **1,208,429** | **0.082 ms** | 0.069 ms | **0.184 ms** | **0.254 ms** | 0 |
| Fiber | 1,184,937 | 0.083 ms | 0.069 ms | 0.192 ms | 0.262 ms | 0 |
| fasthttp | 1,179,073 | 0.084 ms | 0.069 ms | 0.194 ms | 0.266 ms | 0 |

### JSON `GET`

| Server | RPS | Avg | P50 | P95 | P99 | Errors |
|---|---:|---:|---:|---:|---:|---:|
| **fh** | **1,178,284** | **0.084 ms** | **0.068 ms** | 0.198 ms | 0.284 ms | 0 |
| fasthttp | 1,168,060 | **0.084 ms** | 0.070 ms | 0.190 ms | **0.282 ms** | 0 |
| Fiber | 1,163,668 | 0.085 ms | 0.071 ms | **0.187 ms** | 0.297 ms | 0 |

### Params `GET`

| Server | RPS | Avg | P50 | P95 | P99 | Errors |
|---|---:|---:|---:|---:|---:|---:|
| Fiber | **1,207,180** | **0.082 ms** | 0.068 ms | **0.185 ms** | **0.254 ms** | 0 |
| fasthttp | 1,204,856 | **0.082 ms** | **0.067 ms** | 0.191 ms | 0.265 ms | 0 |
| fh | 1,188,403 | 0.083 ms | 0.068 ms | 0.192 ms | 0.267 ms | 0 |

### Query `GET`

| Server | RPS | Avg | P50 | P95 | P99 | Errors |
|---|---:|---:|---:|---:|---:|---:|
| **fh** | **1,170,520** | **0.084 ms** | **0.068 ms** | 0.199 ms | **0.287 ms** | 0 |
| fasthttp | 1,159,043 | 0.085 ms | 0.072 ms | **0.187 ms** | 0.289 ms | 0 |
| Fiber | 1,139,252 | 0.087 ms | 0.071 ms | 0.194 ms | 0.306 ms | 0 |

### Echo `POST`

| Server | RPS | Avg | P50 | P95 | P99 | Errors |
|---|---:|---:|---:|---:|---:|---:|
| **fh** | **1,122,423** | **0.088 ms** | **0.072 ms** | **0.198 ms** | **0.312 ms** | 0 |
| fasthttp | 1,051,504 | 0.094 ms | 0.079 ms | 0.210 ms | 0.339 ms | 0 |
| Fiber | 990,237 | 0.100 ms | 0.080 ms | 0.236 ms | 0.389 ms | 0 |

### Users `GET`

| Server | RPS | Avg | P50 | P95 | P99 | Errors |
|---|---:|---:|---:|---:|---:|---:|
| **fh** | **602,399** | **0.165 ms** | **0.132 ms** | **0.412 ms** | **0.558 ms** | 0 |
| fasthttp | 512,271 | 0.194 ms | 0.136 ms | 0.522 ms | 0.946 ms | 0 |
| Fiber | 493,697 | 0.201 ms | 0.139 ms | 0.548 ms | 1.029 ms | 0 |

## Interpretation

### Strengths demonstrated by fh

1. **Consistent routing throughput**
   `fh` led all ten method-routing scenarios, generally by 5–8% over the next-fastest implementation.

2. **Strong application-path performance**
   The largest gains appeared where request handling involved more work than returning a static response, particularly the echo and users workloads.

3. **Low tail latency**
   `fh` produced the lowest reported P99 latency in most application workloads and every method-routing workload.

4. **Broad method support**
   `fh` and fasthttp completed the custom `QUERY` method test. Fiber returned no successful throughput and recorded 100 errors in the supplied result.

5. **Correctness under the tested load**
   The benchmark reported matching successful responses and zero errors for every supported `fh` test.


## Conclusion

In this recorded run, `fh` was the overall throughput leader and delivered the best method-routing performance. It won 15 of 16 scenarios, maintained zero errors in all its tests, and showed particularly strong gains for echo, users, and general HTTP method dispatch.

The results support the conclusion that `fh` is highly competitive with Fiber and fasthttp for the tested workloads. They do not by themselves establish production superiority across different hardware, operating systems, traffic patterns, middleware stacks, protocol modes, or security configurations. Parameterized routing remains the principal measured optimization opportunity.
