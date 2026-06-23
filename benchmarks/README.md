# fh Cross-Language Benchmark Suite

Benchmarks comparing **fh** against HTTP frameworks from Go, Python, Node.js, PHP, Java, and C++ across identical workloads.

## Included Servers

| Server | Language | Port | Framework |
|--------|----------|------|-----------|
| **fh** | Go | 3001 | [fh](https://github.com/oarkflow/fh) (zero-dependency) |
| **gin** | Go | 3002 | [Gin](https://github.com/gin-gonic/gin) |
| **fiber** | Go | 3003 | [Fiber](https://github.com/gofiber/fiber) |
| **fasthttp** | Go | 3004 | [fasthttp](https://github.com/valyala/fasthttp) |
| **net/http** | Go | 3005 | Go standard library |
| **FastAPI** | Python | 3101 | [FastAPI](https://fastapi.tiangolo.com/) + uvicorn |
| **Flask** | Python | 3102 | [Flask](https://flask.palletsprojects.com/) |
| **Express** | Node.js | 3201 | [Express](https://expressjs.com/) |
| **Fastify** | Node.js | 3202 | [Fastify](https://fastify.dev/) |
| **Slim** | PHP | 3301 | [Slim 4](https://www.slimframework.com/) |
| **Spring Boot** | Java | 3501 | [Spring Boot 3](https://spring.io/projects/spring-boot) |
| **Drogon** | C++ | 3401 | [Drogon](https://github.com/drogonframework/drogon) |

## Scenarios

| Scenario | Method | Path | Description |
|----------|--------|------|-------------|
| Plaintext | GET | `/plaintext` | Return static string `"Hello, World!"` |
| JSON | GET | `/json` | Return `{"message":"Hello, World!"}` |
| Params | GET | `/users/42` | Route parameter extraction + JSON response |
| Query | GET | `/search?q=benchmark` | Query string parameter + JSON response |
| Echo | POST | `/echo` | Parse JSON body and echo back |
| Users | GET | `/users` | Serialize array of 100 user objects |

## Requirements

| Tool | Required for |
|------|-------------|
| Go 1.26+ | Go servers + benchmark runner |
| Python 3.12+ | FastAPI, Flask servers |
| Node.js 24+ | Express, Fastify servers |
| PHP 8+ | Slim server |
| Maven 3+ | Spring Boot server |
| CMake 3.14+ | Drogon server (C++) |
| [bombardier](https://github.com/codesenberg/bombardier) | HTTP benchmark tool (auto-installed) |

## Quick Start

```bash
cd benchmarks
bash run.sh
```

This installs dependencies, starts each server, runs all 6 scenarios with 100 concurrent connections for 10 seconds each, and prints a comparison table.

### Custom Configuration

```bash
# Run with 50 connections for 5 seconds
bash run.sh -c 50 -d 5

# Benchmark only specific servers
bash run.sh --server fh
bash run.sh --server gin --server fiber

# Show help
bash run.sh -h
```

## Results

Results are saved as JSON to `results/bench_YYYYMMDD_HHMMSS.json`.

Example table output (fh on a modern Linux workstation, 50 concurrent connections, 3s duration):

```
--- Plaintext (GET) ---
Server                RPS   Avg Lat (ms)   P50 (ms)   P95 (ms)   P99 (ms)   Errors
----------------------------------------------------------------------------------
fh                 343280          0.144      0.110      0.332      0.655        0

--- JSON (GET) ---
Server                RPS   Avg Lat (ms)   P50 (ms)   P95 (ms)   P99 (ms)   Errors
----------------------------------------------------------------------------------
fh                 302848          0.163      0.122      0.385      0.862        0

--- Params (GET) ---
Server                RPS   Avg Lat (ms)   P50 (ms)   P95 (ms)   P99 (ms)   Errors
----------------------------------------------------------------------------------
fh                 300388          0.165      0.124      0.401      0.800        0

--- Query (GET) ---
Server                RPS   Avg Lat (ms)   P50 (ms)   P95 (ms)   P99 (ms)   Errors
----------------------------------------------------------------------------------
fh                 226721          0.218      0.140      0.622      1.446        0

--- Echo (POST) ---
Server                RPS   Avg Lat (ms)   P50 (ms)   P95 (ms)   P99 (ms)   Errors
----------------------------------------------------------------------------------
fh                 214674          0.231      0.160      0.648      1.495        0

--- Users (GET) ---
Server                RPS   Avg Lat (ms)   P50 (ms)   P95 (ms)   P99 (ms)   Errors
----------------------------------------------------------------------------------
fh                 185824          0.267      0.175      0.859      1.690        0
```

## Architecture

```
benchmarks/
├── README.md              # This file
├── main.go                # Benchmark runner (Go)
├── go.mod                 # Runner module
├── run.sh                 # Shell convenience script
├── results/               # Benchmark result JSON files
├── servers/
│   ├── go/                # Go servers (shared go.mod)
│   │   ├── fh/main.go
│   │   ├── gin/main.go
│   │   ├── fiber/main.go
│   │   ├── fasthttp/main.go
│   │   └── nethttp/main.go
│   ├── python/
│   │   ├── fastapi/       # FastAPI + uvicorn
│   │   └── flask/         # Flask
│   ├── nodejs/
│   │   ├── express/       # Express.js
│   │   └── fastify/       # Fastify
│   ├── php/
│   │   └── slim/          # Slim 4
│   ├── java/
│   │   └── springboot/    # Spring Boot 3 + Maven
│   └── cpp/
│       └── drogon/        # Drogon + CMake
└── scenarios/
    └── payloads/          # JSON payload files
```

## Adding a New Server

1. Create a directory under `servers/<lang>/<name>/`
2. Implement the 6 endpoints (Plaintext, JSON, Params, Query, Echo, Users)
3. Add a `Server` entry in `main.go` with the correct port, start command, and language
4. Run `bash run.sh --server <name>` to test

## Notes

- All servers run on `127.0.0.1` with keep-alive enabled
- Benchmarks use [bombardier](https://github.com/codesenberg/bombardier) for consistent cross-language measurement
- Results may vary significantly based on hardware, OS, and runtime versions
- For best results, run each benchmark multiple times and average
