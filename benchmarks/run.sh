#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

echo "=== fh Cross-Language Benchmark Suite ==="
echo ""

# Install dependencies
echo "[setup] Checking dependencies..."

# Go servers
echo "[setup] Building Go servers..."
(cd servers/go && go mod tidy && go build ./fh ./fiber ./fasthttp) 2>&1 | tail -2

# Bombardier
BOMB=$(which bombardier 2>/dev/null || echo "/home/sujit/go/bin/bombardier")
if [ ! -f "$BOMB" ]; then
    echo "[setup] Installing bombardier..."
    go install github.com/codesenberg/bombardier@latest 2>/dev/null
    BOMB="$HOME/go/bin/bombardier"
fi
echo "[setup] Using $BOMB"
echo ""

# Run benchmark. Pin the harness (and its in-process raw HTTP driver for the
# /methods/* matrix) to the upper half of the cores; main.go pins the servers
# under test to the lower half. Without this split the load generator and the
# servers fight for the same cores and scheduler noise exceeds the real
# differences between frameworks.
echo "[run] Starting benchmarks..."
NPROC=$(nproc)
if command -v taskset >/dev/null 2>&1 && [ "$NPROC" -ge 4 ]; then
    taskset -c "$((NPROC / 2))-$((NPROC - 1))" go run main.go "$@"
else
    go run main.go "$@"
fi

echo ""
echo "Done! Check results/ for output."
