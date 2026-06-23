#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

echo "=== fh Cross-Language Benchmark Suite ==="
echo ""

# Install dependencies
echo "[setup] Checking dependencies..."

# Go servers
echo "[setup] Building Go servers..."
(cd ../servers/go && go mod tidy && go build ./fh ./gin ./fiber ./fasthttp ./nethttp) 2>&1 | tail -2

# Python
if command -v python3 &>/dev/null; then
    echo "[setup] Python available"
    pip3 install -q -r ../servers/python/fastapi/requirements.txt 2>/dev/null || true
    pip3 install -q -r ../servers/python/flask/requirements.txt 2>/dev/null || true
fi

# Node.js
if command -v node &>/dev/null; then
    echo "[setup] Node.js $(node --version)"
    for dir in ../servers/nodejs/*/; do
        (cd "$dir" && npm install --silent 2>/dev/null) || true
    done
fi

# PHP
if command -v php &>/dev/null; then
    echo "[setup] PHP available"
    (cd ../servers/php/slim && composer install --quiet 2>/dev/null) || true
fi

# Bombardier
BOMB=$(which bombardier 2>/dev/null || echo "/home/sujit/go/bin/bombardier")
if [ ! -f "$BOMB" ]; then
    echo "[setup] Installing bombardier..."
    go install github.com/codesenberg/bombardier@latest 2>/dev/null
    BOMB="$HOME/go/bin/bombardier"
fi
echo "[setup] Using $BOMB"
echo ""

# Run benchmark
echo "[run] Starting benchmarks..."
go run main.go "$@"

echo ""
echo "Done! Check results/ for output."
