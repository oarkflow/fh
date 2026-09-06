#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== fh / Fiber / fasthttp apples-to-apples benchmark ==="
echo "Building identical Go server workloads..."
(cd servers/go && go build -o fh-server ./fh && go build -o fiber-server ./fiber && go build -o fasthttp-server ./fasthttp)
echo "Running shared validation and keep-alive load harness..."
go run main.go "$@"
