#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== fh / Fiber / fasthttp apples-to-apples benchmark ==="
echo "Building identical Go server workloads..."
bin_dir="$(mktemp -d "${TMPDIR:-/tmp}/fh-benchmark.XXXXXX")"
trap 'rm -rf "$bin_dir"' EXIT
(cd servers/go && go build -o "$bin_dir/fh-server" ./fh && go build -o "$bin_dir/fiber-server" ./fiber && go build -o "$bin_dir/fasthttp-server" ./fasthttp)
echo "Running shared validation and keep-alive load harness..."
go run main.go -bin-dir="$bin_dir" "$@"
