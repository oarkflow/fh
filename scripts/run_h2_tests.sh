#!/usr/bin/env bash
set -euo pipefail

PKG="${PKG:-./...}"
FUZZ_PKG="${FUZZ_PKG:-.}"
TARGET_URL="${TARGET_URL:-https://127.0.0.1:8443/}"
H2SPEC_HOST="${H2SPEC_HOST:-127.0.0.1}"
H2SPEC_PORT="${H2SPEC_PORT:-8443}"

echo "== gofmt =="
gofmt -w h2_*.go 2>/dev/null || true

echo "== compile + unit tests =="
go test "$PKG" -count=1 -run 'TestH2' -v

echo "== race tests =="
go test "$PKG" -count=1 -race -run 'TestH2Race|TestH2' -v

echo "== benchmarks =="
go test "$PKG" -run '^$' -bench 'BenchmarkH2' -benchmem -count=3

echo "== fuzz smoke tests =="
go test "$FUZZ_PKG" -run '^$' -fuzz FuzzH2HeaderFragment -fuzztime=10s
go test "$FUZZ_PKG" -run '^$' -fuzz FuzzH2ReadFrame -fuzztime=10s
go test "$FUZZ_PKG" -run '^$' -fuzz FuzzH2ValidateRequestFields -fuzztime=10s
go test "$FUZZ_PKG" -run '^$' -fuzz FuzzH2ValidateRequestTrailers -fuzztime=10s

if command -v h2spec >/dev/null 2>&1; then
  echo "== h2spec =="
  h2spec -h "$H2SPEC_HOST" -p "$H2SPEC_PORT" -t -k
else
  echo "== h2spec skipped: install h2spec and run: h2spec -h $H2SPEC_HOST -p $H2SPEC_PORT -t -k =="
fi

if [ -f ./cmd/h2load-lite/main.go ]; then
  echo "== load smoke test =="
  go run ./cmd/h2load-lite -url "$TARGET_URL" -n 5000 -c 100 -k=true
else
  echo "== load smoke skipped: copy cmd/h2load-lite into repo root first =="
fi
