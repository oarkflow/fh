#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
echo '[1/7] gofmt check'
if find . -name '*.go' -print0 | xargs -0 gofmt -l | grep .; then echo 'unformatted source' >&2; exit 1; fi
echo '[2/7] build'
go build ./...
echo '[3/7] vet'
go vet ./...
echo '[4/7] full test suite'
go test ./... -count=1 -timeout 100s
echo '[5/7] focused race suite'
go test -race ./gateway ./delegation ./cluster ./durable ./mira -count=1 -timeout 120s
echo '[6/7] independent conformance'
python3 -m unittest discover -s conformance -p 'test_wire_v4.py' -v
echo '[7/7] TypeScript typechecking'
if command -v tsc >/dev/null 2>&1; then tsc --noEmit --target ES2020 --module ES2020 --lib ES2020,DOM browser/client.ts; else echo 'SKIPPED: tsc unavailable' >&2; fi
echo 'verified v6 checks successful'
