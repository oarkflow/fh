#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
log(){ printf '\n==== %s ====\n' "$*"; }
log 'Format/build/vet'
test -z "$(find . -name '*.go' -print0 | xargs -0 gofmt -l)"
go build ./...
go vet ./...
log 'Go tests: complete'
go test ./... -count=1 -timeout 120s
log 'Go race tests: affected packages'
go test -race ./delegation ./gateway ./mira ./resume ./cluster -count=1 -timeout 150s
log 'Loss/migration/IPv6 regression'
go test ./mira -run 'TestReliabilityWithPacketLoss|TestMTUBlackholeRecoveryRefragmentsReliableMessage|TestRebindAndWaitConfirmsNewPath|TestIPv6LoopbackEncryptedFullDuplex' -count=5 -timeout 120s
log 'Independent Python frame codec'
python3 -m unittest discover -s conformance -p 'test_wire_v4.py' -v
log 'Independent Python encrypted peer'
if python3 -c 'import cryptography' 2>/dev/null; then
  test_binary="$(mktemp)"
  trap 'rm -f "$test_binary"' EXIT
  go build -o "$test_binary" ./cmd/mira-echo
  MIRA_V7_ECHO="$test_binary" python3 -m unittest discover -s conformance -p 'test_peer_v7.py' -v
else
  echo 'SKIPPED Python encrypted peer: pip install cryptography to enable' >&2
fi
log 'Browser client TypeScript'
if command -v tsc >/dev/null 2>&1; then tsc --noEmit --target ES2020 --module ES2020 --lib ES2020,DOM browser/client.ts; else echo 'SKIPPED: tsc missing' >&2; fi
log 'Complete local v7 verification passed (external release gates remain open)'
