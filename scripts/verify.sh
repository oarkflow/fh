#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
echo '[1/8] Formatting'
unformatted="$(find . -name '*.go' -print0 | xargs -0 gofmt -l)"
if [[ -n "$unformatted" ]]; then echo "$unformatted"; exit 1; fi
echo '[2/8] Go vet'
go vet ./...
echo '[3/8] Build'
go build ./...
echo '[4/8] Full tests'
go test ./... -count=1 -timeout 120s
echo '[5/8] Race detector'
go test -race ./... -count=1 -timeout 120s
echo '[6/8] Simulated loss/blackhole stress'
go test ./mira -run 'TestReliabilityWithPacketLoss|TestMTUBlackholeRecoveryRefragmentsReliableMessage|TestRebindAndWaitConfirmsNewPath' -count=10 -timeout 110s
echo '[7/8] Python independent wire parser'
python3 -m unittest discover -s conformance -p 'test_wire_v4.py' -v
echo '[8/8] Browser TypeScript compile'
if command -v tsc >/dev/null 2>&1; then
  tsc --noEmit --target ES2020 --module ES2020 --lib ES2020,DOM browser/client.ts
else
  echo 'TypeScript compiler not installed; optional check skipped'
fi
printf '\nAll available local verification gates passed. External security/WAN/cluster gates are NOT included.\n'
