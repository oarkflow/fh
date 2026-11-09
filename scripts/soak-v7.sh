#!/usr/bin/env bash
# Operator-run WAN / carrier / overnight soak. Never claim it ran unless
# the generated JSONL evidence and test environment have been reviewed.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $# -lt 5 ]]; then
  echo 'usage: soak-v7.sh TCP_ENDPOINT UDP_ENDPOINT CERT_PEM KEY_PEM CA_PEM [hours=24] [output=soak-v7.ndjson]' >&2
  exit 2
fi
tcp="$1"; udp="$2"; cert="$3"; key="$4"; ca="$5"
hours="${6:-24}"; output="${7:-soak-v7.ndjson}"
if ! [[ "$hours" =~ ^[0-9]+$ ]] || (( hours < 1 || hours > 72 )); then echo 'hours must be 1..72' >&2; exit 2; fi
binary="$(mktemp)"; trap 'rm -f "$binary"' EXIT
go build -trimpath -o "$binary" ./cmd/mira-bench
end=$(( $(date +%s) + hours*3600 ))
while (( $(date +%s) < end )); do
  result="$(mktemp)"; trap 'rm -f "$binary" "$result"' EXIT
  "$binary" -tcp "$tcp" -udp "$udp" -cert "$cert" -key "$key" -ca "$ca" -clients 4 -messages 250 -bytes 256 -timeout 2m -output "$result"
  python3 - "$result" "$output" <<'PY'
import json, sys, datetime
src, dst = sys.argv[1:]
with open(src) as f:
    row=json.load(f)
row['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
with open(dst,'a') as f:
    f.write(json.dumps(row,sort_keys=True)+'\n')
if row['failures'] or row['completed'] != row['clients']*row['messages']:
    raise SystemExit('SOAK FAILED: incomplete or failed sessions')
PY
  rm -f "$result"
  sleep 300
done
printf 'soak completed without observed benchmark failures; inspect %s before drawing conclusions\n' "$output"
