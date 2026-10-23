#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

if [[ "${1:-}" == "check" || "${1:-}" == "--check" || "${1:-}" == "test" || "${1:-}" == "--test" ]]; then
    exec ./check.sh
fi

echo "Starting fh production-resilient server..."
echo "Tip: Run './run.sh check' or './check.sh' to execute the live verification suite."
exec go run .
