#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:18184}"
MGMT_URL="${MGMT_URL:-http://127.0.0.1:18185}"
MGMT_KEY="${TCPGUARD_MGMT_API_KEY:-dev-management-key}"
VERIFY_DETAILS="${VERIFY_DETAILS:-false}"

failures=0

request() {
  curl -sS -i "$@" || true
}

status_of() { awk 'NR==1{print $2}'; }

check_status() {
  local name="$1" expected="$2"
  shift 2
  local out status
  out="$(request "$@")"
  status="$(printf '%s' "$out" | status_of)"
  if [[ "$status" != "$expected" ]]; then
    printf 'FAIL %-42s expected=%s got=%s\n%s\n\n' "$name" "$expected" "${status:-none}" "$out"
    failures=$((failures+1))
    return 0
  fi
  printf 'PASS %-42s status=%s\n' "$name" "$status"
}

check_decision() {
  local name="$1" expected_status="$2" expected_decision="$3" detail_needle="${4:-}"
  shift 4 || true
  local out status
  out="$(request "$@")"
  status="$(printf '%s' "$out" | status_of)"
  if [[ "$status" != "$expected_status" || "$out" != *"X-TCPGuard-Decision: $expected_decision"* ]]; then
    printf 'FAIL %-42s expected_status=%s got=%s expected_decision=%s\n%s\n\n' "$name" "$expected_status" "${status:-none}" "$expected_decision" "$out"
    failures=$((failures+1))
    return 0
  fi
  if [[ "$VERIFY_DETAILS" == "true" && -n "$detail_needle" && "$out" != *"$detail_needle"* ]]; then
    printf 'FAIL %-42s status=%s decision=%s missing_detail=%q\n%s\n\n' "$name" "$status" "$expected_decision" "$detail_needle" "$out"
    failures=$((failures+1))
    return 0
  fi
  printf 'PASS %-42s status=%s decision=%s\n' "$name" "$status" "$expected_decision"
}

printf 'Verifying TCPGuard FH example at %s\n' "$BASE_URL"
if [[ "$VERIFY_DETAILS" == "true" ]]; then
  printf 'Detail assertions enabled. Run the server with TCPGUARD_ENV=development for rule/finding IDs.\n'
fi

check_decision "clean public" 200 allow "" "$BASE_URL/public"
check_decision "debug query throttled" 429 throttle "debug-query-probe" "$BASE_URL/public?debug=true"
check_decision "banned user blocked" 403 block "cache-banned-user" -H 'X-User-ID: banned-user' "$BASE_URL/public"
check_decision "tenant lockdown blocked" 403 block "tenant-lockdown" -H 'X-Tenant-ID: locked-tenant' "$BASE_URL/public"
check_decision "bad IP blocked" 403 block "block-bad-ip" -H 'X-Forwarded-For: 203.0.113.10' "$BASE_URL/public"
check_decision "geo restricted blocked" 403 block "geo-country-restriction" -H 'X-Country: US' "$BASE_URL/geo-restricted"
check_decision "admin after-hours challenge" 401 challenge "admin-after-hours-department-check" -X POST -H 'X-User-ID: manager-1' -H 'X-User-Role: admin' -H 'X-Outside-Hours: true' -H 'X-New-Device: true' "$BASE_URL/admin/users"
check_decision "sensitive export challenge" 401 challenge "sensitive-export" -X POST -H 'X-User-ID: manager-1' -H 'X-Sensitivity: high' "$BASE_URL/api/v1/reports/export"
check_decision "high-value payment blocked" 403 block "high-value-payment-after-hours" -X POST -H 'X-User-ID: manager-1' -H 'X-User-Role: finance_approver' -H 'X-Business-Amount: 1500000' -H 'X-Outside-Hours: true' "$BASE_URL/api/v1/payments/approve"
check_decision "dynamic owner challenge" 401 challenge "dynamic-order-change" -X PUT -H 'X-User-ID: user-1' "$BASE_URL/api/users/user-2/order/order-9"
check_decision "invalid transfer blocked" 403 block "signed-transfer-replay-or-mitm" -X POST -H 'X-User-ID: manager-1' -H 'X-TCPGuard-Signature: bad-signature' -H 'X-TCPGuard-Nonce: nonce-demo' -H "X-TCPGuard-Timestamp: $(date +%s)" "$BASE_URL/api/v1/transfers"
check_status "management health" 200 -H "X-API-Key: $MGMT_KEY" "$MGMT_URL/health"

if (( failures > 0 )); then
  printf '\n%d TCPGuard FH example checks failed.\n' "$failures" >&2
  exit 1
fi
printf '\nAll TCPGuard FH example checks passed.\n'
