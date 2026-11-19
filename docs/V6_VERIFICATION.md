# MIRA-T/1 v6 reproducible verification

Environment: Linux amd64, Go 1.23.2; local ephemeral test certificates, loopback IPv4, Python 3 and `tsc` available.

Tests executed with the v6 source:

- `go build ./...` — PASS.
- `go vet ./...` — PASS.
- `go test ./... -count=1 -timeout 100s` — PASS, including the full ~25s transport suite.
- `go test -race ./gateway ./delegation ./cluster ./durable ./mira -count=1 -timeout 120s` — PASS.
- Python v4 independent packet/frame fixture tests — PASS (4).
- Browser TypeScript type check — PASS.
- Delegation across real encrypted UDP: PASS with session and channel-bound Ed25519 proof; wrong session, wrong channel, replay and expired assertions rejected in tests.
- Browser HTTP gateway proof to encrypted UDP backend: PASS.
- Remote mTLS benchmark against separate Go server: 40/40 completed, 0 failed, 2 clients x 20 rounds, 256-byte payload. Example run p50 4.95 ms, p95 9.09 ms, p99 9.72 ms. **Loopback result only**, not a comparison with WebSocket or QUIC.
- `cmd/mira-delegation-key`: files generated with private key permissions `0600`.

See `scripts/verify-v6.sh` to reproduce most checks. Do NOT infer resistance to sophisticated attacks or Internet-grade network fairness from these results.
