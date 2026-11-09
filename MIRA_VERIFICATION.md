# MIRA-T/1 v5 — Local verification record

**Status: evaluated working engineering candidate, not a certified production transport.** Tests run on Linux with Go 1.23.2 using the current source tree. All measurements are local and cannot establish security, WAN fairness, or carrier compatibility.

| Check | Observed result | Scope |
|---|---|---|
| `go vet ./...` | PASS | Static diagnostics |
| `go build ./...` | PASS | Go source and commands |
| `go test ./... -count=1 -timeout 120s` | PASS | Full package tests, v5 integration cases |
| `go test -race ./... -count=1 -timeout 90s` | PASS | Full race-enabled suite, independently rerun |
| `go test -race ./mira -count=3 -timeout 105s` | PASS | 3 consecutive complete runs, ~74 s |
| `go test ./mira -run 'TestReliabilityWithPacketLoss|TestMTUBlackholeRecoveryRefragmentsReliableMessage|TestRebindAndWaitConfirmsNewPath' -count=10 -timeout 115s` | PASS | 10 runs of each lossy, black-hole and migration case, ~24 s total |
| `python3 -m unittest discover -s conformance -p 'test_wire_v4.py' -v` | PASS (4) | Independent Python packet/frame parsing |
| `tsc --noEmit --target ES2020 --module ES2020 --lib ES2020,DOM browser/client.ts` | PASS | Browser client type checking |
| `go run ./cmd/mira-load -clients 12 -messages 60 -bytes 256 -timeout 60s` | PASS | 720/720 echo responses, 0 failed, ~2,268 msgs/s localhost, p95 ~5.9 ms; illustrative only |

A chained release script run previously stalled during the combined race stage; the isolated full race suite then passed, and three repeated transport race runs also passed. This non-repeatable execution observation should be tracked during longer continuous-integration runs. A successful test suite cannot substitute for exhaustive stress or formal verification.

## Verified functionality

- Authenticated MIRA UDP sessions through standard-library TLS 1.3 mTLS bootstrap; bidirectional channels, transport receipts and fragmented reliable data.
- Local simulated packet loss, connection path rebinding and confirmed socket migration, automatic path-MTU probes and 1,400 -> 1,200 byte black-hole fallback.
- Gateway origin restrictions, per-user session limits and strict EdDSA/RS256 access token checks.
- Fenced lease renewal and router failover using an in-memory lease store; two-router mTLS forwarding in a local TLS integration test.
- Encrypted at-rest WAL, journal idempotency, reconnect and durable application replay, reloadable TLS credentials and ACLs.

## NOT verified

- Independent third-party security or cryptographic audit; post-compromise in-band fresh DH; real carrier IPv6/NAT64/4G/5G behavior or general Internet-grade congestion fairness.
- Live three-node PostgreSQL cluster and transactional consumer tests. Go-only PostgreSQL SQL adapters compile, but no database/driver is available in this build.
- Real Android AAR or iOS framework binaries, background radio/battery and reconnection behavior on devices.
- Another independent complete MIRA transport implementation; Python covers framing only.
- 24–72h soak, WAN network partitions, network DDoS and matched WebSocket/QUIC benchmark tests.

For safe deployments, use this as controlled engineering/staging source only. See `TASKS.md` and `SECURITY.md`.
