# MIRA-T/1 v7 implementation status — 2026-11-08

> This is the status record from the imported MIRA reference tree. Its original
> verification claims describe that tree and environment, not fh. For the fh
> integration's verification performed after porting, see `docs/mira/transport.md`
> and the implementation test results recorded with this change.

**Classification:** controlled-staging implementation, **not** certified as an independently audited production replacement for QUIC/TLS/WebSocket. Wire version remains **4**. Native UDP is MIRA; TLS 1.3 is used for peer authentication and initial key agreement in the public deployment profile.

## Workstream priority

- **Complete production security and operations engineering work — done.** Release
  automation, operational procedures, readiness criteria, and implementation
  hardening are recorded in this repository. This completes the repository-side
  workstream; external evidence gates remain open:
  independent cryptographic review and penetration test, real WAN/path-MTU and
  long-soak runs, live multi-node PostgreSQL failover, successful release-workflow
  execution, and operational failover/rollback drills. This status does not claim
  those externally run validations have passed.
- **High priority — make SQL queue claims safe across replicas.** The core
  implementation is in place: atomic claims, expiring fenced job leases, stale
  transition rejection, safe recovery, database-clock retry delays, and
  PostgreSQL/MySQL fenced concurrency-key leases with attempt-preserving
  deferral. PostgreSQL claim, recovery, retry-delay, renewal and stale-fence
  behavior passed local PostgreSQL 18 runs using independent handles with
  different session time zones; current-code runs claimed 128 jobs once across
  16 workers. Remaining high-priority validation is MySQL runtime coverage and
  multi-node PostgreSQL/MySQL failover and network-partition behavior. The
  deployment gate only attests to configured adapter capabilities; it does not
  replace these live-backend and failover checks.

## New functioning code in v7

| Work | Source | Verification |
|---|---|---|
| Overlapping gateway Ed25519 assertion-key rotation | `delegation/keyring.go`, v2 signed proof `kid`, CLI SIGHUP reload | Atomic replacement, retirement, concurrent reload, encrypted mTLS+UDP named assertion |
| JWT emergency revocation and fail-closed expiry | `gateway/revocation.go`, `-jwt-revocations` | Token SHA-256/JTI/subject/not-before, concurrent reload, expired snapshots |
| Existing gateway session invalidation on revocation reload | `gateway/gateway.go`, gateway CLI | Stops previously authenticated gateway streams/sessions on successful reload |
| Bounded concurrent gateway upstream dials | `gateway/gateway.go` | Race-enabled 31-attempt test confirms only one backend dial with quota=1 |
| Durable client replay checkpoints | `resume/checkpoint.go`, `ReplayCheckpointed` | fsynced atomic replace, monotonicity, CRC corruption detection, cross-reconnect end-to-end replay |
| Independent second-language encrypted endpoint | `conformance/independent_peer_v4.py` | Real Python X25519+HKDF+AES-GCM legacy PSK client exchanges OPEN, CREDIT, DATA and ACK with Go UDP echo server |
| IPv6 native loopback | `mira/ipv6_v7_test.go` | Encrypted bidirectional echo over `::1` |
| Three-node mTLS fenced ownership transfer | `cluster/three_node_v7_test.go` | Two peers route to owner, standby takes over, stale fence rejected; simulated lease store |
| Reproducible packaging & release attestation | `scripts/package-source.py`, `.github/workflows/mira-release-v7.yml` | Deterministic MIRA source archive, SPDX SBOM and SHA-256 manifest; GitHub provenance and release publication are configured but still require successful workflow evidence |
| Operator WAN soak harness | `scripts/soak-v7.sh` | Available for 1–72 hour runs; NOT executed in this environment |

## Unfinished implementation and validation

**Protocol/network**
- Fresh-DH in-band post-compromise ratcheting is NOT implemented. TLS 1.3 re-bootstrap on a fresh connection gives fresh keys; symmetric packet epochs do not equal post-compromise recovery.
- Resurrection of old UDP packet/cipher state after a process crash is NOT implemented, deliberately. The supported model is a fresh authenticated transport plus durable idempotent journal replay.
- Internet-grade congestion fairness and complete PLPMTUD RFC-conformance testing remain unproven. IPv6 loopback is not IPv6 carrier, NAT64, or route-MTU certification.
- Production cross-language mTLS interoperability remains absent: the independent Python peer tests only the **loopback-only legacy PSK development mode**.

**Distributed systems**
- Real three-node PostgreSQL failover, MySQL runtime and multi-replica behavior, network partition, transactional outbox with a live driver and idempotent downstream effect validation remain untested. The SQL queue adapter's PostgreSQL claim paths passed local PostgreSQL 18 manual integration runs with independent handles and different session time zones. Current code was checked for 128 claims across 16 workers with no duplicate claim, plus stale-fence rejection after reclaim; earlier runs covered repeated migrations, database-clock delayed eligibility and retry backoff, live-versus-expired recovery, and lease renewal. These runs do not validate multi-node failover; the three-node v7 test still uses a shared in-memory lease implementation. MySQL claims now lock candidates in a transaction and migration rejects non-InnoDB queue tables, but the path still needs runtime validation.
- Production KMS/HSM key lifecycle, CA issuance/rotation runbooks with audited procedures, backups/key escrow and failover drills remain externally dependent.

**Clients/operations**
- Android `.aar` and iOS `.xcframework` builds and physical-device background/mobile/cellular tests require external tools/devices and were not run here.
- Real 24–72h soak, live WAN/4G/5G/NAT64 comparisons, and matched WebSocket vs QUIC vs MIRA benchmarks have not run. A runnable operator soak script is provided, not evidence of having run it.
- JWT offline revocation snapshots require secure distribution/reload to every gateway node. There is no live identity-provider introspection contract or global instant kill switch. Signed bearer tokens issued before revocation persist until expiry without an updated revocation snapshot.
- Independent cryptographic review and penetration test remain open. GitHub release workflows for fh and MIRA now create source archives, SPDX SBOMs and checksum manifests, generate provenance attestations, verify them, and publish release assets. Neither workflow has yet produced execution evidence in GitHub. See [production readiness](docs/production-readiness.md) and the [operations evidence gates](docs/operations-drills.md).
- Real WAN/path-MTU and 24–72 hour soak evidence, PostgreSQL multi-node failover results, operational failover/rollback drills, production KMS/HSM lifecycle, and legal/regulatory sign-off remain open. The scripts and simulated three-node test are not substitutes for those runs.

**Deployment decision:** run in a private staging environment with explicit mTLS identities and authorization policies. Use an established, reviewed TLS/QUIC/WebSocket transport for sensitive production traffic until independently audited and stress-validated.
