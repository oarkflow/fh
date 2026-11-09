# MIRA-T/1 deployment reference (security review required)

**Not a claim of production security certification.** Run in a gated staging or internal network until the external work in `TASKS.md` is complete.

## Network and trust boundaries

- Only expose the TLS control port (TCP 7448) and custom encrypted MIRA datagram port (UDP 7447). Place both behind a DDoS-aware network edge; do not expose legacy PSK mode.
- Every native device uses a separate CA-issued client certificate; trust roots and signing authority must be controlled by your organization.
- Use `ListenTLS` and `DialTLS`. `Listen`/`Dial` reject non-loopback PSK by default and are only for local experiments.
- UDP port and TCP port must route to the *same stateful node* unless your load balancer supports consistent per-session routing. Each node maintains its own in-memory UDP crypto state.
- Keep metrics bound to `127.0.0.1:9109`. Restrict access with your management network or a local scraper.
- Set explicit `-allow-file` identities, with file permissions not readable by unauthorized local users. Access-control reload uses `SIGHUP`; removed identities have their sessions disconnected.
- The browser gateway terminates browser TLS and is an application-level trust boundary. Use `-jwt-public-key`, `-jwt-kid`, `-jwt-issuer`, `-jwt-audience`, and `-origin`; do not use the demo shared bearer token for multiple users. Verify backend authorization for each principal.

## Bootstrap a staging host

1. Issue real server/client certificates with EKUs and appropriate DNS SANs; install files under `/etc/mira` readable only by service user. Do **not** use `mira-dev-certs` in production.
2. Add one identity per line to `/etc/mira/allow.txt`; identities come from verified client certificate URIs or subject CN (depending on issuer policy).
3. Build: `CGO_ENABLED=0 go build -trimpath -o /usr/local/bin/mira-tls-server ./cmd/mira-tls-server`.
4. Configure the included systemd unit or supply mounted secret files to the `Dockerfile` image. Restrict systemd service permissions and bind metrics to loopback.
5. Open TCP 7448 and UDP 7447 from explicit trusted networks during staging. Confirm `go run ./cmd/mira-tls-client` from a separately provisioned client.
6. Check the Prometheus metrics endpoint and network logs; do not log message payloads or credentials.

## Hot reload and revocation

Edit trust roots, key pair and/or `/etc/mira/allow.txt` with atomic rename, then `systemctl kill -s HUP mira-tls`. New TLS connections see the reloaded credentials. Removed ACL identities are revoked from existing sessions. Expiring trust roots without updating clients can block reconnects; overlap trusted CAs during staged rotations.

## Storage and multi-node

The `cluster` package provides renewable fenced ownership, mTLS forwarding, transactional outbox and consumer progress. Actual PostgreSQL integration requires a trusted `database/sql` PostgreSQL driver and an owner-controlled multi-node test environment. Apply `cluster/sql/schema.sql` in a controlled migration. Use `ConsumerGroup.ConsumeNext` and perform the business effect in its SAME database transaction. Never claim exactly-once external calls.

For persistent data, mount a durable encrypted WAL in a persistent volume; keep its historical decryption keys in a separate protected keyring, rotate with overlap, and test restore against a copy. Single-writer WAL files must never be shared by multiple concurrent processes.

## Mobile and browsers

Run `scripts/build-mobile.sh android` on a properly configured Go/JDK/Android SDK host; `scripts/build-mobile.sh ios` requires macOS/Xcode/gomobile. No AAR/XCFramework was produced here. On mobile, handle OS background restrictions, reconnect using TLS bootstrap after process eviction, call `RebindAndWait` on a surviving session after network switch, and use application-level cursor replay for durable messages. Browser clients use the HTTPS gateway, not raw UDP.

## Release gates

Before public traffic: independent cryptographic and transport audit, 24–72h soak tests, live PostgreSQL failover/partitions, real device lab for Android/iOS, IPv4/IPv6/NAT64/carrier field tests, DDoS/fairness tests, and compare against current WebSocket/QUIC implementations under matched TLS, reliability and network conditions. See `TASKS.md`.
