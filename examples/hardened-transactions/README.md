# Hardened Transactions Example

This example demonstrates end-to-end security and reliability across the server, network transport, and outbound/inbound client transactions in `fh`.

## Features Demonstrated

1. **TCP & HTTP Transaction Hardening**:
   - Production TCP keep-alives (15s) to eliminate dead connections.
   - Two-stage graceful HTTP/2 draining (`GOAWAY` $2^{31}-1 \to$ final `lastStream` per RFC 9113).
   - Transient accept error exponential backoff (riding out `EMFILE`/`ENFILE` file descriptor exhaustion without server termination).

2. **Transaction Idempotency with Deadlock Prevention**:
   - `IdempotencyRepository` with processing lock timeouts (`IdempotencyProcessingTimeout`).
   - Automatic `Abort()` cleanup on panics, cancellations, or failures so client retries are never locked out in 409 conflict.

3. **Bidirectional RFC 9421 / RFC 9530 Transaction Signatures**:
   - Cryptographically binds HTTP method, target URI, content type, and SHA-256 `Content-Digest` to an Ed25519 hardware/device key.
   - Non-repudiation and tamper protection across reverse proxies and untrusted intermediaries.

4. **Hardened Outbound HTTP Client**:
   - SSRF defense with blocking for loopback, private IPs, Carrier-Grade NAT (`100.64.0.0/10`), and cloud metadata services.
   - Redirect validation enforcing security policies across every redirect hop.
   - Host-specific Certificate Pinning (`CertPinningConfig`) with exact target hostname matching.
   - Resilient context-aware DNS resolution with multi-IP fallback.

## Running

```bash
# Generate Ed25519 client and server keys
go run ./examples/hardened-transactions -generate-keys

# Run the hardened transaction server
go run ./examples/hardened-transactions
```

## Running Tests

```bash
go test -v ./examples/hardened-transactions
```

