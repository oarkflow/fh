# Device-bound authentication

`mw/deviceauth` is a custom public-key login and request-proof protocol. It is
not a replacement for a security review. Use HTTPS everywhere, rate limit
challenge creation and login attempts, and design a separate account recovery
flow. Protect `BeginEnrollment` behind an application authorization decision;
never expose it as an unauthenticated account lookup endpoint.

## Protocol

1. The client creates an Ed25519 key pair. It keeps the private key; the server
   stores only the public key.
2. An authorized account flow calls `BeginEnrollment(subject, deviceID)`. The
   client uses `SignChallenge` on the returned challenge and the app calls
   `FinishEnrollment`.
3. Login calls `BeginLogin(deviceID)`, verifies the client's challenge signature
   with the enrolled key, and issues a short-lived HMAC-signed session using
   `FinishLogin`.
4. Each API request carries that session plus a device signature over method,
   request target, host, body digest, timestamp, nonce, and session. Middleware
   checks the signature and atomically consumes the nonce before dispatching.

The `ReplayGuard` must implement atomic insert-if-absent and be shared by every
replica. `KVReplayGuard` adapts `pkg/storage/kv.Store`; use it only with a
backend that is actually shared across replicas and partitions operations by
key. Missing or unavailable replay state fails closed. A process-local, sharded
`MemoryReplayGuard` is only for single-node development. Implement `DeviceStore`
with a durable shared store for production. Device insertion is insert-only;
explicitly delete a lost or replaced device before enrolling its replacement.

## Server setup

```go
devices := deviceauth.NewMemoryDevices(100_000) // development only
replay := deviceauth.NewMemoryReplayGuard(250_000) // one process only
manager, err := deviceauth.New(deviceauth.Config{
    SigningKey: sessionSigningKey, // load a random 32+ byte key from a secret manager
    Devices: devices,
    Replay: replay, // production: shared atomic replay service
})
if err != nil { log.Fatal(err) }
protected := manager.Middleware(apiHandler)
```

For a shared KV backend, configure `Replay: &deviceauth.KVReplayGuard{Store: sharedKV}` and `Challenges: &deviceauth.KVChallengeStore{Store: sharedKV}`. The backend's per-key atomic `Mutate` operation must be safe across replicas. `MaxChallenges` bounds the default memory store; configure a capacity limit in the shared backend as well.

The built-in memory device store validates its inputs too: IDs are limited to
128 bytes, subjects to 256 bytes, and public keys must be exactly 32 bytes. The
memory replay guard retains fixed-size key digests, accepts keys up to 256 bytes
by default, and enforces its configured total entry count. The memory challenge
store validates identifier and record sizes and defaults to a 16 MiB retained
byte budget; `Config.MaxChallengeBytes` tunes the default store's budget.

`BeginEnrollment` must only follow an application-authorized step. Challenge
creation and login endpoints need per-account and per-network rate limits. Keep
the signing key stable across replicas and rotate it with an explicit key-ring
design before deployment. Sessions are signed leases that include the device
public key, so ordinary API requests do not read the device store. Deleting a
device prevents new logins; existing sessions remain valid until their expiry
(15 minutes by default, at most one hour). Set `CheckDeviceEachRequest` when
immediate deletion semantics matter more than the per-request store read. The
nonce store remains a per-request operation and must be partitioned across
nodes; load-test it at expected peak throughput.

## Client and WASM

`SignChallenge` and `SignRequest` use the same Go canonicalization and can be
compiled for `GOOS=js GOARCH=wasm`. A WASM module is delivered to and controlled
by the browser, so do not embed the signing key or treat WASM as trusted storage.
Use browser-managed key storage and strict CSP; understand that same-origin
script compromise can still ask the client to sign. HTTPS/TLS protects response
transport. For signed response bodies, compose the existing
[`mw/httpsignature`](../httpsignature) response-signing middleware and verify its
RFC 9421 profile in the client; request proof alone does not sign responses.

The request middleware buffers bodies up to the configured limit so the signed
digest matches dispatched bytes. This is unsuitable for unbounded streaming
uploads; use a separately designed chunk-signing protocol for those routes.
