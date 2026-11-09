# MIRA-T/1 v7 - Complete User, Developer, and Operator Guide

**Version:** MIRA-T/1 v7 implementation, wire revision 4
**Audience:** First-time evaluators, trainers, Go developers, browser/mobile integrators, system administrators, QA, security reviewers  
**Updated:** 9 November 2026  
**Deployment status:** Controlled staging / engineering evaluation; **not independently certified for sensitive public production traffic**

> **Important:** MIRA-T/1 is a **custom communication protocol and Go runtime**, not a consumer messaging app with ready-made sign-up, inbox, contacts, or graphical screens. Users of an application built with MIRA normally interact with that application's UI; engineers and operators set up and use MIRA underneath. The repository's runnable apps are a **server, client, browser gateway, and testing tools**.
>
> **fh integration note:** This guide accompanies the MIRA source integrated into the fh repository. Use the runnable examples in [`examples/mira/`](../../examples/mira/README.md); archive names and paths from the original standalone MIRA delivery do not apply here. The historical startup condition described in Appendix E is already corrected in this checkout.

---

## Contents

1. [Start here: which path is yours?](#1-start-here-which-path-is-yours)
2. [What MIRA is, explained without jargon](#2-what-mira-is-explained-without-jargon)
3. [What happens when a message is sent](#3-what-happens-when-a-message-is-sent)
4. [Concepts everyone should learn](#4-concepts-everyone-should-learn)
5. [Your first successful demonstration - 10 to 15 minutes](#5-your-first-successful-demonstration---10-to-15-minutes)
6. [Teach MIRA to someone else - facilitator script](#6-teach-mira-to-someone-else---facilitator-script)
7. [Architecture and component responsibilities](#7-architecture-and-component-responsibilities)
8. [Integration guide for Go developers](#8-integration-guide-for-go-developers)
9. [Browser access and the HTTPS gateway](#9-browser-access-and-the-https-gateway)
10. [Android and iOS integration](#10-android-and-ios-integration)
11. [Durable messaging, receipts, and reconnection](#11-durable-messaging-receipts-and-reconnection)
12. [Authentication, authorization, and key rotation](#12-authentication-authorization-and-key-rotation)
13. [Staging deployment and operations](#13-staging-deployment-and-operations)
14. [Testing, performance and acceptance checks](#14-testing-performance-and-acceptance-checks)
15. [Troubleshooting](#15-troubleshooting)
16. [Use cases and suggested defaults](#16-use-cases-and-suggested-defaults)
17. [Frequently asked questions](#17-frequently-asked-questions)
18. [Trainer, developer, and operator checklists](#18-trainer-developer-and-operator-checklists)
19. [Glossary](#19-glossary)
20. [Appendices](#appendix-a-common-repository-paths)

---

## 1. Start here: which path is yours?

| Your role | What you need to learn | Recommended sections |
|---|---|---|
| **Nontechnical evaluator** | Why MIRA exists; how it moves messages; what the demonstration proves | 2, 3, 4, 5, 6, 17 |
| **Instructor / team lead** | How to explain and demonstrate it to a class or team | 2-6, 18 |
| **Go backend developer** | Sessions, channels, message modes, replies, errors, reconnect | 3-5, 7, 8, 11 |
| **Frontend developer** | Why browser access uses the gateway and how to use the TypeScript client | 4, 9, 12 |
| **Android / iOS developer** | Go mobile facade, binding build and network transitions | 4, 10, 11 |
| **DevOps / SRE / security engineer** | Certificates, ports, access controls, metrics, backup, incident response | 7, 11-15, 18 |

**Five-minute orientation:** Read sections 2-4. **First hands-on demo:** Complete section 5. **Deploy to a controlled staging network:** Read sections 7 and 11-15 before opening ports.

### What is included in the source repository?

- Native UDP transport (`mira/`) and formal Go interfaces (`protocol/`).
- Example mTLS server and client (`cmd/mira-tls-server`, `cmd/mira-tls-client`).
- Browser-facing authenticated HTTPS bridge (`gateway/`, `cmd/mira-gateway`, `browser/client.ts`).
- Application journal and write-ahead log (`journal/`, `durable/`, `resume/`).
- Mobile-compatible Go facade (`mobile/`), and optional `gomobile` build script.
- Cluster lease, ownership, forwarding and PostgreSQL SQL foundations (`cluster/`).
- Verification, loss tests and benchmark tools (`scripts/`, `conformance/`, `cmd/mira-bench`).

**Not included:** a finished chat UI, push-notification infrastructure, enrollment portal, hosted identity provider, ready-to-install mobile app, or a multi-node production service managed for you.

## 2. What MIRA is, explained without jargon

Think of the MIRA server as a secure delivery center. Each connected device has an authenticated entrance. Once connected, a device can create several independent **lanes** (channels) inside a single session. Different lanes can use different delivery rules.

- **Chat lane:** important messages are sent reliably and in order.
- **Presence lane:** an old “online” state can be replaced by a newer one; delivering every old update is unnecessary.
- **Live position lane:** a fresh update may matter more than recovering an old one.
- **File lane:** large payloads are broken into fragments and recovered without forcing all other channels to wait in a single application-level queue.

**Bidirectional** means that after a session is established, the client **and** the server can each start sending independently. The server does not have to wait for the client to ask before offering a new channel or pushing data. Permissions and channel authorization still apply.

### MIRA vs WebSocket vs SSE

| Property | MIRA native | WebSocket | SSE |
|---|---|---|---|
| Native client and server both initiate data | Yes | Yes | Not within the SSE stream |
| Underlying network | Custom encrypted UDP | TCP, generally with TLS | HTTP over TCP or HTTP/3 transport |
| Several independently configured logical channels | Built in | Usually application-defined | Application-defined streams |
| Reliable and best-effort policies | Multiple | Reliable ordered TCP | Reliable stream delivery |
| Direct standard browser support | **No**; gateway required | Yes | Yes |
| Mature independent ecosystem / audit | **No** | Established | Established |

**Avoid claims like “always faster than WebSocket”.** MIRA's performance must be measured on a matched workload with equivalent authentication, encryption, message guarantees and network conditions.

## 3. What happens when a message is sent

```text
NATIVE CLIENT                         MIRA SERVER
     |                                     |
     |-- TLS 1.3 client authentication --->|  TCP port 7448
     |<-- verified identity/key setup -----|
     |                                     |
     |== encrypted UDP path confirmation ==|  UDP port 7447
     |                                     |
     |-- OPEN reliable ordered channel --->|
     |<---------------- OPEN accepted -----|
     |-- encrypted DATA fragment(s) ------>|
     |<---------------- packet ACK(s) -----|  network acknowledged
     |<================ APP DATA ==========|  server can send independently
     |<--- optional AckStored receipt -----|  only after app persists
     |<--- optional AckProcessed receipt --|  only after app processes
     |                                     |
```

**Walkthrough:**

1. The client proves its identity using its unique certificate, and verifies the server certificate. The recommended profile uses Go's TLS 1.3 stack for initial authentication and key agreement.
2. Both endpoints derive keys for a **new MIRA UDP session**. UDP proves that the client can receive on its claimed network path.
3. Either side opens channels. The receiver decides whether to authorize a channel.
4. Data are framed and encrypted. A large message may become multiple packets.
5. The receiving transport acknowledges authenticated packet reception. Missing packets may be retransmitted according to the delivery policy and congestion rules.
6. Application-level “stored” or “processed” receipts require explicit action by the receiving application. A packet ACK is not proof that a database transaction succeeded.
7. The session continues until closure, idle expiry, credentials are revoked, or the connection fails.
8. After a process restart, the client establishes a **new authenticated session** and, for durable application workflows, asks for events after its last saved cursor. Old UDP cipher/packet state is **not** resurrected.

### What encryption protects

The native MIRA UDP hop uses AES-256-GCM for authenticated packet protection. It does **not** automatically provide end-to-end encryption between two human users across an application server. In the browser configuration, the HTTPS gateway can access application plaintext unless the application adds its own E2EE layer.

## 4. Concepts everyone should learn

### 4.1 Connection / session / channel / message

- **Connection path:** Where the packets travel, such as a UDP socket and network address.
- **Session:** One authenticated logical client-server relationship with its own security state and channels.
- **Channel:** A logical lane within the session. Either endpoint may initiate one.
- **Message:** Application payload sent on a channel, identified with a channel-scoped message ID.
- **Packet:** The network datagram transporting one or more encoded frames; a message can span packets.
- **Cursor:** A durable application-log position used to replay missed events after reconnecting.

### 4.2 Five delivery modes

| Mode in Go | Meaning | Example | Practical caution |
|---|---|---|---|
| `ReliableOrdered` | Retransmission and ordered channel delivery | Chat, commands | Later messages wait for missing earlier ones on that ordered channel |
| `ReliableUnordered` | Reliable transfer without ordering between messages | Independent event updates | Application must handle out-of-order completion |
| `BestEffort` | No reliability promise | Live indicators | Packets can disappear |
| `LatestState` | Newer *queued* state supersedes obsolete unsent state | Typing, presence | Does not retract updates already delivered |
| `Bulk` | Reliable multi-fragment transfers | Documents, media | Configure size, quotas, priorities and storage separately |

### 4.3 Three receipt levels

| Level | Proves | Does not prove |
|---|---|---|
| `AckTransport` | The transport recognized receipt of message data | Database persistence, business action success |
| `AckStored` | Receiver application explicitly reported durable commit | A human read it; an external workflow completed |
| `AckProcessed` | Receiver application explicitly reported processing | Arbitrary business exactly-once effects unless the app designs for them |

A stable idempotency key, a durable store and transactional business logic are necessary when duplicates would be harmful.

### 4.4 How migration works

If a phone moves from Wi-Fi to mobile data while its process and session remain alive, the Go facade offers `RebindAndWait()`. The new path must be authenticated/confirmed. If the process was killed, create a fresh TLS/UDP session and replay durable records; a backgrounded mobile app cannot depend on a permanently running UDP socket.

## 5. Your first successful demonstration - 10 to 15 minutes

**Goal:** send a text message from a Go client to a separate Go server through authenticated TLS bootstrap and encrypted UDP, then receive an echo.

**Prerequisites:** the Go version required by fh's root `go.mod`, a terminal, and local TCP/UDP loopback access. No database, browser, or cloud account is needed for this native echo. Commands below run from the fh repository root; the current runnable code is in [`examples/mira/native`](../../examples/mira/native).

### Step 1: Extract and open the project

```bash
cd /path/to/fh
```

If you are already in the repository, continue directly:

```bash
go version
```

MIRA itself uses the Go standard library; the fh module's existing dependencies
remain those listed in the root `go.mod`.

### Step 2: Generate local, temporary identities

```bash
go run ./cmd/mira-dev-certs -out ./dev-certs
```

This creates `root.pem`, `server.pem`, `server.key`, `client.pem`, and `client.key` (plus the development CA private key). These credentials are **test only**; do not expose them or use this CA in production. The command refuses to overwrite an existing directory. Use a new directory if you rerun it.

### Step 3: Start the server - Terminal A

```bash
go run ./examples/mira/native/server \
  -udp 127.0.0.1:7447 \
  -tcp 127.0.0.1:7448 \
  -cert ./dev-certs/server.pem \
  -key ./dev-certs/server.key \
  -ca ./dev-certs/root.pem \
  -allow device-1
```

You should see a log similar to:

```text
MIRA mTLS bootstrap on 127.0.0.1:7448; encrypted UDP on 127.0.0.1:7447
```

`-allow device-1` must match the generated client's certificate identity. TCP 7448 is used for TLS authentication and bootstrap; UDP 7447 carries encrypted application data.

### Step 4: Start the client - Terminal B

```bash
go run ./examples/mira/native/client \
  -udp 127.0.0.1:7447 \
  -tcp 127.0.0.1:7448 \
  -cert ./dev-certs/client.pem \
  -key ./dev-certs/client.key \
  -ca ./dev-certs/root.pem \
  -name server.local \
  -message "Hello from MIRA"
```

**Expected output:**

```text
server replied: echo: Hello from MIRA
```

This workflow was validated with separate server/client processes from the fh examples. You may optionally add an MTU probe using `cmd/mira-tls-client`; do not assume 1,400-byte datagrams work on the public Internet.

### Step 5: Change the message and observe

Run the client again with `-message "Second test"`. The response should change to `server replied: echo: Second test`. The client process terminates after the echo; the server remains available for another authenticated client session.

### Step 6: Stop the demonstration

Press **Ctrl+C** in the server terminal. Remove temporary certificates when done; don't put the private keys in Git or send them over chat/email.

### Step 7: Run health checks

```bash
go build ./...
go vet ./...
go test ./... -count=1 -timeout 120s
```

More comprehensive verification, which takes longer and requires Python/TypeScript tools for optional checks:

```bash
bash scripts/verify-v7.sh
```

**Important:** The full verification script may skip the independent Python encrypted peer if `cryptography` is unavailable, and TypeScript compilation if `tsc` is missing. Read the output rather than assuming all optional stages ran.

## 6. Teach MIRA to someone else - facilitator script

### 15-minute demo for a general audience

**Minutes 0-2 - The problem:** “An application needs to deliver messages both ways without repeated HTTP polling. Some updates must be reliable, others should be discarded when stale.”

**Minutes 2-4 - Explain the model:** Draw **device -> secure entrance -> delivery lanes -> server**, then arrows in both directions. Introduce *session* and *channel* as the two main ideas.

**Minutes 4-8 - Live walkthrough:** Run section 5. Show the server continuing to listen while new clients connect and send different text. Point out that the output is a command-line demonstration rather than a chat UI.

**Minutes 8-11 - Compare delivery policies:** Ask whether a chat message, a typing dot and a document download deserve the same retry strategy. Explain `ReliableOrdered`, `LatestState`, and `Bulk`.

**Minutes 11-13 - Safety and recovery:** Explain client certificates, revocation, the difference between transport ACK and stored receipt, and what a reconnect actually means.

**Minutes 13-15 - Knowledge check:** Ask (1) Can the server initiate communication? (Yes.) (2) Does MIRA run directly in browser JS? (No.) (3) Does an ACK mean stored in database? (Not unless `AckStored` is explicitly issued.) (4) Can a restarted process reuse old UDP cipher state? (No.)

### One-minute explanation to a customer

“MIRA is the realtime communication engine inside an application. Devices connect securely and can exchange messages both ways. The application chooses whether each kind of message needs guaranteed ordered delivery, quick best-effort updates, or durable replay after a reconnect. What the user sees is the application built around MIRA; the protocol is the invisible transport layer.”

### What to avoid teaching

Do not say MIRA is automatically faster than WebSocket, is a replacement for TLS, guarantees exactly-once execution, provides user-to-user E2EE by itself, or has passed an independent security certification. Those claims are not supported by the v7 source or verification record.

## 7. Architecture and component responsibilities

```text
             Android / iOS app          Native Go service          Browser UI
                    |                          |                        |
              Go mobile facade          Go MIRA client          browser/client.ts
                    |                          |                        |
          mTLS control (TCP 7448)  mTLS control (TCP 7448)     HTTPS gateway
          MIRA data (UDP 7447)     MIRA data (UDP 7447)          |
                    +--------------------------+                MIRA Go client
                               |                               /       \
                               +------- MIRA UDP -------------+         |
                                            |                           |
                                   MIRA-T/1 server                   auth/JWT
                                            |
                              Channels / scheduler / ACK / credits
                                            |
                             Application authorization and handlers
                                            |
                     +----------------------+---------------------+
                     |                                            |
             Journal / encrypted WAL                       Optional cluster
             Idempotency / cursors                          lease/outbox/forward
```

### Component map

| Source path | Responsibility | Primary users |
|---|---|---|
| `mira/` | Network runtime, native transport, sessions, handshake, channels, flow and mobility | Protocol engineers |
| `protocol/` | Go interface contracts and message/delivery types | Integrators |
| `cmd/mira-tls-server/` | Example authenticated echo/journal server | Operators, testers |
| `cmd/mira-tls-client/` | Example client | First-time users |
| `gateway/` + `cmd/mira-gateway/` | HTTPS authentication, browser sessions, forwarding, JWT/key reload | Web backend teams |
| `browser/client.ts` | Browser fetch/stream client | Frontend teams |
| `mobile/` | Go binding facade | Mobile teams |
| `durable/`, `journal/`, `resume/` | Persisted messages, replay and cursor recovery | Backend teams |
| `cluster/` | Lease fencing, routing and SQL outbox primitives | Distributed systems teams |
| `docs/RUNTIME_SPEC.md` | Actual revision-4 wire reference | Protocol developers |
| `docs/RUNBOOK.md` | Operations and incidents | SRE/security |

### Boundaries to remember

- TLS bootstrap and UDP data path must reach the correct stateful node. A load balancer cannot freely send those to unrelated nodes.
- Browser TLS terminates at the gateway; application authorization must remain enforced at the MIRA backend.
- The sample cluster interfaces do not constitute a finished automatic failover service.
- Individual transport sessions are in-memory; durable application records live in the WAL/DB, not in a recoverable old UDP session.

## 8. Integration guide for Go developers

### 8.1 Public interfaces

`protocol/api.go` defines `Session`, `Channel`, `Message`, `ChannelConfig`, `ReceiptLevel` and authorizer interfaces. The operational implementation is in package `mira`.

Relevant signatures:

```go
type Session interface {
    ID() uint64
    OpenChannel(context.Context, ChannelConfig) (Channel, error)
    AcceptChannel(context.Context) (Channel, error)
    Close(context.Context) error
}

type Channel interface {
    ID() uint64
    Send(context.Context, []byte) (uint64, error)
    Receive(context.Context) (Message, error)
    AwaitReceipt(context.Context, uint64, ReceiptLevel) (Receipt, error)
    Acknowledge(context.Context, uint64, ReceiptLevel) error
    Close(context.Context) error
}
```

### 8.2 Client-side sequence

Create a `tls.Config` with a trusted CA, the client's unique X.509 identity, TLS 1.3 minimum version, and a verified `ServerName`; then:

```go
ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
defer cancel()

session, err := mira.DialTLS(ctx, tcpAddress, udpAddress, tlsConfig, mira.Options{})
if err != nil { return err }
defer session.Close(context.Background())

channel, err := session.OpenChannel(ctx, protocol.ChannelConfig{
    Delivery: protocol.ReliableOrdered,
})
if err != nil { return err }
defer channel.Close(context.Background())

messageID, err := channel.Send(ctx, []byte("hello"))
if err != nil { return err }

// If this request expects an echo, receive independently from the ACK.
reply, err := channel.Receive(ctx)
if err != nil { return err }
fmt.Println(string(reply.Payload))

_, err = channel.AwaitReceipt(ctx, messageID, protocol.AckTransport)
return err
```

*This is an integration excerpt, not a complete standalone `main.go`: imports, error handling and TLS certificate loading are omitted. For executable code, use `cmd/mira-tls-client/main.go`.*

### 8.3 Server-side sequence

FH applications should select the native MIRA transport through `app.ListenMIRATLS(udpAddr, tcpAddr, tlsConfig, opts)` and run channel handlers with `app.ServeMIRA(ctx, server, handler)`. The app owns and closes its MIRA endpoints during shutdown. The server callback receives the verified certificate identity and channel, where it can implement application authorization and message handling. For clients use `fh.NewMIRAClient(tlsConfig, opts)` followed by `Connect(ctx, tcpAddr, udpAddr)`; open channels on the returned session. The lower-level `mira.ListenTLS` / `mira.DialTLS` APIs remain available for integrations that manage lifecycle themselves. `fh.Client` remains the HTTP client; MIRA carries native channels and does not wrap HTTP requests.

**Server push:** opening a channel and sending from the server is independent of a client request. The client should run an `AcceptChannel` loop and explicitly authorize/dispatch server-created channels; the simple echo CLI only demonstrates responses, not unsolicited push.

### 8.4 Safe handling patterns

- Use **context deadlines** for connect, open, send, receive and receipt waits.
- **Do not create an unbounded goroutine per message.** Use bounded queues, worker counts and flow credits.
- Distinguish a transport acknowledgment from an application commit.
- Never trust user or tenant IDs supplied inside message payloads; derive them from authenticated principal context.
- A client retry should reuse an idempotency key for durable publish, not assume the previous attempt failed merely because its response timed out.
- Keep per-channel payload limits, per-device quotas and authorization checks.
- Handle `ErrClosed`, backpressure, lease expiry, timeouts and reconnect explicitly.
- Avoid logging tokens, session keys, private certificates or raw sensitive payloads.

### 8.5 Channel choice by application feature

| Feature | Suggested mode | Why |
|---|---|---|
| Private message or command | `ReliableOrdered` | Preserve message order |
| Independent notifications | `ReliableUnordered` plus app dedupe | Delivery without cross-message ordering |
| Typing status, presence | `LatestState` | Stale queued updates are not useful |
| Live sensor or pointer | `BestEffort` | Prefer freshness to retries |
| File or image transfer | `Bulk` | Fragmentation and reliable recovery |

## 9. Browser access and the HTTPS gateway

Browser JavaScript normally cannot open arbitrary UDP sockets, so it cannot directly speak native MIRA. The included browser client talks HTTPS to the gateway. The gateway connects as a Go native MIRA client to the UDP server.

```text
Browser -> HTTPS (JWT or demo bearer) -> Gateway
Gateway -> mTLS bootstrap + encrypted MIRA UDP -> Server
```

### Supported browser gateway endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/connect` | Create an authenticated browser gateway session |
| `POST` | `/v1/send` | Send base64-encoded bytes through an existing session |
| `GET` | `/v1/pull` | Poll current gateway event history |
| `GET` | `/v1/stream` | Receive streaming NDJSON events |
| `DELETE` | `/v1/connect?session=...` | End an existing session |

Every request must be authenticated. A browser session ID is not a substitute for the JWT or bearer token. The gateway limits concurrent sessions and history size; a cursor gap requires application replay, not silently skipping events.

### TypeScript usage

Use the checked-in `browser/client.ts`:

```ts
import { MiraBrowserClient } from "./client";

const client = new MiraBrowserClient(
  "https://gateway.example.com", // your trusted deployed HTTPS gateway
  accessToken,                   // provided by your auth system
  (event) => {
    console.log("event cursor:", event.cursor);
    console.log(new TextDecoder().decode(event.data));
  },
);

await client.connect();
await client.send(new TextEncoder().encode("hello"));
// On sign-out or unmount:
await client.close();
```

**Important implementation detail:** `MiraBrowserClient` holds the token provided to its constructor; on token refresh, create a new authorized client/session. Its streaming retry loop does not automatically re-establish an invalidated gateway session or restore durable backend cursors. Your application must do that.

### Gateway deployment checklist

1. Give the gateway its own **service client certificate** for backend mTLS.
2. Terminate browser HTTPS with a valid hostname and certificate.
3. Authenticate browser principals with a verified, issuer- and audience-pinned JWT (`-jwt-public-key` or rotating `-jwt-jwks-url` / `-oidc-discovery`), not one shared production bearer.
4. Set an explicit `-origin https://your-web-app.example` when browser origins differ.
5. For multi-user operation, configure a gateway Ed25519 delegation signing key or rotating signing ring. Configure the backend's matching public trust ring, `-gateway-identity` and matching audience. This prevents treating every browser user as the gateway service account.
6. Validate that the backend authorizes the delegated subject for every relevant operation.
7. Configure revocation snapshots and key rotation with overlap. Distribute revocation files and reload *every* gateway instance.

**Security warning:** HTTPS from browser to gateway does not automatically make messages end-to-end encrypted across the gateway. If the application requires E2EE, add application-level encryption before handing the bytes to the gateway.

## 10. Android and iOS integration

`mobile/client.go` is a Go facade intended for binding with `gomobile`. It is **not** a ready-to-install Android or iOS application.

### Public mobile methods

| Method | Purpose |
|---|---|
| `NewClient()` | Create facade instance |
| `Connect(tcpAddr, udpAddr, serverName, rootCA, clientCert, clientKey)` | Establish authenticated MIRA session |
| `OpenChannel(delivery)` | Open a local channel; numeric modes follow Go `protocol.Delivery` constants |
| `AcceptChannel(timeoutMillis)` | Accept a server-initiated channel |
| `Send(channelID, payload)` / `Receive(channelID, timeoutMillis)` | Send / receive bytes |
| `WaitTransportReceipt(...)` | Wait for transport acknowledgment |
| `RebindAndWait(timeoutMillis)` | Verify path migration after a surviving network change |
| `TransportStatsJSON()` | Obtain diagnostic JSON |
| `CloseChannel(id)` / `Close()` | Release resources |

### Build scripts

```bash
# Requires an installed gomobile, JDK and Android SDK/toolchain
bash scripts/build-mobile.sh android

# Requires macOS, Xcode and gomobile
bash scripts/build-mobile.sh ios
```

The script targets `build/mobile/Mira.aar` for Android and `build/mobile/Mira.xcframework` for iOS. **These artifacts were not built or tested in the provided environment.** Validate actual generated bindings, permissions, thread safety and network callbacks on real devices before promising SDK stability.

### Mobile lifecycle example

1. User signs in via your application-specific identity service.
2. The application provisions an authorized client certificate, ideally backed by platform secure storage and revocable identity lifecycle.
3. Connect on app foreground / active need, open channels, handle both app sends and server-opened channels.
4. On Wi-Fi-to-cellular change, attempt `RebindAndWait` if the process/session survived.
5. After OS process termination, reconnect with fresh TLS bootstrap and use application cursors to replay durable events.
6. For background delivery, integrate the OS-supported push mechanisms (FCM/APNs); MIRA does not bypass background network limits.
7. On logout or device revocation, close sessions and revoke credentials according to server policy.

## 11. Durable messaging, receipts, and reconnection

### When to use durable data

Use durable storage when losing a message after a server restart would be unacceptable (orders, important notifications, audit events, or accepted chat messages). Live typing/position should not be journaled just because the transport can do so.

### Journal flow

```text
Client publish(idempotency-key, payload)
       -> encrypted reliable channel
Server validates authenticated principal
       -> AppendIdempotent into WAL
       -> fsync / committed cursor
       -> issue AckStored
       -> send "committed" event
Client saves cursor / can acknowledge processed cursor
On reconnect: replay(cursor) -> events after cursor -> replay_complete
```

`journal.Request` operations are `publish`, `replay` and `ack`. The published ID must be stable across retries. In the sample journal handler, a stored receipt follows a successful commit, while a processed receipt means the **journal command** was applied - not that an arbitrary external side effect completed.

### Persistent storage in the mTLS server example

`cmd/mira-tls-server` supports `-wal /path/to/wal -wal-keys /path/to/keyring.json`. Both flags are required for the journal profile; without `-wal`, the server runs its **echo** profile. The keyring has the format:

```json
{
  "active": 1,
  "keys": {
    "1": "REPLACE_WITH_64_HEX_CHARACTERS_FROM_A_SECURE_32_BYTE_RANDOM_KEY"
  }
}
```

Create the key with a cryptographic random source, restrict file permissions to `0600`, and keep its backup separately from the WAL. The keyring parser expects a real 32-byte hex key, not the placeholder above. Prefer a dedicated secrets-management process in controlled staging; production HSM/KMS lifecycle remains a release gate.

**Note:** The supplied `cmd/mira-journal-client` is a **legacy device-PSK** journal demo. It is not a matching mTLS journal CLI. For mTLS journal access, integrate `journal.SendRequest` and `journal.ReadEvent` into an authenticated client. Do not expose the legacy journal client over public networks.

### Replay checkpoints

The `resume/` package supports checkpoint files and `ReplayCheckpointed`; a restored client uses a fresh session and a saved cursor. Application callbacks should be idempotent. A crash can occur between processing an event and writing its new cursor, so replay can present that event again.

## 12. Authentication, authorization, and key rotation

### Native client identity

- Verify server certificates (DNS SAN, trusted CA, TLS 1.3).
- Assign each client/device its own certificate and enforce access with `-allow` or `-allow-file`.
- Set channel authorization based on authenticated identity and capability; do not rely solely on successful handshake.
- Use the development PSK handshake **only on loopback/controlled experiments**, not for public rollout.

### Revocation and TLS reload

The mTLS server accepts `-allow-file /etc/mira/allow.txt` with one identity per line. Atomically replace the file and send `SIGHUP` to reload. Removed identities are actively revoked from live sessions; an invalid reload preserves the previous valid policy. The same signal reloads server certificates/trusted CAs for **new TLS connections**. Existing sessions are not implicitly reauthenticated by replacing the server certificate.

```bash
# Linux example, when running under systemd
sudo systemctl kill -s HUP mira-tls
```

Confirm the denied device cannot establish a new session and that its active session was terminated.

### Gateway assertion-key rotation

A multi-principal browser gateway signs short-lived assertions binding its user identity to an actual MIRA session and channel. A verifier checks key ID, signature, expiry, audience, session/channel binding and replay. Example JSON rings:

**Gateway signer** (private key files):

```json
{"active":"2026-b","keys":[{"id":"2026-b","file":"/etc/mira/gateway-b.key"}]}
```

**Backend verifier** (public key files):

```json
{"keys":[{"id":"2026-a","file":"/etc/mira/gateway-a.pub"},{"id":"2026-b","file":"/etc/mira/gateway-b.pub"}]}
```

Roll out public key B to the backend **first**, then start signing B, wait for existing short-lived assertions and rollout overlap, then retire A. `SIGHUP` supports atomic keyring reloads. The gateway private signing key is separate from the browser JWT issuer's signing key.

### JWT emergency revocations

The gateway optionally accepts `-jwt-revocations /etc/mira/revocations.json`, with token hashes, JWT IDs, disabled subjects and subject-issued-before cutoffs. Reload every node; a successful reload invalidates existing gateway sessions. This is **not instantaneous global identity-provider revocation**.

## 13. Staging deployment and operations

### Topology and ports

| Service | Default | Network scope |
|---|---|---|
| MIRA native UDP | `7447/udp` | Authenticated trusted client / gateway paths |
| mTLS bootstrap | `7448/tcp` | Same stateful node as the UDP endpoint |
| Browser HTTPS gateway | `9443/tcp` in example | Valid HTTPS hostname / client-access policy |
| Metrics/health | `127.0.0.1:9109/tcp` | Loopback only, scrape through controlled management path |

### Before staging

1. **Issue real certificates** with proper CA ownership, server SAN and client identities. Never reuse generated development CA material.
2. Run the MIRA server under an unprivileged service account. Mount private key files owner-only.
3. Open only required TCP/UDP ports. Keep both routes pinned to the same stateful node for a given session.
4. Configure `-allow-file`, channel permissions, quotas, idle expiry and metrics access.
5. If using WAL, create a protected persistent volume, encrypted keyring, backup/recovery procedure and single-writer guarantee.
6. Introduce a health probe. The provided metrics server also supports `GET /healthz` (204 on a healthy process), but this does not prove every backend dependency works.
7. Test reconnect, identity revocation, lost packets, path changes and graceful shutdown before accepting any staging users.

### Example build

```bash
CGO_ENABLED=0 go build -trimpath \
  -o ./mira-tls-server ./cmd/mira-tls-server
```

A sample systemd service, Dockerfile, configuration and deployment instructions live in `deploy/` and `docs/DEPLOYMENT.md`. Review and adjust these instead of copying a dev configuration verbatim.

### Monitoring and alerts

Watch active sessions, handshake failures, dropped packets, retransmission growth, authentication denials, congestion/backpressure, CPU/memory and WAL errors. Collect logs without payloads, tokens or device secrets. Sample metrics:

```bash
curl -fsS http://127.0.0.1:9109/healthz -o /dev/null -w '%{http_code}\n'
curl -fsS http://127.0.0.1:9109/metrics
```

### When something fails

- **Client compromised:** revoke its identity from the allowlist, reload, rotate credentials, check live sessions and audit access.
- **MIRA packet loss:** inspect UDP reachability, RTT and packet drops; investigate path-MTU fallback and congestion; don't simply disable protection.
- **Node crash:** restart a new secure session, replay from durable cursor, do not copy old UDP encryption/packet counters into a new process.
- **Database partition:** ensure stale owners lose their fence before allowing writes. The sample cluster test uses simulated lease storage, not a live PostgreSQL HA deployment.
- **Disk/WAL incident:** restore an isolated copy with its historical encryption keys and test replay before returning traffic.

## 14. Testing, performance and acceptance checks

### Verification commands

```bash
# Essential
make test
make vet
make build

# Race and protocol regression
make race
make loss
make conformance

# Full v7 automation (reads optional tool availability)
bash scripts/verify-v7.sh
```

### Benchmarks

`cmd/mira-bench` connects to a **running mTLS echo server** and reports JSON with client count, message size, completion/failure counts, round trips per second and p50/p95/p99. Example:

```bash
go run ./cmd/mira-bench \
  -tcp 127.0.0.1:7448 -udp 127.0.0.1:7447 \
  -cert ./dev-certs/client.pem \
  -key ./dev-certs/client.key \
  -ca ./dev-certs/root.pem \
  -name server.local \
  -clients 2 -messages 50 -bytes 256
```

`make load` runs an isolated local workload using the loopback development profile; it is a smoke test rather than comparative proof. The independent Python parser/interoperability checks cover wire revision 4, while the encrypted Python peer uses only the development PSK profile.

### Minimum internal acceptance matrix

| Scenario | Expected behavior |
|---|---|
| Correct identity and trusted server CA | Connection succeeds |
| Incorrect CA or missing authorized device identity | Connection rejected |
| Client sends text | Server receives it and echo returns |
| Server initiates a channel | Client accepts and receives data |
| Lost UDP packets on reliable channel | Message recovers within bounded conditions |
| Best-effort path drops packet | Application tolerates missing update |
| Duplicate durable publish ID | Same committed cursor, no second stored record |
| Reconnect after process restart | Fresh session, cursor replay |
| Revoked device | Active session closed; reconnect denied |
| Gateway token invalid or revoked | HTTP request rejected; relevant sessions invalidated |
| Gateway source origin unauthorized | Cross-origin request denied |
| WAL backup restore | Data decrypts with retained keyring and cursor is valid |

**Before public production:** run independent cryptographic/penetration review; 24-72h WAN soaks, real IPv6/NAT64/4G/5G tests, live PostgreSQL failover and network partitions, Android/iOS real-device integration and matched QUIC/WebSocket benchmarks. These remain open v7 release gates.

## 15. Troubleshooting

| Symptom | Likely reason | What to do |
|---|---|---|
| `gateway identity and delegation public key must be configured together` on default server launch | Historical standalone v7 startup condition; fixed in this fh checkout | Confirm you are running the current source and inspect the exact flag combination |
| `connect: connection refused` on TCP 7448 | Server not listening / wrong address | Check Terminal A, process exit logs, TCP bind and firewall |
| TLS `unknown authority`, bad certificate or name mismatch | Wrong root CA, SAN or `-name` | Use matching dev certs; never disable verification as a shortcut |
| Client authenticates but server closes | Identity not on `-allow`/`-allow-file` | Confirm certificate CN/URI mapping and allowlist |
| Connection opens but data never returns | UDP 7447 filtered / sent to different node | Open UDP both ways; pin TCP/UDP to same stateful node |
| `-probe-mtu 1400` fails | Path does not support proposed datagram | Remove manual probe, use conservative defaults |
| `AckTransport` observed but business state absent | ACK is transport-only | Require a committed receipt and inspect application storage |
| Duplicate application action after reconnect | Replay is at-least-once under crash windows | Use transactional idempotency and stable IDs |
| Browser CORS/auth rejection | Origin mismatch or JWT invalid | Verify exact HTTPS origin, issuer, audience, expiry, JWT signing key |
| Browser stream stops after revocation | Revocation deliberately invalidates sessions | Obtain new valid token and establish a new authorized session |
| WAL fails to open | Missing/corrupt keyring, wrong permissions, wrong key IDs | Keep keyring `0600`, retain historical keys, restore tested backup |
| `gomobile` not found / iOS build fails | Platform toolchain missing | Install supported gomobile toolchain; iOS requires macOS/Xcode |
| `bash scripts/verify-v7.sh` reports SKIPPED | Optional Python crypto or TypeScript missing | Install optional tools and rerun those stages |
| High latency / low throughput | Loss, congestion, oversized payloads or CPU/backpressure | Measure p95/p99, loss, rate limits, MTU, pacing and compare controlled baselines |

## 16. Use cases and suggested defaults

### Case A - Workplace messaging

- `ReliableOrdered` per conversation channel.
- Durable journal for messages that must survive reconnect.
- Receipt semantics separated into transport/stored/processed.
- Identity-based permissions, expiration and revocation enforced by the application.

### Case B - Presence and typing

- `LatestState` for updates where the newest queued state supersedes earlier queued state.
- Short expiry at the application layer.
- Never treat lost typing events as errors requiring database repair.

### Case C - Live telemetry or multiplayer state

- `BestEffort` for high-frequency transient updates.
- `ReliableOrdered` for authoritative commands or important state transitions.
- Implement reconciliation, limits and abuse prevention in the application.

### Case D - Business events / workflow requests

- `ReliableOrdered` or `ReliableUnordered` depending on causal ordering.
- Generate a stable idempotency key.
- Persist before reporting `AckStored`.
- Use a transactional outbox/consumer effect where required.

### Case E - File transfer

- `Bulk` channel with bounded transfer size and storage quotas.
- Resume/checkpoint at the application layer if transfer progress must persist across process crashes.
- Content scanning, encryption keys, recipient authorization and retention are application concerns.

## 17. Frequently asked questions

**Is MIRA itself an app I can install on my phone?** No. It is a protocol and Go library with examples. Developers integrate it into an Android, iOS, desktop or web application.

**Can it send from server to client without polling?** Native MIRA can: either side may create a channel and send. The browser bridge uses authenticated HTTP/streaming to expose this functionality to browsers.

**Does it use WebSocket, SSE or QUIC internally?** The native packet transport is custom encrypted UDP. The browser adapter is HTTPS/NDJSON; the bootstrap uses standard TLS 1.3. It is not entirely independent of established cryptography or protocols.

**Is every message reliably delivered?** No. It depends on the selected channel delivery mode, timeouts and eventual connection state. Durable application delivery requires an explicit store and replay contract.

**Does `AckProcessed` mean the recipient read my message?** No. It means the receiver application asserted processing. Human read receipts need a separate application workflow.

**Will it work behind any firewall?** Not necessarily; native UDP can be filtered. This source does not provide a validated, universal TCP fallback for every native deployment.

**Can it run on more than one node?** Cluster-related code exists, but fully automated production failover and real PostgreSQL partition testing are not completed in v7.

**Is it ready to secure sensitive public conversations?** Not as an independently reviewed transport. Use established audited transports for sensitive production traffic until the required external gates are passed.

**What does “wire revision 4” mean?** It is the on-network packet version shared by the current v7 runtime. It is not interchangeable with older incompatible wire versions.

## 18. Trainer, developer, and operator checklists

### Trainer checklist

- [ ] Explain that MIRA is infrastructure, not a finished app UI.
- [ ] Demonstrate client->server echo with development certificates.
- [ ] Describe server-initiated messaging and its separate application handler.
- [ ] Explain why chat, presence and files have different delivery policies.
- [ ] Explain that transport ACK != database commit.
- [ ] State security review and deployment limitations accurately.

### Developer checklist

- [ ] Use mTLS for any remotely reachable environment.
- [ ] Map authenticated identity to application permissions.
- [ ] Implement both outgoing and incoming channel handlers.
- [ ] Set bounded message sizes, buffers, contexts and quotas.
- [ ] Use `AckStored` only after persistence and idempotency for retryable commands.
- [ ] Test reconnect, duplicate delivery, revoked identity and graceful shutdown.
- [ ] Avoid relying on process-persistent UDP session recovery.

### Operator checklist

- [ ] Provision CA-issued certificates, distinct per-device credentials.
- [ ] Keep UDP/TCP bootstrap traffic routed to the same node.
- [ ] Restrict ports, identities, metrics and key permissions.
- [ ] Configure key rotation/revocation distribution and record the reload outcome.
- [ ] Protect WAL/backup/keyring separately and test restore.
- [ ] Run races, loss, real WAN/mobile soak and penetration tests.
- [ ] Confirm rollback does not corrupt newer durable cursors or key epochs.
- [ ] Do not label this release independently production-audited.

## 19. Glossary

| Term | Definition |
|---|---|
| ACK | Acknowledgment; its meaning depends on transport vs application receipt level |
| AEAD | Authenticated encryption protecting confidentiality and integrity of packets |
| Backpressure | Limiting senders when buffers/receiver capacity are constrained |
| Channel | Logical message lane inside a session |
| Cursor | Durable application event offset for replay |
| E2EE | End-to-end encryption where intermediaries cannot decrypt app messages |
| Fencing token | Monotonic authorization token preventing stale cluster owners from writing |
| Full duplex | Both endpoints can send at the same time |
| Idempotency | Repeating a command has the same intended durable effect |
| JWKS | JSON Web Key Set; used for rotating JWT verification keys |
| mTLS | Mutual TLS; server and client present authenticated certificates |
| MTU | Maximum packet size acceptable across a network path |
| NAT rebinding | A client appears from a different external address/port while still connected |
| Replay | Re-delivering durable records after a saved cursor |
| Session | One authenticated MIRA logical connection and its channel state |
| UDP | Datagram-oriented network transport used by native MIRA |
| WAL | Write-ahead log for crash-recoverable application records |
| Wire revision | Packet layout/version recognized by a protocol peer |

## Appendix A. Common repository paths

```text
fh/
  examples/mira/README.md     Complete integration guide and runnable examples
  examples/mira/native/       mTLS server and client examples
  examples/mira/browser/      HTTPS browser-gateway sample
  docs/mira/ARCHITECTURE.md   Detailed design
  docs/mira/RUNTIME_SPEC.md   Wire revision 4 reference
  docs/mira/DEPLOYMENT.md     Staging network/security model
  docs/mira/RUNBOOK.md        Incident response
  mira/                       UDP transport and sessions
  protocol/                   Public Go interface contracts
  cmd/mira-tls-server/        mTLS echo / WAL journal server
  cmd/mira-tls-client/        mTLS demonstration client
  cmd/mira-gateway/           Browser gateway command
  gateway/                    HTTP/JWT/CORS/session implementation
  browser/client.ts           Browser SDK example
  mobile/                     Go mobile facade
  durable/ journal/ resume/   Durable processing and replay
  cluster/                    Ownership/fencing/outbox foundations
  conformance/                Independent wire and peer tests
  scripts/verify-v7.sh        MIRA development verification
```

## Appendix B. Recommended staged adoption path

**Stage 1: Classroom / desktop proof.** Run section 5 on localhost, show protocols/receipts and explain limits.  
**Stage 2: Internal integration.** Make a custom handler using `mira.DialTLS`/`ListenTLS`; test server-push and authorization.  
**Stage 3: Gated staging.** Provision real identities, browser delegation where applicable, WAL if needed, monitoring and explicit feature gates.  
**Stage 4: External validation.** Independent audit, prolonged carrier/WAN soak, production database failover tests, mobile device tests and matched benchmark comparisons.  
**Stage 5: Carefully gated deployment.** Only after stage 4 succeeds and operational controls are approved.

## Appendix C. Troubleshooting commands

```bash
# Compile all source
go build ./...

# Type-check / diagnostics
go vet ./...

# Unit + integration regression
go test ./... -count=1 -timeout 120s

# Benchmark with mTLS server running
go run ./cmd/mira-bench -tcp 127.0.0.1:7448 -udp 127.0.0.1:7447 \
  -cert ./dev-certs/client.pem -key ./dev-certs/client.key \
  -ca ./dev-certs/root.pem -name server.local -clients 2 -messages 50 -bytes 256

# Operator-only local monitoring endpoint
curl -fsS http://127.0.0.1:9109/metrics
```

## Appendix D. Known v7 limitations at a glance

- Fresh-DH post-compromise in-session rekeying is not implemented.
- Old in-memory UDP cipher/session state is not recovered across process death.
- Fully automatic real PostgreSQL multi-node failover is not verified.
- Mobile AAR/XCFramework artifacts are not supplied or physical-device tested.
- Production cross-language mTLS client interoperability is not demonstrated.
- Real 4G/5G/NAT64/long-duration WAN and independently matched performance testing remain incomplete.
- Independent cryptographic/security audit and formal production certification remain outstanding.

## Appendix E. Historical standalone v7 source startup fix

This section records a bug in the original standalone v7 source archive. The
corrected condition is already present in the MIRA source integrated into fh;
this is provided only to explain old reports and archived copies.

The original v7 server contains the following condition:

```go
if (*gatewayIdentity == "") != (*gatewayPublicKey != "" || *gatewayKeyring != "") {
    log.Fatal("gateway identity and delegation public key must be configured together")
}
```

It incorrectly rejects **both settings omitted**, which is the normal default when a gateway is not used. Replace `!=` with `==`:

```go
if (*gatewayIdentity == "") == (*gatewayPublicKey != "" || *gatewayKeyring != "") {
    log.Fatal("gateway identity and delegation public key must be configured together")
}
```

This correction does not change the MIRA packet wire format. Use this
repository's current code rather than applying the historical edit again.

---

**End of handbook.** Keep this document together with the corresponding exact source revision. For public production use, require independent security assessment and real-world infrastructure testing; successful local integration tests do not establish universal reliability or safety.
