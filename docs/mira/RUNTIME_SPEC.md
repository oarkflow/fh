# MIRA-T/1 v4 wire reference (experimental)

**Normative behavior lives in `mira/` and `internal/wire/`.** No publication or independent protocol security review. Incompatible with wire v3: no mixed-version rolling upgrade support.

## UDP packet header (26 octets, network byte order)

| Offset | Size | Name | Description |
|---:|---:|---|---|
| 0 | 2 | Magic | `0x4D54` |
| 2 | 1 | Version | **4** |
| 3 | 1 | Kind | HELLO/HELLO_ACK/FINISH/DATA/RESET kinds as implemented by `internal/wire/packet.go` |
| 4 | 1 | Flags | DATA ACK-required flag and reserved bits |
| 5 | 1 | HeaderLen | **26** |
| 6 | 8 | CID | Connection identifier |
| 14 | 8 | PacketNo | Directional packet number |
| 22 | 2 | Epoch | Symmetric traffic key epoch |
| 24 | 2 | BodyLen | Exact body size after the header |

Maximum accepted datagram: **1472 bytes**; ordinary default emission is smaller unless a larger path probe succeeds. DATA body is AES-256-GCM ciphertext and a 16-byte authentication tag; fixed authenticated overhead totals 42 bytes before encoded frame and UDP/IP overhead. All 26 header bytes are authenticated as AEAD associated data. UDP amplification limits and path validation apply to unknown source paths.

## Frame TLV

`Type(byte) | Flags(byte) | BodyLength(canonical uvarint) | Body`, bounded and strictly parsed; receiver rejects noncanonical varints. Existing type IDs 1–15 are retained. Types added in v4:

| ID | Name | Body |
|---:|---|---|
| 1 | DATA | `ChannelID(varint) | MessageID(varint) | Total(varint) | Offset(varint) | FragmentSize(varint) | FragmentBytes` |
| 2 | ACK | Legacy single-number frame compatibility reader; engine emits ACK_RANGES. |
| 3 | OPEN | `ChannelID(varint) | Delivery(byte) | Priority(byte) | MaxMessage(varint) | LeaseMillis(varint)` |
| 4 | OPEN_ACK | `ChannelID(varint)` |
| 5 | CLOSE | `ChannelID(varint)` |
| 6 | RESET | `ChannelID(varint)` |
| 7 | CREDIT | absolute max byte credit (varint) |
| 8 | PING | liveness / handshake ready |
| 9 | RECEIPT | `ChannelID | MessageID | ReceiptLevel` |
| 11 | PATH_CHALLENGE | eight random bytes |
| 12 | PATH_RESPONSE | eight echoed bytes |
| 14 | GOAWAY | empty |
| 15 | ACK_RANGES | `Count(varint)`, pairs `Low,High` inclusive, decreasing, nonoverlapping. **32 ranges**, **512 aggregate packet numbers** maximum. |
| 16 | MTU_PROBE | authenticated padding; ACK confirms tested packet size |
| 17 | HALF_CLOSE | `ChannelID(varint) | FinalMessageID(varint)`; applies to ordered channel writes |

The legacy DATA layout changed by adding `FragmentSize`; therefore v3 and v4 cannot exchange DATA. The reassembler requires a fixed fragment size for every fragment of the same message and bounds total allocation. `BestEffort`/`LatestState` remain restricted to single-datagram payloads, without retransmission. Reliable fragments may be resent as identical ciphertext without a reused-nonce new encryption operation.

## Recommended TLS 1.3 bootstrap

A separate TCP TLS 1.3 control connection uses mutual certificate verification, ALPN `mira-t/1-udp-bootstrap`, and TLS exporter label `EXPORTER-MIRA-T1-UDP-v1`. Server-side identity derives from the certificate with a verified chain. This cryptographically anchors a new UDP traffic master and demands an authenticated return-path check. Browser access is not handled by this native handshake.

The legacy stateless HELLO/HELLO_ACK PSK+X25519 flow remains available only on explicit native legacy listeners for controlled development. The mTLS server profile does not accept that legacy authentication flow.

## Epochs and packet ACK semantics

Outgoing/incoming AEAD keys are independent and evolve via a direction-specific one-way symmetric epoch chain each 4096 packet numbers. A small old-key receive window allows packet reordering; key updates are not a fresh-DH post-compromise rekey. AEAD packet numbers must never repeat with the same key; ciphertext retransmissions are byte-identical. Transport ACK signifies volatile acceptance, while stored/processed receipts require an explicit application acknowledgment after the relevant operation.

## Recovery and interoperability

A reconnected device reauthenticates over a new TLS/bootstrap connection and requests journal replay by cursor. It does **not** resume the original UDP session's inflight packet state. A migrated socket is handled by authenticated path validation without dropping the live session. The optional Postgres outbox does not alter transport wire format. `conformance/mira_wire_v4.py` is a second-language parser for **framing only**, not a compatible Python transport endpoint.
