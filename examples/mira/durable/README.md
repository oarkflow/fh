# Durable application replay example

The MIRA runtime does not restore old UDP session keys or in-flight packet
state after a process restart. Durable application replay is a separate layer:
persist accepted operations in the journal, reconnect with a fresh authenticated
session, then replay from the last application cursor.

## Run the provided server

From the repository root, create local test credentials and a private data
directory:

```sh
go run ./cmd/mira-dev-certs -out ./dev-certs
mkdir -m 700 -p ./tmp/mira
```

The server's encrypted WAL mode requires an owner-only JSON keyring. Use a
secret manager for non-test deployments. Example keyring shape:

```json
{"active":1,"keys":{"1":"REPLACE_WITH_64_HEX_CHARACTERS"}}
```

Create it with a fresh random key and restrict it to the current user. Never
commit real keys:

```sh
umask 077
printf '{"active":1,"keys":{"1":"%s"}}\n' "$(openssl rand -hex 32)" > ./tmp/mira/wal-keys.json
go run ./cmd/mira-tls-server \
  -udp 127.0.0.1:7447 -tcp 127.0.0.1:7448 \
  -cert ./dev-certs/server.pem -key ./dev-certs/server.key \
  -ca ./dev-certs/root.pem -allow device-1 \
  -wal ./tmp/mira/messages.wal -wal-keys ./tmp/mira/wal-keys.json
```

In another terminal, publish a stable operation ID and replay the event using
the mTLS client example:

```sh
go run ./examples/mira/durable/client \
  -tcp 127.0.0.1:7448 -udp 127.0.0.1:7447 \
  -cert ./dev-certs/client.pem -key ./dev-certs/client.key \
  -ca ./dev-certs/root.pem -name server.local \
  -id order-000001 -message 'first durable event' \
  -checkpoint /tmp/fh-mira-cursor
```

Run the same client later without `-message` to replay events after the saved
cursor. Delete the cursor file first if you want to replay from the beginning.
The cursor path must be absolute, its parent directory must already exist, and
the checkpoint file is created with owner-only permissions. `resume/` uses an
atomic, fsynced cursor update. See [`journal/`](../../../journal) and
[`resume/`](../../../resume).

`cmd/mira-journal-client` is a separate legacy loopback-only PSK demonstration;
it does not connect to the mTLS server above.

The WAL is single-writer. A stored receipt means the receiver explicitly
acknowledged its storage commit; it does not guarantee exactly-once effects in
an unrelated business database. Use a transactional outbox/inbox or an
idempotent downstream operation for those effects.
