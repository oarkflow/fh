# MIRA-T/1 quick start in fh

This guide runs the MIRA examples from the `fh` repository. MIRA wire version
4 is an opt-in custom UDP transport; it is not HTTP/3, QUIC, WebSocket, or a
finished messaging application. Read the [complete examples guide](../../examples/mira/README.md)
for the protocol format, real application scenarios, browser gateway, durable
replay, clustering, and mobile facade.

## Requirements

- The Go version required by the root `go.mod`.
- Localhost TCP and UDP access.
- A terminal. The certificate generator creates development-only credentials.

## Generate local test certificates

Run from the repository root. The target directory must not already exist.

```sh
go run ./cmd/mira-dev-certs -out ./dev-certs
```

This creates a short-lived test CA, a server certificate for `server.local`,
and a client certificate whose identity is `device-1`. Never use these keys
outside local development.

## Start the mTLS server

In terminal A:

```sh
go run ./examples/mira/native/server \
  -udp 127.0.0.1:7447 -tcp 127.0.0.1:7448 \
  -cert ./dev-certs/server.pem -key ./dev-certs/server.key \
  -ca ./dev-certs/root.pem -allow device-1
```

The TCP listener authenticates the TLS 1.3 bootstrap. Encrypted MIRA packets
travel over UDP. The server verifies the client certificate and authorizes its
identity to open channels.

## Send a message

In terminal B:

```sh
go run ./examples/mira/native/client \
  -tcp 127.0.0.1:7448 -udp 127.0.0.1:7447 \
  -cert ./dev-certs/client.pem -key ./dev-certs/client.key \
  -ca ./dev-certs/root.pem -name server.local \
  -message 'Hello from MIRA'
```

Expected output: `server replied: echo: Hello from MIRA`.

## Next steps

- [Complete MIRA guide and real-use examples](../../examples/mira/README.md)
- [Wire and runtime specification](RUNTIME_SPEC.md)
- [Architecture](ARCHITECTURE.md)
- [Deployment guide](DEPLOYMENT.md) and [operator runbook](RUNBOOK.md)
- [Security and staging limits](../../MIRA_SECURITY.md)
