# Hardened server example

This example enables the opt-in deployment security gate and a 64 MiB
process-wide buffered request body budget. It exposes `/health/live`,
`/health/ready`, and `/`. Readiness turns unhealthy during draining; register
dependency checks with `AddHealthCheck` for application-specific readiness.

Direct public origin with a real certificate:

```sh
go run ./examples/hardened-server -mode direct-origin -listen :8443 \
  -host api.example.com -cert /path/to/fullchain.pem -key /path/to/key.pem
```

Origin behind an edge that terminates TLS:

```sh
go run ./examples/hardened-server -mode trusted-edge -listen :8080 \
  -host api.example.com -trusted-proxies 10.20.0.0/16,2001:db8:20::/48
```

In trusted-edge mode, the app accepts only connections whose **socket peer**
is in the listed CIDRs. Keep the origin private to the edge as well. This
sample does not use forwarded client IP headers. If your application does,
configure the same trusted peers in its real-IP or PROXY-protocol middleware.

The example is stateless and declares one replica. For more replicas, set the
actual `ExpectedReplicas` value and use shared stores for any queue,
idempotency, replay, or session state you enable. Run the adapter contracts in
`pkg/storage/contracttest` against your real backend. The gate cannot check
firewall rules or your application authentication policy.

See [production readiness](../../docs/production-readiness.md) for limits,
shutdown hooks, and release checks.
