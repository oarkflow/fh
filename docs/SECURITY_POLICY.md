# Security Policy

fh is a networking-facing framework (HTTP/1.1, HTTP/2, WebSocket, TLS). We
treat parsing, protocol, authentication, and cryptographic issues as
high-priority and want to hear about them before they are disclosed publicly.

## Reporting a vulnerability

**Do not open a public issue or merge request for a suspected security
vulnerability.**

Email **s.baniya.np@gmail.com** with:

- a description of the vulnerability and its impact;
- the affected version/commit;
- steps to reproduce, or a minimal PoC (request bytes, a small Go program,
  etc.);
- whether you believe it is already being exploited.

You will receive an acknowledgment within **5 business days**. We aim to
provide an initial assessment (severity, affected versions, expected fix
timeline) within **10 business days** of acknowledgment.

We currently handle reports without a formal PGP channel. If you require
encrypted communication, say so in your first message and we will arrange a
key exchange before you send further details.

## Disclosure process

1. You report privately using the address above.
2. We confirm receipt and begin triage.
3. We develop and test a fix on a private branch.
4. We coordinate a disclosure date with you — by default, the earlier of
   (a) 90 days after the report, or (b) release of the fix — unless you and
   we agree to a different timeline (e.g. active exploitation warrants a
   faster release, or a more complex fix needs more time).
5. We publish the fix, credit the reporter (unless anonymity is requested),
   and describe the issue and impact in the release notes.

We ask that you not publicly disclose the issue until a fix is available and
the coordinated date has passed.

## Supported versions

fh is currently **pre-v1**; there is no long-term-support branch. Security
fixes are made against the `main` branch and released as the next tagged
version. Until a v1 compatibility policy is published (tracked as part of
the project's stability roadmap), pin an exact version/commit in production
and review changes before upgrading, rather than assuming backport support
for older tags.

Once v1 ships, this section will be updated with a supported-version table
and a backport policy for the current and previous major versions.

## Scope

In scope: the `fh` root package, `mw/*` middleware, `pkg/*` supporting
packages (HPACK, WebSocket, secure transport, HTTP signatures, storage
adapters), and the kernel-assisted transport paths (`kernel/`, `native_transport.go`).

### HTTP/2 flow control

`fh` implements HTTP/2 in Go and does not embed NGINX. Its flow-control
windows use checked signed 31-bit arithmetic; receive-credit refunds are
bounded before a `WINDOW_UPDATE` is sent. HPACK decoding and concurrent
streams are also capped by configuration. These controls address the
flow-control and resource-boundary classes raised by recent NGINX HTTP/2
advisories, but do not patch an NGINX reverse proxy deployed separately.
Operators using NGINX must apply the upstream fixed release for their branch.

Out of scope: findings that require an already-compromised host, denial of
service via unbounded resource use that is already documented as an
unmitigated limitation (see [README — Known Limitations](README.md#known-limitations)
and [`docs/production-readiness.md`](docs/production-readiness.md)), and
vulnerabilities in third-party dependencies (report those upstream — see
[`go.mod`](go.mod) for the current dependency list; run `govulncheck ./...`
to check your build).

## Security-relevant design docs

- [`docs/security.md`](docs/security.md) — TLS/mTLS, read budgets, trusted-proxy
  identity, message integrity.
- [`docs/production-readiness.md`](docs/production-readiness.md) — release
  gates for a production deployment.
- [`SECURITY_AUDIT_REPORT.md`](SECURITY_AUDIT_REPORT.md) — most recent internal
  hardening audit and its findings.
