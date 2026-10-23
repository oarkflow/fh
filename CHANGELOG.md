# Changelog

All notable changes to `fh` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- **Redirect Middleware (`mw/redirect`)**: HTTP redirect handler supporting automatic HTTPS enforcement, canonical domain/host redirection, and path mapping rules.
- **Trailing Slash Middleware (`mw/trailingslash`)**: Normalizes trailing slashes via HTTP 301/308 redirects or transparent zero-roundtrip in-flight rewrites.
- **Favicon Middleware (`mw/favicon`)**: Serves favicon from memory or file with ETag validation, long-lived caching, and 204 No Content fallback.
- **JSON Buffer Growth**: Implemented grow-on-demand buffer allocation (`GrowBuffer`) for direct JSON serializations and interleaved benchmarks.
- **Documentation Verification**: Full suite doc-example compile-checker testing all 32 subsystem guides.
- **Supply Chain Integrity**: Added `go mod verify` step to lint pipeline.

### Changed
- **Minimum Go Version**: Bumped minimum supported toolchain to Go 1.27.3 in `go.mod` and `README.md` to ensure consumers build with patched crypto/x509 libraries.

### Fixed
- **Cache Store Expiry**: Increased TTL tolerance in `mw/cache/file_store_test.go` to prevent scheduling races under high-concurrency CI environments.
- **Doc Test Compatibility**: Reclassified illustrative architectural pseudocode snippets in `docs/runtime-execution-fabric.md` and `docs/ref-no-code-platform.md` to prevent compilation false positives.

---

## [v0.0.26] - 2026-10-18

### Added
- **Runtime Execution Fabric (REF)**: Zero-dependency decoupling layer connecting `fh` handlers with declarative intent-based execution workflows.
- **Production App Template**: Checked-in reference application in `examples/production-app` generated via `fh-init`.
- **WASM Trusted Pipeline**: Added `make wasm-trusted` target enforcing loud failures on missing WASM credentials.

### Fixed
- **TCP Guard**: Resolved false-positive connection resets under rapid loopback probe patterns.
- **WASM Client**: Fixed error propagation and configuration handling in browser-side secure fetch client.

---

## [v0.0.25] - 2026-10-14

### Added
- **Method Override & Proxy Protocol**: Enhanced proxy validation and header normalization routines with configurable logging.

### Changed
- **Dependencies**: Cleaned up indirect dependencies across root and example modules.

---

## [v0.0.24] - 2026-10-10

### Added
- **Scaffolding CLI (`cmd/fh-init`)**: Tool to generate hardened, production-ready application skeletons with RBAC, session management, SQLite storage, and audit logs.
- **Kernel Server Improvements**: Configurable port mapping and graceful socket handoff for kernel-assisted listeners.

---

## [v0.0.23] - 2026-10-06

### Added
- **Security Middlewares**: Added `mw/hostguard` and `mw/security` for Host header validation and automated Content Security Policy enforcement.
- **Stress & Soak Tests**: Load testing scenarios covering graceful shutdown, soak conditions, and slow-client mitigation.

---

## [v0.0.22] - 2026-10-02

### Added
- **WASM Secure Fetch**: WebAssembly-based client for encrypted, authenticated client-server communication.
- **Middleware Suite**: Added `mw/smartcache`, `mw/slidingwindow`, `mw/ipthrottle`, `mw/ipreputation`, `mw/coalesce`, `mw/timestamp`, and `mw/fetchmetadata`.

### Fixed
- **Cache Eviction**: Improved locking and eviction accuracy in `mw/cache`.
- **Error Formatting**: Standardized error responses to use RFC 9457 Problem Details across all core handlers.

---

## [v0.0.21] - 2026-09-28

### Added
- **Validation Middleware**: Added declarative request validation middleware (`mw/validate`).
- **Shared State Provider**: Atomic in-memory and clustered shared state engine.
- **Merkle Audit Chains**: Cryptographically verifiable audit logging in `merkle_audit.go`.

---

## [v0.0.20] - 2026-09-20

### Added
- **Kernel Transport**: Native OS socket polling engines (epoll on Linux, kqueue on macOS/BSD, IOCP on Windows).
- **HTTP/2 Full Protocol**: In-repo implementation of RFC 7540 HTTP/2 framing, multiplexing, flow control, and server push.
- **Zero-Allocation Context**: Pooled request context with safe detachment primitives (`BodyCopy`, `SafeParams`).
