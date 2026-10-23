# Versioning and API Stability Policy

This document defines the versioning scheme, stability expectations, and deprecation policy for `fh` (`github.com/oarkflow/fh`).

---

## Semantic Versioning

`fh` adheres to [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html) (`vMAJOR.MINOR.PATCH`):

- **MAJOR**: Incompatible API changes, removals, or architectural shifts.
- **MINOR**: Backwards-compatible new features, new middleware, or extensions.
- **PATCH**: Backwards-compatible bug fixes, performance improvements, and security patches.

---

## Pre-v1 Policy (`v0.y.z`)

`fh` is currently in **pre-v1 development** (`v0.0.x`). While the framework is battle-tested in real deployments, the pre-v1 designation indicates that API contracts are subject to refinement before locking in long-term backwards-compatibility guarantees.

### Guidelines for Pre-v1 Consumers:
1. **Pin Exact Releases**: Production applications should pin an explicit tag or commit hash in `go.mod`:
   ```text
   require github.com/oarkflow/fh v0.0.26
   ```
2. **Review Changelogs**: Check [CHANGELOG.md](../CHANGELOG.md) before updating to evaluate changes, new features, and deprecation notices.
3. **Patch Releases (`0.0.z`)**: Focus on security patches, bug fixes, performance optimizations, and non-breaking refinements.
4. **Breaking Changes**: If a signature change or breaking refactoring is required prior to v1.0, it will be highlighted in `CHANGELOG.md` with explicit migration instructions.

---

## Roadmap to v1.0.0 (Stability Guarantee)

Reaching `v1.0.0` signals a formal commitment to backwards compatibility:

1. **Frozen Core APIs**:
   - The primary interfaces (`fh.Ctx`, `fh.HandlerFunc`, `fh.ErrorHandler`) and core router methods (`app.Get`, `app.Post`, `app.Use`, `app.Listen`) will be strictly frozen.
   - Any signature changes to exported methods will require a major version bump (`v2.0.0`).
2. **Automated Compatibility Checks**:
   - CI will enforce API compatibility checks using tooling such as `apidiff` or `gorelease` against the latest release tag to detect unintended breaking changes before merging.
3. **Deprecation Cycle**:
   - Obsolete methods or configurations will be marked with a `// Deprecated:` comment at least **one minor version** before removal, providing alternatives and deprecation timelines.
4. **Supported Go Versions**:
   - `fh` supports the two most recent major Go releases (e.g. Go 1.27 and Go 1.26), matching the official Go release and security support window.

---

## Module Path Stability

The canonical module path for `fh` is:
```text
github.com/oarkflow/fh
```

When a major version `v2` is released in the future, it will follow Go standard module versioning practices (`github.com/oarkflow/fh/v2`).

---

## Security Backports

- For pre-v1 releases, security fixes are released immediately in the latest patch release on the `main` branch.
- After v1.0.0, critical security vulnerabilities will be backported to supported major/minor releases in accordance with our [Security Policy](../SECURITY.md).
