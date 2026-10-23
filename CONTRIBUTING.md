# Contributing to fh

Thank you for your interest in contributing to `fh`! We welcome contributions from the community, whether reporting bugs, improving documentation, submitting pull requests, or benchmarking performance.

---

## Code of Conduct

We are committed to providing a welcoming, inclusive, and harassment-free environment for everyone. Please be respectful, constructive, and empathetic in all interactions.

---

## Security Vulnerabilities

**Do not report security vulnerabilities via public issues or pull requests.**

If you discover a potential security vulnerability, follow our responsible disclosure process detailed in [SECURITY.md](SECURITY.md). Reports should be emailed privately to `s.baniya.np@gmail.com`.

---

## Reporting Issues

Before opening a new issue:
1. Search existing [Issues](https://github.com/oarkflow/fh/issues) to verify the problem has not already been reported or resolved.
2. Verify you are using the latest version of `fh`.
3. Provide a clear, minimal, reproducible example (a self-contained Go file or test case).
4. Include:
   - Operating system and architecture (`go env GOOS GOARCH`)
   - Go version (`go version`)
   - `fh` version or commit SHA
   - Expected vs. actual behavior
   - Relevant stack traces or error logs

---

## Development Setup

### Prerequisites
- **Go 1.27.3 or higher** (installed and available in your `PATH`)
- `git`
- Recommended tools:
  ```bash
  go install honnef.co/go/tools/cmd/staticcheck@latest
  go install golang.org/x/vuln/cmd/govulncheck@latest
  ```

### Getting the Code
```bash
git clone https://github.com/oarkflow/fh.git
cd fh
go mod download
```

---

## Coding Standards & Conventions

1. **Idiomatic Go**: Follow standard Go idioms and [Effective Go](https://go.dev/doc/effective_go). All code must be formatted with standard `gofmt`.
2. **Context Lifecycle (`fh.Ctx`)**:
   - `fh.Ctx` is an interface; pass it by value, not by pointer (`func(c fh.Ctx) error`).
   - The default context implementation is pooled. Memory references (like slices returned from `c.Body()`, `c.Params()`, etc.) are recycled after the handler returns. If values need to survive beyond the handler lifetime (e.g., in a separate goroutine), use `c.BodyCopy()` or enable `SafeParams`.
3. **Error Handling**:
   - Return errors from handlers; do not swallow unexpected failures.
   - Use `fh.NewHTTPError(code, machineKey, message)` for consistent RFC 9457 Problem Details error responses.
   - Do not leak internal stack traces or connection credentials in client-facing error payloads.
4. **Minimal External Dependencies**:
   - `fh` maintains a zero-external-dependency posture for its core runtime (outside standard library and `golang.org/x/*` cryptographic utilities). Third-party drivers should live in adapter modules or `examples/`.
5. **Linting & Hygiene**:
   Run the full lint suite before committing:
   ```bash
   # Format check
   test -z "$(gofmt -l . | grep -v '^wasm/dist/')"

   # Vet
   go vet ./...

   # Staticcheck
   staticcheck ./...

   # Vulnerability check
   govulncheck ./...

   # Module verification
   go mod verify
   ```

---

## Testing Guidelines

Every bug fix and new feature must include tests.

1. **Unit & Integration Tests**:
   ```bash
   go test ./... -count=1
   ```
2. **Race Detector**:
   All tests must pass under the race detector without warnings:
   ```bash
   go test -race ./... -count=1
   ```
3. **Documentation Example Verification**:
   All code snippets in `docs/*.md` and `README.md` are compile-tested by the test harness. Ensure they pass:
   ```bash
   go test . -run TestDocExamples -v
   ```
4. **Fuzzing & Benchmarks**:
   If modifying network parsing, frame handling, or buffer codecs, run relevant fuzzers and benchmarks:
   ```bash
   go test -bench=. -benchmem ./...
   ```

---

## Pull Request Lifecycle

1. **Create a Topic Branch**:
   ```bash
   git checkout -b feature/my-new-feature
   # or
   git checkout -b fix/issue-description
   ```
2. **Commit Messages**:
   We recommend Conventional Commits formatting:
   - `feat: add adaptive rate limiter option`
   - `fix: prevent timeout leak in stream flush`
   - `docs: clarify context retention rules`
   - `perf: optimize buffer pooling in json writer`
3. **Developer Certificate of Origin (DCO)**:
   By contributing, you certify that you have the right to submit the code under the project's MIT license. Please sign your commits:
   ```bash
   git commit -s -m "feat: description of change"
   ```
4. **Push and Open a Pull Request**:
   - Ensure the CI pipeline passes cleanly on all stages (lint, test, fuzz).
   - Describe the problem being solved, the approach taken, and link any related issues.
   - Keep pull requests focused; avoid combining unrelated refactorings with bug fixes.
