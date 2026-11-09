GO ?= go
NPM ?= npm

.PHONY: check test run keys wasm frontend check-secure-fetch

check:
	$(GO) test ./...

# Fails if any frontend source file outside lib/secure-client.ts calls
# fetch() directly or imports the plain-fetch helper (lib/http.ts) - every
# protected /api/* request must go through the WASM secure-fetch client.
# Run on its own (not part of `check`, which stays Node-free) and as part
# of `frontend`, since anyone rebuilding frontend assets already has npm.
check-secure-fetch:
	@command -v $(NPM) >/dev/null 2>&1 || (echo "npm was not found on PATH. Install Node.js 22.6+." >&2; exit 1)
	cd web/frontend && $(NPM) run check:secure-fetch

test:
	$(GO) test ./...

run:
	./run.sh

keys:
	@$(GO) run ./cmd/server -generate-transport-key
	@$(GO) run ./cmd/server -generate-signing-key

# web/public/app.js and app.css are build output, not checked in (see
# web/frontend/README.md) - the server has nothing to serve at /assets/*
# until this has run at least once. Also re-run after changing
# web/frontend/src. Requires Node.js 22.6+.
frontend: check-secure-fetch
	@command -v $(NPM) >/dev/null 2>&1 || (echo "npm was not found on PATH. Install Node.js 22.6+." >&2; exit 1)
	cd web/frontend && $(NPM) install && $(NPM) run build

# TinyGo pins a maximum supported Go version below what this project's
# go.mod requires (0.41.x supports Go 1.19-1.26), so `go` on PATH usually
# needs to be a matching 1.26.x for this target specifically, not whatever
# newer toolchain `go build`/`go test` use elsewhere. If tinygo reports
# "requires go version 1.19 through 1.26", install one side by side and put
# it first on PATH for this command:
#   go install golang.org/dl/go1.26.5@latest && go1.26.5 download
#   PATH="$$(go1.26.5 env GOROOT)/bin:$$PATH" make wasm
TINYGO ?= tinygo
wasm:
	@mkdir -p web/wasm
	@test -n "$(WASM_TRUSTED_ORIGIN)" -a -n "$(WASM_TRUSTED_TRANSPORT_KEY)" -a -n "$(WASM_TRUSTED_TRANSPORT_KEY_ID)" -a -n "$(WASM_TRUSTED_RESPONSE_KEY)" -a -n "$(WASM_TRUSTED_RESPONSE_KEY_ID)" || (echo "all five WASM_TRUSTED_* values are required" >&2; exit 1)
	@command -v $(TINYGO) >/dev/null 2>&1 || (echo "tinygo was not found on PATH. Install it (e.g. 'brew install tinygo')." >&2; exit 1)
	@command -v wasm-opt >/dev/null 2>&1 || (echo "wasm-opt was not found on PATH. TinyGo's wasm build shells out to it; install binaryen (e.g. 'brew install binaryen')." >&2; exit 1)
	$(TINYGO) build -target wasm -no-debug -ldflags="-X main.embeddedTrustedOrigin=$(WASM_TRUSTED_ORIGIN) -X main.embeddedTransportPublicKey=$(WASM_TRUSTED_TRANSPORT_KEY) -X main.embeddedTransportKeyID=$(WASM_TRUSTED_TRANSPORT_KEY_ID) -X main.embeddedResponseSigningPublicKey=$(WASM_TRUSTED_RESPONSE_KEY) -X main.embeddedResponseSigningKeyID=$(WASM_TRUSTED_RESPONSE_KEY_ID)" -o web/wasm/securefetch.wasm github.com/oarkflow/fh/wasm/cmd/securefetch
	@echo "tinygo build: $$(wc -c < web/wasm/securefetch.wasm) bytes"
	cp "$$($(GO) list -m -f '{{.Dir}}' github.com/oarkflow/fh)/wasm/cmd/securefetch/wasm_exec.js" web/wasm/wasm_exec.js
	$(GO) run github.com/oarkflow/fh/wasm/cmd/manifest -dir web/wasm
