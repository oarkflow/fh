GO ?= go
NPM ?= npm
TSC ?= tsc
TINYGO ?= tinygo
WASM_DIR := wasm
WASM_DIST := $(WASM_DIR)/dist
WASM_EXAMPLE_DIR := examples/secure_wasm/wasm
# wasm_exec.js for a TinyGo-compiled binary must come from TinyGo, not the Go
# toolchain, and specifically from the patched copy checked in next to
# cmd/securefetch -- see that file's header comment for why the stock
# TinyGo-bundled wasm_exec.js does not work as-is for this binary.
WASM_EXEC_SRC := $(WASM_DIR)/cmd/securefetch/wasm_exec.js
WASM_BINARY := $(WASM_DIST)/securefetch.wasm
XDP_SOURCE ?= kernel/xdp/fh_xdp.c
XDP_OBJECT ?= kernel/xdp/fh_xdp.o
XDP_INTERFACE ?= eth0
XDP_MODE ?= native
XDP_PORTS ?= 80,443
XDP_PIN ?= /sys/fs/bpf/fh/$(XDP_INTERFACE)

WASM_TRUST_VALUES := $(strip $(WASM_TRUSTED_ORIGIN)$(WASM_TRUSTED_TRANSPORT_KEY)$(WASM_TRUSTED_TRANSPORT_KEY_ID)$(WASM_TRUSTED_RESPONSE_KEY)$(WASM_TRUSTED_RESPONSE_KEY_ID))
WASM_TRUST_LDFLAGS :=
ifneq ($(WASM_TRUST_VALUES),)
ifeq ($(strip $(WASM_TRUSTED_ORIGIN)),)
$(error WASM_TRUSTED_ORIGIN is required when building a trusted WASM artifact)
endif
ifeq ($(strip $(WASM_TRUSTED_TRANSPORT_KEY)),)
$(error WASM_TRUSTED_TRANSPORT_KEY is required when building a trusted WASM artifact)
endif
ifeq ($(strip $(WASM_TRUSTED_TRANSPORT_KEY_ID)),)
$(error WASM_TRUSTED_TRANSPORT_KEY_ID is required when building a trusted WASM artifact)
endif
ifeq ($(strip $(WASM_TRUSTED_RESPONSE_KEY)),)
$(error WASM_TRUSTED_RESPONSE_KEY is required when building a trusted WASM artifact)
endif
ifeq ($(strip $(WASM_TRUSTED_RESPONSE_KEY_ID)),)
$(error WASM_TRUSTED_RESPONSE_KEY_ID is required when building a trusted WASM artifact)
endif
WASM_TRUST_LDFLAGS := -X main.embeddedTrustedOrigin=$(WASM_TRUSTED_ORIGIN) -X main.embeddedTransportPublicKey=$(WASM_TRUSTED_TRANSPORT_KEY) -X main.embeddedTransportKeyID=$(WASM_TRUSTED_TRANSPORT_KEY_ID) -X main.embeddedResponseSigningPublicKey=$(WASM_TRUSTED_RESPONSE_KEY) -X main.embeddedResponseSigningKeyID=$(WASM_TRUSTED_RESPONSE_KEY_ID)
endif

.PHONY: all test check template-check secure-test kernel-test kernel-probe xdp-build xdp-attach xdp-detach wasm wasm-go wasm-ts wasm-runtime wasm-manifest wasm-check wasm-example wasm-clean clean

all: test wasm

test:
	$(GO) test ./...

check: test template-check

template-check:
	@tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
		$(GO) run ./cmd/fh-init -module example.com/fh/generated -dir "$$tmp"; \
		(cd "$$tmp" && $(GO) mod tidy && $(GO) test ./...)

secure-test:
	$(GO) test ./pkg/securetransport ./mw/securetransport

kernel-test:
	CGO_ENABLED=0 $(GO) test ./kernel . -run 'TestKernel|TestIOUring|TestPortKey|TestNormalizeKernel' -count=1

kernel-probe:
	$(GO) run ./cmd/fh-kernelctl probe

xdp-build:
	$(GO) run ./cmd/fh-kernelctl build-xdp -source $(XDP_SOURCE) -output $(XDP_OBJECT)

xdp-attach: xdp-build
	$(GO) run ./cmd/fh-kernelctl attach-xdp -interface $(XDP_INTERFACE) -mode $(XDP_MODE) -object $(XDP_OBJECT) -pin $(XDP_PIN) -ports $(XDP_PORTS)

xdp-detach:
	$(GO) run ./cmd/fh-kernelctl detach-xdp -interface $(XDP_INTERFACE) -mode $(XDP_MODE)

wasm: wasm-example

# TinyGo pins a maximum supported Go version well below this module's go.mod
# floor (0.41.x supports Go 1.19-1.26; go.mod requires >= 1.26.5), so `go` on
# PATH usually needs to be a matching 1.26.x, not whatever newer toolchain
# `go build`/`go test` use elsewhere in this repo. See wasm/README.md for the
# exact `go install golang.org/dl/go1.26.5@latest && go1.26.5 download` +
# PATH recipe if tinygo reports "requires go version 1.19 through 1.26".
wasm-go:
	@command -v $(TINYGO) >/dev/null 2>&1 || (echo "tinygo was not found on PATH. Install it (e.g. 'brew install tinygo') -- see wasm/README.md for the required Go/TinyGo version pairing." >&2; exit 1)
	@command -v wasm-opt >/dev/null 2>&1 || (echo "wasm-opt was not found on PATH. TinyGo's wasm build shells out to it; install binaryen (e.g. 'brew install binaryen')." >&2; exit 1)
	@mkdir -p $(WASM_DIST)
	$(TINYGO) build -target wasm -no-debug -ldflags="$(WASM_TRUST_LDFLAGS)" -o $(WASM_BINARY) ./wasm/cmd/securefetch
	@echo "tinygo build: $$(wc -c < $(WASM_BINARY)) bytes"

wasm-runtime:
	@test -s "$(WASM_EXEC_SRC)" || (echo "$(WASM_EXEC_SRC) is missing" >&2; exit 1)
	@mkdir -p $(WASM_DIST)
	cp "$(WASM_EXEC_SRC)" "$(WASM_DIST)/wasm_exec.js"

wasm-ts:
	cd $(WASM_DIR) && $(TSC) -p tsconfig.json

wasm-manifest: wasm-go wasm-runtime wasm-ts
	$(GO) run ./wasm/cmd/manifest -dir $(WASM_DIST)

wasm-check: wasm-manifest
	@test -s "$(WASM_BINARY)"
	@test -s "$(WASM_DIST)/wasm_exec.js"
	@test -s "$(WASM_DIST)/secure-fetch.js"
	@test -s "$(WASM_DIST)/asset-manifest.json"
	@cd $(WASM_DIST) && { \
		if command -v sha256sum >/dev/null 2>&1; then \
			sha256sum securefetch.wasm wasm_exec.js secure-fetch.js storage.js index.js; \
		else \
			shasum -a 256 securefetch.wasm wasm_exec.js secure-fetch.js storage.js index.js; \
		fi; \
	} > SHA256SUMS
	@echo "Built $(WASM_BINARY) and copied TinyGo runtime to $(WASM_DIST)/wasm_exec.js"

wasm-example: wasm-check
	@mkdir -p $(WASM_EXAMPLE_DIR)
	cp $(WASM_DIST)/securefetch.wasm $(WASM_DIST)/wasm_exec.js $(WASM_DIST)/secure-fetch.js $(WASM_DIST)/storage.js $(WASM_DIST)/index.js $(WASM_DIST)/secure-fetch.d.ts $(WASM_DIST)/storage.d.ts $(WASM_DIST)/index.d.ts $(WASM_DIST)/asset-manifest.json $(WASM_DIST)/SHA256SUMS $(WASM_EXAMPLE_DIR)/
	@echo "Synchronized complete WASM bundle to $(WASM_EXAMPLE_DIR)"

wasm-clean:
	rm -f $(WASM_DIST)/securefetch.wasm $(WASM_DIST)/wasm_exec.js $(WASM_DIST)/SHA256SUMS $(WASM_DIST)/asset-manifest.json
	rm -f $(WASM_DIST)/*.js $(WASM_DIST)/*.d.ts
	rm -f $(WASM_EXAMPLE_DIR)/securefetch.wasm $(WASM_EXAMPLE_DIR)/wasm_exec.js $(WASM_EXAMPLE_DIR)/SHA256SUMS $(WASM_EXAMPLE_DIR)/asset-manifest.json
	rm -f $(WASM_EXAMPLE_DIR)/*.js $(WASM_EXAMPLE_DIR)/*.d.ts

clean: wasm-clean
