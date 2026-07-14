GO ?= go
NPM ?= npm
TSC ?= tsc
GOROOT := $(shell $(GO) env GOROOT)
WASM_DIR := wasm
WASM_DIST := $(WASM_DIR)/dist
WASM_EXEC_SRC := $(firstword $(wildcard $(GOROOT)/lib/wasm/wasm_exec.js $(GOROOT)/misc/wasm/wasm_exec.js))
WASM_BINARY := $(WASM_DIST)/securefetch.wasm

.PHONY: all test secure-test wasm wasm-go wasm-ts wasm-runtime wasm-manifest wasm-check wasm-clean clean

all: test wasm

test:
	$(GO) test ./...

secure-test:
	$(GO) test ./pkg/securetransport ./mw/securetransport

wasm: wasm-check

wasm-go:
	@mkdir -p $(WASM_DIST)
	GOOS=js GOARCH=wasm CGO_ENABLED=0 $(GO) build -trimpath -buildvcs=false -ldflags="-s -w" -o $(WASM_BINARY) ./wasm/cmd/securefetch

wasm-runtime:
	@test -n "$(WASM_EXEC_SRC)" || (echo "wasm_exec.js was not found under $(GOROOT)" >&2; exit 1)
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
	@echo "Built $(WASM_BINARY) and copied Go runtime to $(WASM_DIST)/wasm_exec.js"

wasm-clean:
	rm -f $(WASM_DIST)/securefetch.wasm $(WASM_DIST)/wasm_exec.js $(WASM_DIST)/SHA256SUMS $(WASM_DIST)/asset-manifest.json
	rm -f $(WASM_DIST)/*.js $(WASM_DIST)/*.d.ts

clean: wasm-clean
