GO ?= go
.PHONY: run test vet race build wasm
run:
	$(GO) run ./cmd/api
test:
	$(GO) test ./...
vet:
	$(GO) vet ./...
race:
	$(GO) test -race ./...
build:
	$(GO) build ./cmd/api
wasm:
	@mkdir -p wasm/dist
	GOOS=js GOARCH=wasm CGO_ENABLED=0 $(GO) build -trimpath -buildvcs=false -ldflags="-s -w" -o wasm/dist/securefetch.wasm ./wasm/cmd/securefetch
	@command -v npm >/dev/null && (cd wasm && npm install && npm run build:ts) || echo "npm unavailable; Go WASM binary was built, install Node.js to build TypeScript assets"
