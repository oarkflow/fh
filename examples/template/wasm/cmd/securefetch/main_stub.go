//go:build !js || !wasm

package main

// The secure fetch runtime is built with GOOS=js GOARCH=wasm. This stub keeps
// repository-wide native builds and tests deterministic.
func main() {}
