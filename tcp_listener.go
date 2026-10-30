package fh

import (
	"context"
	"net"
)

// listenTCP keeps framework-owned listeners on ordinary TCP. This reduces
// exposure to the MPTCP-dependent exploit of Linux CVE-2026-98432, but a
// patched Linux kernel is required to fix the underlying TCP vulnerability.
func listenTCP(addr string) (net.Listener, error) {
	var cfg net.ListenConfig
	cfg.SetMultipathTCP(false)
	return cfg.Listen(context.Background(), "tcp", addr)
}
