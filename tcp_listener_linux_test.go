//go:build linux

package fh

import (
	"net"
	"syscall"
	"testing"
)

func TestFrameworkTCPListenerDisablesMultipathTCP(t *testing.T) {
	t.Setenv("GODEBUG", "multipathtcp=1")
	var defaultCfg net.ListenConfig
	if !defaultCfg.MultipathTCP() {
		t.Fatal("test requires the Go MPTCP default to be enabled")
	}

	ln, err := listenTCP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	tcp, ok := ln.(*net.TCPListener)
	if !ok {
		t.Fatalf("listener has type %T, want *net.TCPListener", ln)
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var protocol int
	var socketErr error
	if err := raw.Control(func(fd uintptr) {
		protocol, socketErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_PROTOCOL)
	}); err != nil {
		t.Fatal(err)
	}
	if socketErr != nil {
		t.Fatal(socketErr)
	}
	if protocol != syscall.IPPROTO_TCP {
		t.Fatalf("listener socket protocol = %d, want TCP (%d)", protocol, syscall.IPPROTO_TCP)
	}
}
