//go:build !linux

package kernel

import "net/netip"

func defaultXDPPinPath(interfaceName string) string { return "" }

func (m *XDPManager) Attach() error                                    { return ErrXDPUnsupported }
func (m *XDPManager) Detach() error                                    { return nil }
func (m *XDPManager) SetRateLimit(rate, burst uint64) error            { return ErrXDPUnsupported }
func (m *XDPManager) ProtectPort(port uint16) error                    { return ErrXDPUnsupported }
func (m *XDPManager) UnprotectPort(port uint16) error                  { return ErrXDPUnsupported }
func (m *XDPManager) setBlockedIP(addr netip.Addr, blocked bool) error { return ErrXDPUnsupported }

func DetachXDP(iface string, mode XDPMode) error { return ErrXDPUnsupported }
