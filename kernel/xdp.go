package kernel

import (
	"errors"
	"net/netip"
	"sync"
)

var ErrXDPUnsupported = errors.New("fh: XDP is unsupported on this platform")

// XDPManager owns an optional fh XDP program attachment and its pinned policy
// maps. Attach and Detach are idempotent. Policy updates require PinPath because
// bpftool addresses the verified kernel maps through bpffs.
type XDPManager struct {
	cfg XDPConfig
	mu  sync.Mutex

	attached bool
	ownedPin bool
}

func NewXDPManager(cfg XDPConfig) *XDPManager { return &XDPManager{cfg: cfg} }

// DefaultXDPPinPath returns the isolated bpffs directory used by automatic attachment.
func DefaultXDPPinPath(interfaceName string) string { return defaultXDPPinPath(interfaceName) }

// Attached reports whether this manager successfully attached the program.
func (m *XDPManager) Attached() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.attached
}

// BlockIP adds one IPv4 or IPv6 address to the pinned kernel drop map.
func (m *XDPManager) BlockIP(addr netip.Addr) error { return m.setBlockedIP(addr, true) }

// UnblockIP removes one IPv4 or IPv6 address from the pinned kernel drop map.
func (m *XDPManager) UnblockIP(addr netip.Addr) error { return m.setBlockedIP(addr, false) }
