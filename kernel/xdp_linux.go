//go:build linux

package kernel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

func (m *XDPManager) Attach() error {
	if m == nil {
		return errors.New("fh: nil XDP manager")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.attached {
		return nil
	}
	iface := strings.TrimSpace(m.cfg.Interface)
	if iface == "" {
		return errors.New("fh: XDP interface is required")
	}
	object := strings.TrimSpace(m.cfg.ObjectPath)
	if object == "" {
		object = strings.TrimSpace(os.Getenv("FH_XDP_OBJECT"))
	}
	if object == "" {
		for _, candidate := range []string{"kernel/xdp/fh_xdp.o", "/usr/lib/fh/fh_xdp.o", "/usr/local/lib/fh/fh_xdp.o"} {
			if _, err := os.Stat(candidate); err == nil {
				object = candidate
				break
			}
		}
	}
	if object == "" {
		return errors.New("fh: XDP object not found; build kernel/xdp/fh_xdp.o or set XDP.ObjectPath/FH_XDP_OBJECT")
	}
	if _, err := os.Stat(object); err != nil {
		return fmt.Errorf("fh: XDP object %q: %w", object, err)
	}
	section := m.cfg.Section
	if section == "" {
		section = "xdp"
	}

	if m.cfg.PinPath != "" {
		if err := m.attachPinned(object, section); err != nil {
			return err
		}
	} else {
		if len(m.cfg.ProtectedPorts) != 0 || m.cfg.RatePerSecond != 0 || m.cfg.Burst != 0 {
			return errors.New("fh: XDP PinPath is required for protected ports or rate policy")
		}
		mode := xdpIPMode(m.cfg.Mode)
		args := []string{"link", "set", "dev", iface, mode, "obj", object, "sec", section}
		if out, err := exec.Command("ip", args...).CombinedOutput(); err != nil {
			return fmt.Errorf("fh: ip XDP attach: %w: %s", err, strings.TrimSpace(string(out)))
		}
	}
	m.attached = true
	return nil
}

func (m *XDPManager) attachPinned(object, _ string) error {
	pin := filepath.Clean(m.cfg.PinPath)
	if pin == "." || pin == string(filepath.Separator) {
		return errors.New("fh: unsafe XDP pin path")
	}
	_, statErr := os.Stat(pin)
	createdRoot := errors.Is(statErr, os.ErrNotExist)
	progDir := filepath.Join(pin, "progs")
	mapDir := filepath.Join(pin, "maps")
	if err := os.MkdirAll(progDir, 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(mapDir, 0o700); err != nil {
		if createdRoot {
			_ = os.RemoveAll(pin)
		}
		return err
	}
	cleanup := func() {
		if createdRoot {
			_ = os.RemoveAll(pin)
		}
	}
	args := []string{"prog", "loadall", object, progDir, "type", "xdp", "pinmaps", mapDir}
	if out, err := exec.Command("bpftool", args...).CombinedOutput(); err != nil {
		cleanup()
		return fmt.Errorf("fh: bpftool XDP load: %w: %s", err, strings.TrimSpace(string(out)))
	}
	m.ownedPin = createdRoot
	prog := filepath.Join(progDir, "fh_xdp")
	mode := xdpIPMode(m.cfg.Mode)
	args = []string{"link", "set", "dev", m.cfg.Interface, mode, "pinned", prog}
	if out, err := exec.Command("ip", args...).CombinedOutput(); err != nil {
		cleanup()
		return fmt.Errorf("fh: ip pinned XDP attach: %w: %s", err, strings.TrimSpace(string(out)))
	}
	for _, port := range m.cfg.ProtectedPorts {
		if port == 0 {
			continue
		}
		if err := updatePinnedMap(filepath.Join(mapDir, "fh_ports"), portKey(port), []byte{1}); err != nil {
			_ = DetachXDP(m.cfg.Interface, m.cfg.Mode)
			cleanup()
			return err
		}
	}
	if m.cfg.RatePerSecond > 0 || m.cfg.Burst > 0 {
		if err := m.updateRateMap(mapDir); err != nil {
			_ = DetachXDP(m.cfg.Interface, m.cfg.Mode)
			cleanup()
			return err
		}
	}
	return nil
}

func (m *XDPManager) updateRateMap(mapDir string) error {
	rate := m.cfg.RatePerSecond
	burst := m.cfg.Burst
	if burst == 0 {
		burst = rate
	}
	value := make([]byte, 16)
	nativeByteOrder().PutUint64(value[:8], rate)
	nativeByteOrder().PutUint64(value[8:], burst)
	if err := updatePinnedMap(filepath.Join(mapDir, "fh_config"), []byte{0, 0, 0, 0}, value); err != nil {
		return fmt.Errorf("fh: configure XDP rate map (%s/%s): %w", strconv.FormatUint(rate, 10), strconv.FormatUint(burst, 10), err)
	}
	return nil
}

// SetRateLimit updates the pinned per-source packet rate configuration.
func (m *XDPManager) SetRateLimit(rate, burst uint64) error {
	if m == nil {
		return errors.New("fh: nil XDP manager")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	mapDir, err := m.policyMapDir()
	if err != nil {
		return err
	}
	m.cfg.RatePerSecond = rate
	m.cfg.Burst = burst
	return m.updateRateMap(mapDir)
}

// ProtectPort activates XDP admission for one TCP destination port.
func (m *XDPManager) ProtectPort(port uint16) error {
	if m == nil {
		return errors.New("fh: nil XDP manager")
	}
	if port == 0 {
		return errors.New("fh: protected XDP port must be non-zero")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	mapDir, err := m.policyMapDir()
	if err != nil {
		return err
	}
	return updatePinnedMap(filepath.Join(mapDir, "fh_ports"), portKey(port), []byte{1})
}

// UnprotectPort removes one TCP destination port from XDP admission.
func (m *XDPManager) UnprotectPort(port uint16) error {
	if m == nil {
		return errors.New("fh: nil XDP manager")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	mapDir, err := m.policyMapDir()
	if err != nil {
		return err
	}
	return deletePinnedMapKey(filepath.Join(mapDir, "fh_ports"), portKey(port))
}

func (m *XDPManager) setBlockedIP(addr netip.Addr, blocked bool) error {
	if m == nil {
		return errors.New("fh: nil XDP manager")
	}
	if !addr.IsValid() {
		return errors.New("fh: invalid XDP block address")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	mapDir, err := m.policyMapDir()
	if err != nil {
		return err
	}
	var mapName string
	var key []byte
	if addr.Is4() {
		v := addr.As4()
		mapName = "fh_block_v4"
		key = append([]byte(nil), v[:]...)
	} else {
		v := addr.As16()
		mapName = "fh_block_v6"
		key = append([]byte(nil), v[:]...)
	}
	path := filepath.Join(mapDir, mapName)
	if blocked {
		return updatePinnedMap(path, key, []byte{1})
	}
	return deletePinnedMapKey(path, key)
}

func (m *XDPManager) policyMapDir() (string, error) {
	if strings.TrimSpace(m.cfg.PinPath) == "" {
		return "", errors.New("fh: XDP PinPath is required for policy updates")
	}
	return filepath.Join(filepath.Clean(m.cfg.PinPath), "maps"), nil
}

func (m *XDPManager) Detach() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.attached {
		return nil
	}
	if err := DetachXDP(m.cfg.Interface, m.cfg.Mode); err != nil {
		return err
	}
	m.attached = false
	if m.ownedPin && m.cfg.PinPath != "" {
		_ = os.RemoveAll(filepath.Clean(m.cfg.PinPath))
		m.ownedPin = false
	}
	return nil
}

// DetachXDP removes any XDP program attached in the requested mode, regardless
// of which process attached it.
func DetachXDP(iface string, mode XDPMode) error {
	iface = strings.TrimSpace(iface)
	if iface == "" {
		return errors.New("fh: XDP interface is required")
	}
	args := []string{"link", "set", "dev", iface, xdpIPMode(mode), "off"}
	out, err := exec.Command("ip", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("fh: XDP detach: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func defaultXDPPinPath(iface string) string {
	var b strings.Builder
	for _, r := range iface {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		b.WriteString("default")
	}
	return filepath.Join("/sys/fs/bpf/fh", b.String())
}

func updatePinnedMap(path string, key, value []byte) error {
	args := []string{"map", "update", "pinned", path, "key", "hex"}
	args = append(args, hexArgs(key)...)
	args = append(args, "value", "hex")
	args = append(args, hexArgs(value)...)
	if out, err := exec.Command("bpftool", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("fh: bpftool map update %s: %w: %s", path, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func deletePinnedMapKey(path string, key []byte) error {
	args := []string{"map", "delete", "pinned", path, "key", "hex"}
	args = append(args, hexArgs(key)...)
	if out, err := exec.Command("bpftool", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("fh: bpftool map delete %s: %w: %s", path, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func hexArgs(v []byte) []string {
	out := make([]string, len(v))
	for i, b := range v {
		out[i] = fmt.Sprintf("%02x", b)
	}
	return out
}

func portKey(port uint16) []byte {
	key := make([]byte, 2)
	binary.BigEndian.PutUint16(key, port)
	return key
}

func nativeByteOrder() binary.ByteOrder {
	switch runtime.GOARCH {
	case "ppc64", "s390x":
		return binary.BigEndian
	default:
		return binary.LittleEndian
	}
}

func xdpIPMode(mode XDPMode) string {
	switch mode {
	case XDPModeGeneric:
		return "xdpgeneric"
	case XDPModeOffload:
		return "xdpoffload"
	default:
		return "xdp"
	}
}
