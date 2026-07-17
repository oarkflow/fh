package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/netip"
	"os"
	"strconv"
	"strings"

	"github.com/oarkflow/fh"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "probe":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		err = enc.Encode(fh.ProbeKernel())
	case "build-xdp":
		fs := flag.NewFlagSet("build-xdp", flag.ExitOnError)
		source := fs.String("source", "kernel/xdp/fh_xdp.c", "XDP C source")
		output := fs.String("output", "kernel/xdp/fh_xdp.o", "BPF object output")
		_ = fs.Parse(os.Args[2:])
		err = fh.BuildXDP(*source, *output)
	case "attach-xdp":
		err = attachXDP(os.Args[2:])
	case "detach-xdp":
		fs := flag.NewFlagSet("detach-xdp", flag.ExitOnError)
		iface := fs.String("interface", "", "network interface")
		mode := fs.String("mode", string(fh.XDPModeNative), "native, generic, or offload")
		_ = fs.Parse(os.Args[2:])
		err = fh.DetachXDP(*iface, fh.XDPMode(*mode))
	case "block-ip", "unblock-ip":
		err = updateIPPolicy(os.Args[1] == "block-ip", os.Args[2:])
	case "protect-port", "unprotect-port":
		err = updatePortPolicy(os.Args[1] == "protect-port", os.Args[2:])
	case "set-rate":
		err = updateRatePolicy(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func attachXDP(args []string) error {
	fs := flag.NewFlagSet("attach-xdp", flag.ExitOnError)
	iface := fs.String("interface", "", "network interface")
	object := fs.String("object", "kernel/xdp/fh_xdp.o", "BPF object")
	mode := fs.String("mode", string(fh.XDPModeNative), "native, generic, or offload")
	pin := fs.String("pin", "", "bpffs pin directory; defaults to /sys/fs/bpf/fh/<interface>")
	ports := fs.String("ports", "", "comma-separated protected TCP ports")
	rate := fs.Uint64("rate", 0, "packets per second per IPv4 source")
	burst := fs.Uint64("burst", 0, "token bucket capacity")
	_ = fs.Parse(args)
	protected, err := parsePorts(*ports)
	if err != nil {
		return err
	}
	if *pin == "" {
		*pin = fh.DefaultXDPPinPath(*iface)
	}
	manager := fh.NewXDPManager(fh.XDPConfig{
		Interface: *iface, ObjectPath: *object, Mode: fh.XDPMode(*mode), PinPath: *pin,
		ProtectedPorts: protected, RatePerSecond: *rate, Burst: *burst,
	})
	return manager.Attach()
}

func updateIPPolicy(block bool, args []string) error {
	fs := flag.NewFlagSet("ip-policy", flag.ExitOnError)
	pin := fs.String("pin", "", "bpffs pin directory")
	address := fs.String("ip", "", "IPv4 or IPv6 address")
	_ = fs.Parse(args)
	addr, err := netip.ParseAddr(strings.TrimSpace(*address))
	if err != nil {
		return fmt.Errorf("invalid IP address: %w", err)
	}
	manager := fh.NewXDPManager(fh.XDPConfig{PinPath: *pin})
	if block {
		return manager.BlockIP(addr)
	}
	return manager.UnblockIP(addr)
}

func updatePortPolicy(protect bool, args []string) error {
	fs := flag.NewFlagSet("port-policy", flag.ExitOnError)
	pin := fs.String("pin", "", "bpffs pin directory")
	port := fs.Uint("port", 0, "TCP destination port")
	_ = fs.Parse(args)
	if *port == 0 || *port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	manager := fh.NewXDPManager(fh.XDPConfig{PinPath: *pin})
	if protect {
		return manager.ProtectPort(uint16(*port))
	}
	return manager.UnprotectPort(uint16(*port))
}

func updateRatePolicy(args []string) error {
	fs := flag.NewFlagSet("set-rate", flag.ExitOnError)
	pin := fs.String("pin", "", "bpffs pin directory")
	rate := fs.Uint64("rate", 0, "packets per second per IPv4 source; zero disables")
	burst := fs.Uint64("burst", 0, "token bucket capacity")
	_ = fs.Parse(args)
	return fh.NewXDPManager(fh.XDPConfig{PinPath: *pin}).SetRateLimit(*rate, *burst)
}

func parsePorts(value string) ([]uint16, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	ports := make([]uint16, 0, len(parts))
	seen := make(map[uint16]struct{}, len(parts))
	for _, part := range parts {
		v, err := strconv.ParseUint(strings.TrimSpace(part), 10, 16)
		if err != nil || v == 0 {
			return nil, fmt.Errorf("invalid protected port %q", part)
		}
		port := uint16(v)
		if _, ok := seen[port]; ok {
			continue
		}
		seen[port] = struct{}{}
		ports = append(ports, port)
	}
	return ports, nil
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: fh-kernelctl <probe|build-xdp|attach-xdp|detach-xdp|block-ip|unblock-ip|protect-port|unprotect-port|set-rate> [flags]")
}
