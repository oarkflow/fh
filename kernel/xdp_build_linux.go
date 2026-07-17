//go:build linux

package kernel

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// BuildXDP compiles the bundled XDP C program into an ELF BPF object. clang
// must include the BPF target. Production builds should run this during image
// construction rather than at application startup.
func BuildXDP(source, output string) error {
	if source == "" {
		source = "kernel/xdp/fh_xdp.c"
	}
	if output == "" {
		output = "kernel/xdp/fh_xdp.o"
	}
	arch, multiarch := bpfTargetArch(runtime.GOARCH)
	args := []string{"-O2", "-g", "-Wall", "-Werror", "-target", "bpf", "-D__TARGET_ARCH_" + arch}
	includeDir := filepath.Dir(source)
	if includeDir != "." {
		args = append(args, "-I"+includeDir)
	}
	if multiarch != "" {
		if _, err := os.Stat(multiarch); err == nil {
			args = append(args, "-I"+multiarch)
		}
	}
	if dir := filepath.Dir(output); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("fh: create XDP output directory: %w", err)
		}
	}
	args = append(args, "-c", source, "-o", output)
	out, err := exec.Command("clang", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("fh: compile XDP object: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func bpfTargetArch(goarch string) (target, multiarch string) {
	switch goarch {
	case "arm64":
		return "arm64", "/usr/include/aarch64-linux-gnu"
	case "riscv64":
		return "riscv", "/usr/include/riscv64-linux-gnu"
	case "s390x":
		return "s390", "/usr/include/s390x-linux-gnu"
	case "ppc64":
		return "powerpc", "/usr/include/powerpc64-linux-gnu"
	case "ppc64le":
		return "powerpc", "/usr/include/powerpc64le-linux-gnu"
	case "386":
		return "x86", "/usr/include/i386-linux-gnu"
	default:
		return "x86", "/usr/include/x86_64-linux-gnu"
	}
}
