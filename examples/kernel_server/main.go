package main

import (
	"log"
	"runtime"
	"time"

	"github.com/oarkflow/fh"
)

func main() {
	kernel := fh.DefaultKernelConfig()
	kernel.Enabled = true
	kernel.Backend = fh.KernelBackendAuto
	kernel.Reactors = runtime.GOMAXPROCS(0)
	kernel.ReusePort = kernel.Reactors > 1
	kernel.ReusePortBPF = kernel.ReusePort
	kernel.PinThreads = true
	kernel.TCPUserTimeout = 30 * time.Second

	// XDP is deliberately disabled in the runnable example because attaching it
	// changes a host network interface and requires Linux capabilities. Enable it
	// only after building kernel/xdp/fh_xdp.o and selecting the correct interface.
	// kernel.XDP = fh.XDPConfig{
	//     Enabled: true, AutoAttach: true, Interface: "eth0",
	//     ObjectPath: "kernel/xdp/fh_xdp.o", RatePerSecond: 100_000, Burst: 200_000,
	// }

	app := fh.NewProduction(fh.WithKernel(kernel))
	app.Get("/", func(c fh.Ctx) error {
		return c.JSON(fh.Map{
			"server":    "fh",
			"transport": app.KernelRuntimeInfo().Backend,
			"message":   "kernel-assisted HTTP server",
		})
	})
	app.Get("/_kernel", func(c fh.Ctx) error {
		return c.JSON(app.KernelRuntimeInfo())
	})

	log.Fatal(app.ListenWithGracefulShutdown(":8080"))
}
