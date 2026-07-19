// Command prefork demonstrates fh's OS-process prefork supervisor and
// doubles as the target binary for prefork_integration_test.go, which drives
// it with real SIGHUP/SIGTERM signals to prove zero-downtime rolling
// restarts. Configuration is via environment variables (rather than flags)
// so the test can control it precisely without argument-parsing overhead:
//
//	PREFORK_ADDR    listen address, default ":8091"
//	PREFORK_WORKERS worker process count, default 2
package main

import (
	"log"
	"os"
	"strconv"

	"github.com/oarkflow/fh"
)

func main() {
	addr := os.Getenv("PREFORK_ADDR")
	if addr == "" {
		addr = ":8091"
	}
	workers := 2
	if v := os.Getenv("PREFORK_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			workers = n
		}
	}

	app := fh.New()
	app.Get("/health", func(c fh.Ctx) error {
		return c.JSON(fh.Map{
			"pid":   os.Getpid(),
			"index": os.Getenv("FH_PREFORK_INDEX"),
			"gen":   os.Getenv("FH_PREFORK_GEN"),
		})
	})

	if err := app.ListenPrefork(addr, fh.WithPreforkWorkers(workers)); err != nil {
		log.Fatal(err)
	}
}
