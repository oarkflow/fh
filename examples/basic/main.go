package main

import (
	"github.com/oarkflow/fh"
)

func main() {
	// NewFast disables production admission limits and per-request safeguards
	// that would otherwise skew a loopback throughput benchmark.
	app := fh.NewFast()

	app.Get("/", func(c fh.Ctx) error {
		return c.SendString("Hello, World!")
	})
	app.Listen(":8082")
}
