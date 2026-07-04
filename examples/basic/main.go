package main

import "github.com/oarkflow/fh"

func main() {
	app := fh.New()
	app.Get("/", func(c fh.Ctx) error {
		return c.SendString("Hello, World!")
	})
	app.Listen(":3000")
}
