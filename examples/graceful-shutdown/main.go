package main

import (
	"log"
	"time"

	"github.com/oarkflow/fh"
)

func main() {
	app := fh.New(
		fh.WithReadTimeout(5*time.Second),
		fh.WithWriteTimeout(10*time.Second),
		fh.WithIdleTimeout(60*time.Second),
		fh.WithShutdownTimeout(15*time.Second),
	)

	app.OnListen(func() error {
		log.Println("server started — send SIGINT (Ctrl+C) or SIGTERM to stop")
		return nil
	})

	app.OnShutdown(func() error {
		log.Println("closing database connections...")
		time.Sleep(500 * time.Millisecond) // simulate cleanup
		return nil
	})

	app.OnShutdown(func() error {
		log.Println("flushing metrics...")
		return nil
	})

	app.Get("/", func(c *fh.Ctx) error {
		return c.SendString("Hello, World!")
	})

	app.Get("/slow", func(c *fh.Ctx) error {
		time.Sleep(5 * time.Second) // simulate long request
		return c.SendString("slow response completed")
	})

	if err := app.ListenWithGracefulShutdown(":3000"); err != nil {
		log.Fatal(err)
	}
}
