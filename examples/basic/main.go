package main

import (
	"log"

	fiber "github.com/oarkflow/fasthttp"
)

func main() {
    // Fiber instance
    app := fiber.New()

    // Routes
    app.Get("/", hello)

    // Start server
    log.Fatal(app.Listen(":3000"))
}

// Handler
func hello(c *fiber.Ctx) error {
    return c.SendString("Hello, World 👋!")
}
