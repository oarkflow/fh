package main

import (
	"log"
	"os"
	"strconv"

	"github.com/gofiber/fiber/v3"
	"github.com/oarkflow/fh/benchmarks/servers/go/benchstats"
)

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

var users = benchmarkUsers()

func benchmarkUsers() []user {
	result := make([]user, 100)
	for i := range result {
		result[i] = user{ID: i + 1, Name: "User " + strconv.Itoa(i+1)}
	}
	return result
}

func main() {
	addr := os.Getenv("FH_BENCH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:3003"
	}
	app := fiber.New(fiber.Config{
		DisableDefaultDate: true,
		ReadBufferSize:     16 << 10,
		BodyLimit:          4 << 20,
	})

	app.Get("/plaintext", func(c fiber.Ctx) error { return c.SendString("Hello, World!") })
	app.Get("/json", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{"message": "Hello, World!"})
	})
	app.Get("/users/:id", func(c fiber.Ctx) error {
		return c.JSON(user{Name: "User " + c.Params("id")})
	})
	app.Get("/search", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{"query": c.Query("q")})
	})
	app.Post("/echo", func(c fiber.Ctx) error {
		var value map[string]any
		if err := c.Bind().Body(&value); err != nil {
			return err
		}
		return c.JSON(value)
	})
	app.Get("/users", func(c fiber.Ctx) error { return c.JSON(users) })
	app.Get("/__bench/stats", func(c fiber.Ctx) error { return c.JSON(benchstats.Read()) })

	log.Fatal(app.Listen(addr))
}
