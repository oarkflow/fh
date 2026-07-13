package main

import (
	"log"
	"strconv"

	"github.com/gofiber/fiber/v3"
)

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

var users []User

func init() {
	users = make([]User, 100)
	for i := 0; i < 100; i++ {
		users[i] = User{ID: i + 1, Name: "User " + strconv.Itoa(i+1)}
	}
}

func main() {
	app := fiber.New(fiber.Config{
		DisableDefaultDate: true,
		ReadBufferSize:     16 << 10,
		BodyLimit:          4 << 20,
	})

	app.Get("/plaintext", func(c fiber.Ctx) error {
		return c.SendString("Hello, World!")
	})

	app.Get("/json", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{"message": "Hello, World!"})
	})

	app.Get("/users/:id", func(c fiber.Ctx) error {
		id := c.Params("id")
		return c.JSON(User{Name: "User " + id})
	})

	app.Get("/search", func(c fiber.Ctx) error {
		q := c.Query("q")
		return c.JSON(fiber.Map{"query": q})
	})

	app.Post("/echo", func(c fiber.Ctx) error {
		var body map[string]any
		if err := c.Bind().Body(&body); err != nil {
			return err
		}
		return c.JSON(body)
	})

	app.Get("/users", func(c fiber.Ctx) error {
		return c.JSON(users)
	})

	methodReply := func(c fiber.Ctx) error { return c.SendString("OK") }
	app.Get("/methods/get", methodReply)
	app.Head("/methods/head", methodReply)
	app.Post("/methods/post", methodReply)
	app.Put("/methods/put", methodReply)
	app.Patch("/methods/patch", methodReply)
	app.Delete("/methods/delete", methodReply)
	app.Options("/methods/options", methodReply)
	app.Add([]string{"CONNECT"}, "/methods/connect", methodReply)
	app.Add([]string{"TRACE"}, "/methods/trace", methodReply)
	// Fiber rejects extension methods such as QUERY at registration time. The
	// benchmark keeps the scenario so that unsupported methods are visible.

	log.Fatal(app.Listen(":3003"))
}
