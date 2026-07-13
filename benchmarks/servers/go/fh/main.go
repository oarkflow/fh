package main

import (
	"log"
	"strconv"

	"github.com/oarkflow/fh"
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
	app := fh.NewFast(
		fh.WithDisableHTTP2(true),
		fh.WithDisablePanicRecovery(true),
	)

	app.Get("/plaintext", func(c fh.Ctx) error {
		return c.SendString("Hello, World!")
	})
	app.Get("/json", func(c fh.Ctx) error {
		return c.JSON(fh.Map{"message": "Hello, World!"})
	})

	app.Get("/users/:id", func(c fh.Ctx) error {
		return c.JSON(User{Name: "User " + c.Params("id")})
	})

	app.Get("/search", func(c fh.Ctx) error {
		return c.JSON(fh.Map{"query": c.Query("q")})
	})

	app.Post("/echo", func(c fh.Ctx) error {
		var body map[string]any
		if err := c.BodyParser(&body); err != nil {
			return err
		}
		return c.JSON(body)
	})

	app.Get("/users", func(c fh.Ctx) error {
		return c.JSON(users)
	})

	methodReply := func(c fh.Ctx) error { return c.SendString("OK") }
	app.Get("/methods/get", methodReply)
	app.Head("/methods/head", methodReply)
	app.Post("/methods/post", methodReply)
	app.Put("/methods/put", methodReply)
	app.Patch("/methods/patch", methodReply)
	app.Delete("/methods/delete", methodReply)
	app.Options("/methods/options", methodReply)
	app.Connect("/methods/connect", methodReply)
	app.Trace("/methods/trace", methodReply)
	app.Query("/methods/query", methodReply)

	log.Fatal(app.Listen(":3001"))
}
