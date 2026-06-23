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
	app := fh.New(fh.Config{DisableKeepAlive: false})

	app.Get("/plaintext", func(c *fh.Ctx) error {
		return c.SendString("Hello, World!")
	})

	app.Get("/json", func(c *fh.Ctx) error {
		return c.JSON(map[string]string{"message": "Hello, World!"})
	})

	app.Get("/users/:id", func(c *fh.Ctx) error {
		id := c.Params("id")
		return c.JSON(User{Name: "User " + id})
	})

	app.Get("/search", func(c *fh.Ctx) error {
		q := c.Query("q")
		return c.JSON(map[string]string{"query": q})
	})

	app.Post("/echo", func(c *fh.Ctx) error {
		var body map[string]any
		if err := c.BodyParser(&body); err != nil {
			return err
		}
		return c.JSON(body)
	})

	app.Get("/users", func(c *fh.Ctx) error {
		return c.JSON(users)
	})

	log.Fatal(app.Listen(":3001"))
}
