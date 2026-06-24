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

var (
	users     []User
	usersJSON []byte
)

func init() {
	users = make([]User, 100)
	usersJSON = append(usersJSON, '[')
	for i := 0; i < 100; i++ {
		users[i] = User{ID: i + 1, Name: "User " + strconv.Itoa(i+1)}
		if i > 0 {
			usersJSON = append(usersJSON, ',')
		}
		usersJSON = append(usersJSON, `{"id":`...)
		usersJSON = strconv.AppendInt(usersJSON, int64(users[i].ID), 10)
		usersJSON = append(usersJSON, `,"name":"User `...)
		usersJSON = strconv.AppendInt(usersJSON, int64(i+1), 10)
		usersJSON = append(usersJSON, `"}`...)
	}
	usersJSON = append(usersJSON, ']')
}

func main() {
	app := fh.New(
		fh.WithFastMode(true),
		fh.WithDisableHTTP2(true), // HTTP/1 benchmark mode; enable HTTP/2 for h2/h2c deployments.
	)

	app.Get("/plaintext", func(c *fh.Ctx) error {
		return c.SendString("Hello, World!")
	})

	app.Get("/json", func(c *fh.Ctx) error {
		return c.JSONString(`{"message":"Hello, World!"}`)
	})

	app.Get("/users/:id", func(c *fh.Ctx) error {
		id := c.Params("id")
		return c.JSONAppend(func(dst []byte) ([]byte, error) {
			dst = append(dst, `{"id":0,"name":"User `...)
			dst = append(dst, id...)
			dst = append(dst, `"}`...)
			return dst, nil
		})
	})

	app.Get("/search", func(c *fh.Ctx) error {
		q := c.Query("q")
		return c.JSONAppend(func(dst []byte) ([]byte, error) {
			dst = append(dst, `{"query":"`...)
			dst = append(dst, q...)
			dst = append(dst, `"}`...)
			return dst, nil
		})
	})

	app.Post("/echo", func(c *fh.Ctx) error {
		return c.EchoJSON()
	})

	app.Get("/users", func(c *fh.Ctx) error {
		return c.JSONBytes(usersJSON)
	})

	log.Fatal(app.Listen(":3001"))
}
