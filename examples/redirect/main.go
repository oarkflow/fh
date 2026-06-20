package main

import (
	"log"

	"github.com/oarkflow/fh"
)

func main() {
	app := fh.New()

	app.Get("/old", func(c *fh.Ctx) error {
		return c.Redirect("/new", 301)
	})

	app.Get("/new", func(c *fh.Ctx) error {
		return c.SendString("Welcome to the new page!")
	})

	app.Get("/redirect-me", func(c *fh.Ctx) error {
		return c.Redirect("https://example.com", fh.StatusTemporaryRedirect)
	})

	app.Get("/users/:id", func(c *fh.Ctx) error {
		return c.SendString("User ID: " + c.Param("id"))
	}).Name("users.show")

	app.Get("/go-to-user", func(c *fh.Ctx) error {
		return c.RedirectTo("users.show", map[string]string{"id": "123"}, 302)
	})

	app.Get("/back", func(c *fh.Ctx) error {
		return c.RedirectBack("/fallback")
	})

	app.Get("/fallback", func(c *fh.Ctx) error {
		return c.SendString("Fallback page")
	})

	log.Fatal(app.Listen(":3000"))
}
