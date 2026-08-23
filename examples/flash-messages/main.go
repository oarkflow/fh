package main

import (
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/pkg/storage/kv"
	"github.com/oarkflow/template"
)

func main() {
	app := newApp()

	fmt.Println("Flash messages example running on http://localhost:3000")
	if err := app.Listen(":3000"); err != nil {
		log.Fatal(err)
	}
}

func newApp() *fh.App {
	// SPL template engine (github.com/oarkflow/spl)
	engine := template.NewSPL("templates", ".html")
	engine.Config(template.SPLConfig{
		Directory: "templates",
		Extension: ".html",
		Reload:    true, // reload templates on every request (dev mode)
	})

	app := fh.New(
		fh.WithDebug(true),
		fh.WithEnvironment(fh.EnvDevelopment),
		fh.WithServerHeader("flash-demo"),
		fh.WithTemplateEngine(engine),
	)

	// Session middleware (required for flash messages)
	store := kv.NewMemoryStore(kv.WithShardCount(4), kv.WithMaxEntries(10000))
	sessions := session.NewSessionManager(store,
		session.SessionSecrets([]byte("demo-secret-must-be-32-bytes-ok!!")),
		session.SessionMaxAge(24*time.Hour),
		session.SessionSecure(false),
	)
	app.Use(session.New(sessions))

	// Routes
	app.Get("/", indexHandler)
	app.Post("/items", createItemHandler)
	app.Post("/items/delete", deleteItemHandler)
	app.Get("/items", listItemsHandler)

	return app
}

// indexHandler shows the form and any flash messages.
func indexHandler(c fh.Ctx) error {
	return c.Render("index.html", withFlashDefaults(map[string]any{
		"Title": "Flash Messages Demo",
	}))
}

// createItemHandler simulates creating an item, sets a flash message, and redirects.
func createItemHandler(c fh.Ctx) error {
	name := c.Query("name")
	if name == "" {
		name = "New Item"
	}

	// Set flash messages for the next request
	c.Flash("success", fmt.Sprintf("Item %q created successfully!", name))
	c.Flash("type", "success")

	return c.Redirect("/items", 302)
}

// deleteItemHandler simulates deleting an item using RedirectWithFlash.
func deleteItemHandler(c fh.Ctx) error {
	name := c.Query("name")
	if name == "" {
		name = "Item"
	}

	// RedirectWithFlash is a convenience: sets flash + redirects in one call
	return c.RedirectWithFlash("/items", 302, map[string]any{
		"success": fmt.Sprintf("Item %q deleted.", name),
		"type":    "warning",
	})
}

// listItemsHandler shows items page with flash messages auto-injected into template.
func listItemsHandler(c fh.Ctx) error {
	items := []string{"Laptop", "Phone", "Tablet", "Headphones"}

	// Flash data is automatically merged into the template data by c.Render().
	// No need to manually pass flash — just call Render normally.
	return c.Render("items.html", withFlashDefaults(map[string]any{
		"Title": "Items",
		"Items": items,
	}))
}

func withFlashDefaults(data map[string]any) map[string]any {
	if _, ok := data["success"]; !ok {
		data["success"] = false
	}
	if _, ok := data["type"]; !ok {
		data["type"] = "success"
	}
	return data
}
