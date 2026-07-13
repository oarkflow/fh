package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/fh"
)

// APIRequest demonstrates header parsing into a struct using `header` tags.
type APIRequest struct {
	ContentType string `header:"Content-Type"`
	Auth        string `header:"Authorization"`
	RequestID   string `header:"X-Request-Id"`
	Page        int    `header:"X-Page"`
}

func main() {
	app := fh.New()

	// Parse request headers into a struct.
	app.Get("/api", func(c fh.Ctx) error {
		var req APIRequest
		if err := c.HeaderParser(&req); err != nil {
			return c.Status(400).SendString("error: " + err.Error())
		}
		return c.JSON(fh.Map{
			"content_type": req.ContentType,
			"auth":         req.Auth,
			"request_id":   req.RequestID,
			"page":         req.Page,
		})
	})

	// Parse headers into a map[string]string.
	app.Get("/all", func(c fh.Ctx) error {
		m := make(map[string]string)
		if err := c.HeaderParser(&m); err != nil {
			return c.Status(400).SendString("error: " + err.Error())
		}
		return c.JSON(m)
	})

	// Standalone DecodeHeaders usage (no Ctx required).
	app.Get("/standalone", func(c fh.Ctx) error {
		h := c.RequestHeader()
		var req APIRequest
		if err := fh.DecodeHeaders(h, &req); err != nil {
			return c.Status(400).SendString("error: " + err.Error())
		}
		return c.SendString(fmt.Sprintf("page=%d", req.Page))
	})

	log.Fatal(app.Listen(":3000"))
}
