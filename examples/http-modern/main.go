package main

import (
	_ "embed"
	"fmt"
	"log"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/favicon"
	"github.com/oarkflow/fh/mw/redirect"
	"github.com/oarkflow/fh/mw/trailingslash"
)

//go:embed favicon.ico
var sampleFavicon []byte

func buildApp() *fh.App {
	app := fh.New()

	// 1. Favicon middleware: serves sample favicon.ico with ETag and immutable caching
	app.Use(favicon.New(favicon.Config{
		Data: sampleFavicon,
	}))

	// 2. Trailing slash normalization: removes trailing slashes with transparent rewrite
	app.Use(trailingslash.RemoveSlash(false))

	// 3. Redirect middleware: redirect legacy endpoints to canonical routes
	app.Use(redirect.New(redirect.Config{
		Rules: map[string]string{
			"/old-priority": "/priority",
		},
	}))

	app.Get("/", func(c fh.Ctx) error {
		return c.HTML(`<!doctype html>
<html>
<head><title>fh modern HTTP features</title></head>
<body>
<h1>Modern HTTP features</h1>
<ul>
<li><a href="/early-hints">Early Hints</a></li>
<li><a href="/resources">Resource hints</a></li>
<li><a href="/priority">RFC 9218 priority</a></li>
<li><a href="/old-priority">Legacy redirect (/old-priority -> /priority)</a></li>
<li><a href="/favicon.ico">Favicon (/favicon.ico)</a></li>
</ul>
</body>
</html>`)
	})

	app.Get("/early-hints", func(c fh.Ctx) error {
		c.SendInformational(102, map[string]string{"X-Progress": "preparing"})
		c.EarlyHintsWithHeaders("/static/site.css", map[string]string{
			"rel": "preload",
			"as":  "style",
		})
		c.EarlyHintsWithHeaders("/static/site.js", map[string]string{
			"rel": "preload",
			"as":  "script",
		})
		return c.SendString("The final response follows the 103 Early Hints response.")
	})

	app.Get("/resources", func(c fh.Ctx) error {
		if pusher, ok := c.(interface {
			PushResource(string, string, map[string]string) bool
		}); ok {
			pusher.PushResource("/static/site.css", "GET", map[string]string{
				"as": "style",
			})
			pusher.PushResource("/static/site.js", "GET", map[string]string{
				"as": "script",
			})
			pusher.PushResource("/static/site.woff2", "GET", map[string]string{
				"as": "font",
			})
		}
		return c.SendString("Resource hints use HTTP/2 push when available and 103 Early Hints on HTTP/1.1.")
	})

	app.Get("/priority", func(c fh.Ctx) error {
		requestPriority := c.RequestPriority()
		responsePriority := fh.HTTPPriority{
			Urgency:     requestPriority.Urgency,
			Incremental: requestPriority.Incremental,
		}
		c.SetResponsePriority(responsePriority)
		return c.JSON(map[string]any{
			"request_priority":  requestPriority,
			"response_priority": responsePriority,
			"message":           fmt.Sprintf("request urgency=%d incremental=%t", requestPriority.Urgency, requestPriority.Incremental),
		})
	})

	return app
}

func main() {
	app := buildApp()
	log.Fatal(app.Listen(":8084"))
}
