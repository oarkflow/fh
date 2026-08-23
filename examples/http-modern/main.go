package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/fh"
)

func main() {
	app := fh.New()

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

	log.Fatal(app.Listen(":8084"))
}
