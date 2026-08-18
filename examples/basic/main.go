package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/static"
)

func main() {
	app := fh.New()

	app.Use(static.New("/", static.Config{
		Root:     "./public",
		Prefix:   "/",
		Browse:   true,
		MaxAge:   3600,
		Download: false, // Content-Disposition: attachment
	}))
	app.Listen(":8082")
}
