package main

import (
	"context"
	"log"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref/platform"
)

func main() {
	// app.bcl requires DATABASE_URL and a 32+ byte SESSION_SECRET. BCL resolves
	// them before any listener starts; TODO_NOTIFICATION_URL is optional in the
	// example and should point at the deployed mail/notification service.
	opts := platform.DefaultLoadOptions()
	p, err := platform.LoadFile(context.Background(), "examples/ref-platform-todo/app.bcl", opts)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	app := fh.NewFast()
	if err := p.Mount(app); err != nil {
		log.Fatal(err)
	}
	if err := app.Listen(":8089"); err != nil {
		log.Fatal(err)
	}
}
