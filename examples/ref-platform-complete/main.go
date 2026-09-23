package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref/platform"
)

func main() {
	config := flag.String("config", "examples/ref-platform-complete/app.bcl", "path to the application document")
	addr := flag.String("addr", ":8090", "address to listen on")
	flag.Parse()

	app, err := platform.LoadFile(context.Background(), *config, platform.DefaultLoadOptions())
	if err != nil {
		log.Fatalf("webhook relay: compile: %v", err)
	}
	defer app.Close()

	server := fh.NewFast()
	if err := app.Mount(server); err != nil {
		log.Fatalf("webhook relay: mount: %v", err)
	}
	for _, route := range app.Document.Routes {
		log.Printf("webhook relay: %-6s %s -> %s", route.Method, route.Path, route.Intent)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		log.Print("webhook relay: shutting down")
		if err := server.ShutdownWithTimeout(10 * time.Second); err != nil {
			log.Printf("webhook relay: shutdown: %v", err)
		}
	}()

	log.Printf("webhook relay: listening on %s", *addr)
	if err := server.Listen(*addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("webhook relay: %v", err)
	}
}
