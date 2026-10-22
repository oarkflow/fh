package main

import (
	"log"
	"os"
	"runtime/pprof"
	"strconv"

	"github.com/oarkflow/fh"
)

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

var users = benchmarkUsers()

func benchmarkUsers() []user {
	result := make([]user, 100)
	for i := range result {
		result[i] = user{ID: i + 1, Name: "User " + strconv.Itoa(i+1)}
	}
	return result
}

func main() {
	opts := []fh.Option{
		fh.WithDisableHTTP2(true),
		fh.WithDisablePanicRecovery(true),
		fh.WithSendDateHeader(false),
	}
	if raw := os.Getenv("FH_BENCH_WRITE_BUFFER_SIZE"); raw != "" {
		size, err := strconv.Atoi(raw)
		if err != nil || size <= 0 {
			log.Fatal("FH_BENCH_WRITE_BUFFER_SIZE must be a positive byte count")
		}
		opts = append(opts, fh.WithWriteBufferSize(size))
	}
	app := fh.NewFast(opts...)

	app.Get("/plaintext", func(c fh.Ctx) error { return c.SendString("Hello, World!") })
	app.Get("/json", func(c fh.Ctx) error {
		return c.JSON(fh.Map{"message": "Hello, World!"})
	})
	app.Get("/users/:id", func(c fh.Ctx) error {
		return c.JSON(user{Name: "User " + c.Params("id")})
	})
	app.Get("/search", func(c fh.Ctx) error {
		return c.JSON(fh.Map{"query": c.Query("q")})
	})
	app.Post("/echo", func(c fh.Ctx) error {
		var value map[string]any
		if err := c.BodyParser(&value); err != nil {
			return err
		}
		return c.JSON(value)
	})
	app.Get("/users", func(c fh.Ctx) error { return c.JSON(users) })

	if path := os.Getenv("FH_BENCH_CPU_PROFILE"); path != "" {
		file, err := os.Create(path)
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.StartCPUProfile(file); err != nil {
			log.Fatal(err)
		}
		err = app.ListenWithGracefulShutdown("127.0.0.1:3001")
		pprof.StopCPUProfile()
		_ = file.Close()
		if err != nil {
			log.Fatal(err)
		}
		return
	}
	log.Fatal(app.Listen("127.0.0.1:3001"))
}
