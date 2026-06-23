package main

import (
	"errors"
	"log"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/recover"
	"github.com/oarkflow/fh/mw/requestid"
)

func main() {
	app := fh.New(
		fh.WithEnvironment(fh.EnvDevelopment),
		fh.WithErrorOptions(fh.ErrorOptions{
			Environment:     fh.EnvDevelopment,
			ProblemTypeBase: "https://api.example.com/problems",
			ExposeCauses:    true,
		}),
	)

	app.Use(requestid.New())
	app.Use(recover.New())

	app.Get("/validation", func(c *fh.Ctx) error {
		return &fh.ValidationError{Fields: []fh.FieldError{
			{Field: "email", Code: "required", Message: "email is required"},
		}}
	})

	app.Get("/not-found", func(c *fh.Ctx) error {
		return fh.NotFound("User not found")
	})

	app.Get("/dependency", func(c *fh.Ctx) error {
		return fh.DependencyFailure("Payment provider is unavailable").WithCause(errors.New("upstream timeout token=super-secret"))
	})

	app.Get("/panic", func(c *fh.Ctx) error {
		panic("unexpected invariant failure password=secret")
	})

	log.Fatal(app.Listen(":3000"))
}
