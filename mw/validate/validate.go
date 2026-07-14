// Package validate provides request validation middleware for fh.
//
// The validation engine (struct tags, rules, Validator interface) lives in the
// core fh package. This package provides middleware that wires it into the
// request lifecycle.
//
// Usage with typed routes (automatic — no middleware needed):
//
//	type CreateUserRequest struct {
//	    Name  string `json:"name" validate:"required,min=2"`
//	    Email string `json:"email" validate:"required,email"`
//	}
//
//	app.PostTyped("/users", func(c fh.Ctx, req CreateUserRequest) (UserResponse, error) {
//	    // Validation already ran before this handler
//	    return createUser(req), nil
//	})
//
// Usage with middleware (for non-typed routes):
//
//	app.Post("/users", validate.Body(&CreateUserRequest{}), handler)
//	app.Get("/search", validate.Query(&SearchQuery{}), handler)
//	app.Get("/data", validate.Headers(&AuthHeaders{}), handler)
package validate

import (
	"github.com/oarkflow/fh"
)

// Config controls validation middleware behavior.
type Config struct {
	// Skip returns true to skip validation for this request.
	Skip func(fh.Ctx) bool

	// OnError is called when validation fails. If nil, returns 422 with problem details.
	OnError func(fh.Ctx, *fh.ValidationError) error
}

// Body returns a middleware that decodes the request body into a new instance
// of the factory's return type, then validates it using struct tags and the
// Validator interface.
//
//	type CreateUserRequest struct {
//	    Name  string `json:"name" validate:"required,min=2"`
//	    Email string `json:"email" validate:"required,email"`
//	}
//
//	app.Post("/users", validate.Body(func() any {
//	    return &CreateUserRequest{}
//	}), handler)
func Body(factory func() any, cfg ...Config) fh.HandlerFunc {
	c := getConfig(cfg)
	return func(ctx fh.Ctx) error {
		if c.Skip != nil && c.Skip(ctx) {
			return ctx.Next()
		}
		v := factory()
		if v == nil {
			return ctx.Next()
		}
		if len(ctx.Body()) > 0 {
			if err := ctx.BodyParser(v); err != nil {
				return fh.BadRequest("Invalid request body")
			}
		}
		if err := fh.ValidateStruct(v); err != nil {
			return handleError(ctx, err, c)
		}
		if validator, ok := v.(fh.Validator); ok {
			if err := validator.Validate(); err != nil {
				return handleError(ctx, err, c)
			}
		}
		return ctx.Next()
	}
}

// Query returns a middleware that decodes query parameters into v and validates.
//
//	type SearchQuery struct {
//	    Q    string `query:"q" validate:"required,min=1"`
//	    Page int    `query:"page" validate:"min=1"`
//	}
//
//	app.Get("/search", validate.Query(&SearchQuery{}), handler)
func Query(v any, cfg ...Config) fh.HandlerFunc {
	c := getConfig(cfg)
	return func(ctx fh.Ctx) error {
		if c.Skip != nil && c.Skip(ctx) {
			return ctx.Next()
		}
		if err := ctx.QueryParser(v); err != nil {
			return fh.BadRequest("Invalid query parameters")
		}
		if err := fh.ValidateStruct(v); err != nil {
			return handleError(ctx, err, c)
		}
		if validator, ok := v.(fh.Validator); ok {
			if err := validator.Validate(); err != nil {
				return handleError(ctx, err, c)
			}
		}
		return ctx.Next()
	}
}

// Headers returns a middleware that decodes request headers into v and validates.
//
//	type AuthHeaders struct {
//	    Authorization string `header:"Authorization" validate:"required"`
//	}
//
//	app.Get("/data", validate.Headers(&AuthHeaders{}), handler)
func Headers(v any, cfg ...Config) fh.HandlerFunc {
	c := getConfig(cfg)
	return func(ctx fh.Ctx) error {
		if c.Skip != nil && c.Skip(ctx) {
			return ctx.Next()
		}
		if err := ctx.HeaderParser(v); err != nil {
			return fh.BadRequest("Invalid request headers")
		}
		if err := fh.ValidateStruct(v); err != nil {
			return handleError(ctx, err, c)
		}
		if validator, ok := v.(fh.Validator); ok {
			if err := validator.Validate(); err != nil {
				return handleError(ctx, err, c)
			}
		}
		return ctx.Next()
	}
}

func getConfig(cfg []Config) Config {
	if len(cfg) > 0 {
		return cfg[0]
	}
	return Config{}
}

func handleError(ctx fh.Ctx, err error, cfg Config) error {
	if cfg.OnError != nil {
		if ve, ok := err.(*fh.ValidationError); ok {
			return cfg.OnError(ctx, ve)
		}
		return cfg.OnError(ctx, &fh.ValidationError{Fields: []fh.FieldError{{Field: "_", Code: "VALIDATION_ERROR", Message: err.Error()}}})
	}
	if ve, ok := err.(*fh.ValidationError); ok {
		return ctx.Status(422).JSON(map[string]any{
			"error":      "validation_failed",
			"message":    "Validation failed",
			"request_id": ctx.Locals("request_id"),
			"errors":     ve.Fields,
		})
	}
	return ctx.Status(400).JSON(map[string]any{
		"error":   "bad_request",
		"message": err.Error(),
	})
}
