package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/validate"
)

// ── Request types with validation tags ───────────────────────────────────

type CreateUserRequest struct {
	Name     string `json:"name" validate:"required,min=2,max=100"`
	Email    string `json:"email" validate:"required,email"`
	Password string `json:"password" validate:"required,min=8"`
	Confirm  string `json:"confirm" validate:"eqfield=Password"`
	Role     string `json:"role" validate:"oneof=admin user guest"`
	Age      int    `json:"age" validate:"min=0,max=150"`
}

// Validate implements fh.Validator for custom cross-field validation.
func (r *CreateUserRequest) Validate() error {
	if r.Role == "admin" && r.Age < 21 {
		return &fh.ValidationError{
			Fields: []fh.FieldError{
				{Field: "role", Code: "AGE_REQUIREMENT", Message: "admins must be at least 21 years old"},
			},
		}
	}
	return nil
}

type UpdateUserRequest struct {
	Name  string `json:"name" validate:"min=2,max=100"`
	Email string `json:"email" validate:"email"`
	Role  string `json:"role" validate:"oneof=admin user guest"`
}

type SearchUsersQuery struct {
	Q      string `query:"q" validate:"required,min=1"`
	Page   int    `query:"page" validate:"min=1"`
	Limit  int    `query:"limit" validate:"min=1,max=100"`
	Status string `query:"status" validate:"oneof=active inactive"`
}

type AuthHeaders struct {
	Authorization string `header:"Authorization" validate:"required"`
	XAPIKey       string `header:"X-API-Key"`
}

type UserParams struct {
	ID string `param:"id" validate:"required,numeric"`
}

// ── Response types ──────────────────────────────────────────────────────

type UserResponse struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
	Role  string `json:"role"`
}

type ListUsersResponse struct {
	Users []UserResponse `json:"users"`
	Total int            `json:"total"`
}

type ErrorResponse struct {
	Error   string            `json:"error"`
	Message string            `json:"message"`
	Errors  []fh.FieldError   `json:"errors,omitempty"`
}

// ── Handlers ────────────────────────────────────────────────────────────

var users = map[string]UserResponse{
	"1": {ID: "1", Name: "Alice", Email: "alice@example.com", Role: "admin"},
	"2": {ID: "2", Name: "Bob", Email: "bob@example.com", Role: "user"},
}

func createUser(c fh.Ctx, req CreateUserRequest) (UserResponse, error) {
	id := fmt.Sprintf("%d", len(users)+1)
	user := UserResponse{ID: id, Name: req.Name, Email: req.Email, Role: req.Role}
	users[id] = user
	return user, nil
}

func getUser(c fh.Ctx) error {
	id := c.Param("id")
	user, ok := users[id]
	if !ok {
		return c.Status(404).JSON(ErrorResponse{Error: "not_found", Message: "user not found"})
	}
	return c.JSON(user)
}

func listUsers(c fh.Ctx, query SearchUsersQuery) (ListUsersResponse, error) {
	var result []UserResponse
	for _, u := range users {
		if query.Q != "" && !strings.Contains(strings.ToLower(u.Name), strings.ToLower(query.Q)) {
			continue
		}
		if query.Status != "" {
			// simplified: all users are "active" in this demo
			if query.Status == "inactive" {
				continue
			}
		}
		result = append(result, u)
	}
	return ListUsersResponse{Users: result, Total: len(result)}, nil
}

func updateUser(c fh.Ctx, req UpdateUserRequest) (UserResponse, error) {
	id := c.Param("id")
	user, ok := users[id]
	if !ok {
		return UserResponse{}, fh.NotFound("user not found")
	}
	if req.Name != "" {
		user.Name = req.Name
	}
	if req.Email != "" {
		user.Email = req.Email
	}
	if req.Role != "" {
		user.Role = req.Role
	}
	users[id] = user
	return user, nil
}

func main() {
	app := fh.New()

	// ── Typed routes (validation is automatic) ──────────────────────────

	// POST /users — validates body automatically
	app.PostTyped("/users", createUser)

	// GET /users — validates query automatically
	app.GetTyped("/users", listUsers)

	// GET /users/:id — validates params automatically
	app.Get("/users/:id", getUser)

	// PUT /users/:id — validates body + params automatically
	app.PutTyped("/users/:id", updateUser)

	// ── Non-typed routes with middleware ────────────────────────────────

	// POST /validate/manual — explicit validation middleware
	app.Post("/validate/manual",
		validate.Body(func() any { return &CreateUserRequest{} }),
		func(c fh.Ctx) error {
			// By this point, the body has been decoded and validated.
			// If validation fails, the middleware returns 422 automatically.
			return c.JSON(map[string]string{"status": "validated manually"})
		},
	)

	// GET /validate/search — query validation middleware
	app.Get("/validate/search",
		validate.Query(&SearchUsersQuery{}),
		func(c fh.Ctx) error {
			return c.JSON(map[string]string{"status": "query validated"})
		},
	)

	// GET /validate/secure — header validation middleware
	app.Get("/validate/secure",
		validate.Headers(&AuthHeaders{}),
		func(c fh.Ctx) error {
			return c.JSON(map[string]string{"status": "headers validated"})
		},
	)

	// ── Custom error handler ───────────────────────────────────────────

	app.Post("/validate/custom",
		validate.Body(func() any { return &CreateUserRequest{} }, validate.Config{
			OnError: func(c fh.Ctx, ve *fh.ValidationError) error {
				return c.Status(422).JSON(map[string]any{
					"custom":  true,
					"message": "Validation failed",
					"fields":  ve.Fields,
				})
			},
		}),
		func(c fh.Ctx) error {
			return c.JSON(map[string]string{"status": "custom error handled"})
		},
	)

	// ── Skip validation conditionally ──────────────────────────────────

	app.Post("/validate/skip",
		validate.Body(func() any { return &CreateUserRequest{} }, validate.Config{
			Skip: func(c fh.Ctx) bool {
				return c.Query("skip") == "true"
			},
		}),
		func(c fh.Ctx) error {
			return c.JSON(map[string]string{"status": "validation was skipped"})
		},
	)

	log.Println("Validation example running on :8080")
	log.Println("")
	log.Println("Try these:")
	log.Println("  curl -X POST http://localhost:8080/users -H 'Content-Type: application/json' -d '{\"name\":\"Alice\",\"email\":\"alice@example.com\",\"password\":\"secret123\",\"confirm\":\"secret123\",\"role\":\"admin\",\"age\":25}'")
	log.Println("  curl -X POST http://localhost:8080/users -H 'Content-Type: application/json' -d '{\"name\":\"\",\"email\":\"bad\"}'  # 422 validation error")
	log.Println("  curl 'http://localhost:8080/users?q=ali&page=1&limit=10'")
	log.Println("  curl 'http://localhost:8080/validate/manual' -X POST -H 'Content-Type: application/json' -d '{\"name\":\"Bob\",\"email\":\"bob@test.com\",\"password\":\"pass1234\",\"confirm\":\"pass1234\",\"role\":\"user\",\"age\":30}'")
	log.Println("  curl 'http://localhost:8080/validate/secure' -H 'Authorization: Bearer token123'")
	log.Println("  curl 'http://localhost:8080/validate/custom' -X POST -H 'Content-Type: application/json' -d '{\"name\":\"\",\"email\":\"bad\"}'  # custom error format")
	log.Println("  curl 'http://localhost:8080/validate/skip?skip=true' -X POST -H 'Content-Type: application/json' -d '{}'  # skips validation")
	log.Println("")

	log.Fatal(app.Listen(":8080"))
}
