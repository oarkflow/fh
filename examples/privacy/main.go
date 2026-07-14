package main

import (
	"fmt"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/privacy"
)

func main() {
	app := fh.New()

	// Create a privacy filter with strict policies
	filter := privacy.New(privacy.Config{
		HeaderAllowlist: []string{
			"Content-Type", "Accept", "User-Agent",
			"X-Request-ID", "X-Correlation-ID",
		},
		QueryRedact:   []string{"token", "api_key", "secret"},
		PathTemplate:  true,
		SecretDetection: true,
		BodyLogging:    false,
		HashFields:     []string{"user_id", "email"},
		NeverExport:    []string{"password", "ssn", "credit_card"},
		TenantLogPolicies: map[string]*privacy.TenantPolicy{
			"enterprise": {
				HeaderAllowlist: []string{"Content-Type", "Accept"},
				BodyLogging:     true,
				SamplingRate:    1.0,
			},
			"free": {
				SamplingRate: 0.1,
				NeverExport:  []string{"email", "phone"},
			},
		},
	})

	app.Use(privacy.PrivacyMiddleware(filter))

	app.Get("/users/:id", func(c fh.Ctx) error {
		path := filter.TemplatePath(c.Path())
		// /users/123 becomes /users/:id in telemetry
		return c.JSON(fh.Map{
			"user_id": c.Param("id"),
			"path_template": path,
		})
	})

	app.Get("/search", func(c fh.Ctx) error {
		// Query params like ?token=secret123 are redacted
		query := filter.FilterQuery(c.Query("_"))
		return c.JSON(fh.Map{"filtered_query": query})
	})

	fmt.Println("Privacy filter example on :3000")
	fmt.Println("  GET /users/123      - path templated to /users/:id")
	fmt.Println("  GET /search?t=abc   - sensitive query values redacted")
	app.Listen(":3000")
}
