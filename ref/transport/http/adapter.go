package http

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/runtime"
)

// Adapter creates an fh.HandlerFunc that dispatches requests through the REF engine.
func Adapter(engine *runtime.Engine, intentName intent.Name) fh.HandlerFunc {
	return func(c fh.Ctx) error {
		inv := &invocation.Invocation{
			ID:     invocation.ID(c.Get("X-Request-ID")),
			Intent: invocation.IntentID(intentName),
			Input:  invocation.NewInput(c.Body(), c.Get("Content-Type")),
			Principal: invocation.PrincipalHint{
				BearerToken: extractBearer(c),
				APIKey:      extractAPIKey(c),
			},
			Metadata: invocation.NewHTTPMeta(
				c.Method(),
				c.Path(),
				c.Path(),
				c.Hostname(),
				c.GetReqHeaders(),
				nil,
				nil,
			),
			Transport: invocation.Transport{
				Protocol: "http",
				RemoteIP: c.IP(),
				TLS:      c.Protocol() == "https",
			},
		}

		result, err := engine.Dispatch(c.Context(), inv)
		if err != nil {
			return projectFailure(c, err)
		}

		if result.Meta.CacheControl != "" {
			c.Set("Cache-Control", result.Meta.CacheControl)
		}

		return c.JSON(result.Value)
	}
}

// projectFailure maps an intent.Failure or standard error to an HTTP response.
func projectFailure(c fh.Ctx, err error) error {
	if f, ok := err.(intent.Failure); ok {
		status := categoryToHTTPStatus(f.Category)
		return c.Status(status).JSON(map[string]any{
			"error": map[string]any{
				"code":    f.Code,
				"message": f.Message,
			},
		})
	}
	return c.Status(500).JSON(map[string]any{
		"error": map[string]any{
			"code":    "INTERNAL_ERROR",
			"message": err.Error(),
		},
	})
}

func categoryToHTTPStatus(cat intent.Category) int {
	switch cat {
	case intent.CategoryInvalidInput:
		return 422
	case intent.CategoryNotFound:
		return 404
	case intent.CategoryConflict:
		return 409
	case intent.CategoryPermission:
		return 403
	case intent.CategoryAuth:
		return 401
	case intent.CategoryRateLimit:
		return 429
	case intent.CategoryUnavailable:
		return 503
	case intent.CategoryTimeout:
		return 504
	default:
		return 500
	}
}

func extractBearer(c fh.Ctx) string {
	auth := c.Get("Authorization")
	if len(auth) > 7 && auth[:7] == "Bearer " {
		return auth[7:]
	}
	return ""
}

func extractAPIKey(c fh.Ctx) string {
	if k := c.Get("X-API-Key"); k != "" {
		return k
	}
	return c.Query("api_key")
}
