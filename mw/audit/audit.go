package audit

import "github.com/oarkflow/fh"

type Config struct {
	Action     string
	Resource   string
	ResourceID func(fh.Ctx) string
	OnError    bool
	Next       func(fh.Ctx) bool
}

func New(cfg Config) fh.HandlerFunc {
	if cfg.Action == "" {
		cfg.Action = "http.request"
	}
	return func(c fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}
		err := c.Next()
		id := ""
		if cfg.ResourceID != nil {
			id = cfg.ResourceID(c)
		}
		result := "success"
		if err != nil {
			result = "error"
		}
		if err == nil || cfg.OnError {
			_ = c.Audit().Record(cfg.Action, cfg.Resource, id, fh.Map{"result": result, "status": c.StatusCode()})
		}
		return err
	}
}
