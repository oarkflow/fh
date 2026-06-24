package contract

import (
	"strings"

	"github.com/oarkflow/fh"
)

type Config struct {
	Methods        []string
	ContentTypes   []string
	MaxBodyBytes   int
	RequireHeaders []string
}

func New(cfg Config) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		if len(cfg.Methods) > 0 && !equal(cfg.Methods, c.Method()) {
			return fh.NewHTTPError(fh.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method is not allowed")
		}
		if cfg.MaxBodyBytes > 0 && len(c.Body()) > cfg.MaxBodyBytes {
			return fh.PayloadTooLarge("request body exceeds contract limit")
		}
		if len(cfg.ContentTypes) > 0 && !prefix(cfg.ContentTypes, c.Get(fh.HeaderContentType)) {
			return fh.UnsupportedMediaType("content type is not supported")
		}
		for _, h := range cfg.RequireHeaders {
			if c.Get(h) == "" {
				return fh.NewHTTPError(fh.StatusBadRequest, "HEADER_REQUIRED", "required header is missing: "+h)
			}
		}
		return c.Next()
	}
}
func equal(vs []string, s string) bool {
	for _, v := range vs {
		if strings.EqualFold(v, s) {
			return true
		}
	}
	return false
}
func prefix(vs []string, s string) bool {
	for _, v := range vs {
		if strings.HasPrefix(strings.ToLower(s), strings.ToLower(v)) {
			return true
		}
	}
	return false
}
