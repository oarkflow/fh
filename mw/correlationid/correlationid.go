package correlationid

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/requestid"
)

type Config struct {
	Header        string
	LocalKey      string
	TrustIncoming bool
	Generator     requestid.Generator
	Validator     requestid.Validator
}

func New(config ...Config) fh.HandlerFunc {
	cfg := Config{Header: "X-Correlation-ID", LocalKey: "correlationID", TrustIncoming: true, Generator: requestid.NewAtomicGenerator(), Validator: requestid.DefaultValidator}
	if len(config) > 0 {
		o := config[0]
		if o.Header != "" {
			cfg.Header = o.Header
		}
		if o.LocalKey != "" {
			cfg.LocalKey = o.LocalKey
		}
		cfg.TrustIncoming = o.TrustIncoming
		if o.Generator != nil {
			cfg.Generator = o.Generator
		}
		if o.Validator != nil {
			cfg.Validator = o.Validator
		}
	}
	return func(c *fh.Ctx) error {
		id := ""
		if cfg.TrustIncoming {
			id = c.Get(cfg.Header)
			if id != "" && !cfg.Validator(id) {
				return fh.NewHTTPError(fh.StatusBadRequest, "CORRELATION_ID_INVALID", "correlation id is invalid")
			}
		}
		if id == "" {
			id = cfg.Generator.Generate(c)
		}
		c.Set(cfg.Header, id)
		c.Locals(cfg.LocalKey, id)
		return c.Next()
	}
}
