package tenant

import (
	"log/slog"

	"github.com/oarkflow/fh"
)

type Config struct {
	Header   string
	Required bool
	Next     func(fh.Ctx) bool

	// Validate, if set, is called with the resolved tenant ID (from the
	// authenticated Principal or, absent that, the raw request header) and
	// must return true to accept it. A resolved tenant is only ever
	// authenticated when it comes from the Principal (JWT claims, verified
	// mTLS subject, session store); when there is no Principal, it falls
	// back to trusting the client-supplied header as-is. Set Validate to
	// check the tenant against a known-tenant list/registry in deployments
	// without a trusted upstream that strips the header, to prevent
	// cross-tenant access via a spoofed header value.
	Validate func(fh.Ctx, string) bool
}

func New(cfg Config) fh.HandlerFunc {
	header := cfg.Header
	if header == "" {
		header = "X-Tenant-ID"
	}
	if cfg.Validate == nil {
		slog.Warn("fh/mw/tenant: no Validate configured — when a request has no authenticated Principal, the tenant ID falls back to the raw " + header + " header, which any caller can set to any value; set Validate to check it against a known-tenant registry unless a trusted upstream strips this header before it reaches fh")
	}
	extract := fh.TenantExtractor(fh.PrincipalTenantExtractor(), fh.HeaderString(header))
	return func(c fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}
		tenant, _, err := extract(c)
		if err != nil {
			return err
		}
		if tenant == "" {
			if cfg.Required {
				return fh.NewHTTPError(fh.StatusBadRequest, "TENANT_REQUIRED", "tenant is required")
			}
			return c.Next()
		}
		if cfg.Validate != nil && !cfg.Validate(c, tenant) {
			return fh.NewHTTPError(fh.StatusForbidden, "TENANT_REJECTED", "tenant is not recognized")
		}
		c.Locals("tenant_id", tenant)
		return c.Next()
	}
}
