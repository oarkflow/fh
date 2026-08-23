// Package fetchmetadata implements W3C Fetch Metadata Request Headers security policies.
// It provides automated defense against Cross-Site Request Forgery (CSRF),
// Cross-Site Script Inclusion (XSSI), and XS-Leaks at the transport layer.
package fetchmetadata

import (
	"strings"

	"github.com/oarkflow/fh"
)

// Config defines the configuration for Fetch Metadata isolation policy.
type Config struct {
	// Filter defines a predicate to skip this middleware.
	Filter func(c fh.Ctx) bool

	// AllowTopLevelNavigations allows cross-site GET/HEAD navigations (e.g. clicking a link to your site).
	// Defaults to true.
	AllowTopLevelNavigations bool

	// AllowedDestinations allows cross-site requests with specific Sec-Fetch-Dest values (e.g. "image").
	AllowedDestinations []string

	// ExemptPaths contains exact paths or prefixes to exempt from Fetch Metadata enforcement (e.g. public webhooks).
	ExemptPaths []string

	// OnDenied is called when a request violates the fetch metadata policy.
	// Defaults to returning 403 Forbidden with RFC 9457 Problem Details.
	OnDenied fh.HandlerFunc
}

// DefaultConfig provides a secure default resource isolation policy.
var DefaultConfig = Config{
	AllowTopLevelNavigations: true,
	OnDenied: func(c fh.Ctx) error {
		return c.Status(fh.StatusForbidden).ProblemDetails(
			fh.StatusForbidden,
			"CROSS_SITE_REQUEST_BLOCKED",
			"Request blocked by Fetch Metadata Resource Isolation Policy",
			"https://www.w3.org/TR/fetch-metadata/",
		)
	},
}

// New creates a Fetch Metadata resource isolation middleware.
func New(config ...Config) fh.HandlerFunc {
	cfg := DefaultConfig
	if len(config) > 0 {
		if config[0].Filter != nil {
			cfg.Filter = config[0].Filter
		}
		cfg.AllowTopLevelNavigations = config[0].AllowTopLevelNavigations
		cfg.AllowedDestinations = config[0].AllowedDestinations
		cfg.ExemptPaths = config[0].ExemptPaths
		if config[0].OnDenied != nil {
			cfg.OnDenied = config[0].OnDenied
		}
	}

	allowedDestMap := make(map[string]struct{}, len(cfg.AllowedDestinations))
	for _, d := range cfg.AllowedDestinations {
		allowedDestMap[strings.ToLower(strings.TrimSpace(d))] = struct{}{}
	}

	return func(c fh.Ctx) error {
		if cfg.Filter != nil && cfg.Filter(c) {
			return c.Next()
		}

		path := c.Path()
		for _, exempt := range cfg.ExemptPaths {
			if path == exempt || strings.HasPrefix(path, exempt) {
				return c.Next()
			}
		}

		secFetchSite := strings.ToLower(c.Get("Sec-Fetch-Site"))
		// 1. Browsers that do not support Fetch Metadata are allowed through
		if secFetchSite == "" {
			return c.Next()
		}

		// 2. Allow same-origin, same-site, and direct user navigations (bookmarks, typed URLs)
		if secFetchSite == "same-origin" || secFetchSite == "same-site" || secFetchSite == "none" {
			return c.Next()
		}

		// 3. Request is cross-site: check if it qualifies as an allowed top-level navigation
		secFetchMode := strings.ToLower(c.Get("Sec-Fetch-Mode"))
		secFetchDest := strings.ToLower(c.Get("Sec-Fetch-Dest"))
		method := c.Method()

		if cfg.AllowTopLevelNavigations {
			isTopLevel := (secFetchMode == "navigate" || secFetchMode == "nested-navigate") &&
				(method == fh.MethodGET || method == fh.MethodHEAD) &&
				secFetchDest != "iframe"

			if isTopLevel {
				return c.Next()
			}
		}

		// 4. Check explicitly allowed destinations
		if len(allowedDestMap) > 0 {
			if _, ok := allowedDestMap[secFetchDest]; ok {
				return c.Next()
			}
		}

		// 5. Violates resource isolation policy -> reject
		c.Vary("Sec-Fetch-Site", "Sec-Fetch-Mode", "Sec-Fetch-Dest")
		return cfg.OnDenied(c)
	}
}
