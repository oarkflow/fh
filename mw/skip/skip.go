// Package skip conditionally bypasses middleware.
package skip

import "github.com/oarkflow/fh"

type Predicate func(*fh.Ctx) bool

// New wraps middleware and skips it when predicate returns true.
func New(middleware fh.HandlerFunc, predicate Predicate) fh.HandlerFunc {
	if middleware == nil {
		panic("skip: nil middleware")
	}
	if predicate == nil {
		return middleware
	}
	return func(c *fh.Ctx) error {
		if predicate(c) {
			return c.Next()
		}
		return middleware(c)
	}
}

// Paths skips middleware for exact request paths.
func Paths(paths ...string) Predicate {
	set := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		set[path] = struct{}{}
	}
	return func(c *fh.Ctx) bool { _, ok := set[c.Path()]; return ok }
}
