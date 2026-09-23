package platform

import (
	"fmt"
	"strings"
	"time"
)

// What BCL can and cannot express, and how this package works with it.
//
// BCL v0.0.31 has seven behaviours a spec has to be designed around. Every one of
// them fails *silently* — a field stays empty, a timeout becomes zero, a whole
// block vanishes — which is why they are written down here and asserted by
// bcl_test.go rather than rediscovered one incident at a time.
//
//  1. `when` and `const` are parser keywords. A field or config key with either
//     name does not merely fail to bind: it derails the parse and swallows the rest
//     of the document. This package therefore spells every guard `condition`.
//
//  2. `type`, `map`, `import` and `include` never bind to a struct field. A
//     `bcl:"type"` field silently stays empty. This package spells the node and
//     step family `family`, the edge and trigger discriminator `kind`, and the
//     edge's rename shorthand `extract`.
//
//  3. `schema` and `field` are reserved *block* names — BCL has its own schema
//     support, and a block by either name never reaches the document. Shapes are
//     declared with `shape "X" { }` and their fields with `prop "y" { }`.
//
//  4. A `time.Duration` field silently decodes to zero. Every duration in the spec
//     is therefore a Duration string, parsed here — which also buys a real error
//     message for `timeout "5 minutes"` instead of a timeout that quietly became
//     zero.
//
//  5. A bare identifier only binds to a field tagged `,ident`. Enum-ish fields
//     carry that tag so `kind effect` and `kind "effect"` both work.
//
//  6. Inside a block, put one key per line. Several keys on one line bind
//     unreliably — `from` and `to` in particular never bind inline, because BCL
//     reads `from X to Y` as a range expression. The examples are written one key
//     per line throughout, which is also how they read best.
//
//  7. audit is BCL schema metadata, not a route field. Route audit policy is
//     named route_audit so its nested settings reach the platform spec.
//
// reservedBCLNames is asserted by a test, so a future BCL release that changes any
// of this is caught here rather than in somebody's deployment.
var reservedBCLNames = []string{"when", "const", "type", "map", "import", "include", "schema", "field", "audit"}

// inlineUnsafeBCLNames are keys that bind only on a line of their own.
var inlineUnsafeBCLNames = []string{"from", "to"}

// Duration is a configured duration: a string in the document, a time.Duration
// once parsed.
//
// Keeping the parse explicit means an unparseable value fails the deployment with a
// message naming the field, which is strictly better than the zero a typed
// time.Duration field would have produced.
type Duration string

// Parse resolves the duration, returning the fallback when it is unset.
func (d Duration) Parse(what string, fallback time.Duration) (time.Duration, error) {
	text := strings.TrimSpace(string(d))
	if text == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(text)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a duration (try 30s, 5m, 48h): %w", what, text, err)
	}
	if parsed < 0 {
		return 0, fmt.Errorf("%s: %q is negative", what, text)
	}
	return parsed, nil
}

// MustParse is Parse for a value already validated at compile time.
func (d Duration) MustParse(fallback time.Duration) time.Duration {
	parsed, err := d.Parse("duration", fallback)
	if err != nil {
		return fallback
	}
	return parsed
}

// Set reports whether a duration was configured at all, which matters where zero
// and unset mean different things.
func (d Duration) Set() bool { return strings.TrimSpace(string(d)) != "" }

// durationField parses one duration field, prefixing errors with its location.
func durationField(what, field string, value Duration, fallback time.Duration) (time.Duration, error) {
	return value.Parse(what+" "+field, fallback)
}
