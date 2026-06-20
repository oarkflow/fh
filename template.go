package fh

import "io"

// TemplateEngine is the interface for template rendering engines.
// Implementations render named templates with data and optional layouts.
type TemplateEngine interface {
	Render(w io.Writer, name string, data any, layout ...string) error
}
