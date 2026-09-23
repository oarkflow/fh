package platform

import (
	"fmt"
	"net/mail"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/oarkflow/fh/ref/intent"
)

// Schema validation is the platform's cheapest safety net: it runs before any
// resource is touched and rejects a malformed payload while the invocation has
// still spent nothing. One schema block, declared once, is enforced identically
// at every boundary that names it — an intent's input, a node's data pipeline,
// a CRUD resource, a human task's completion form, a process run's start.
//
// A schema is compiled once at load time into a tree of field checks. There is
// no reflection and no re-parsing per request.

// CompiledSchema is one validated, ready-to-run schema.
type CompiledSchema struct {
	Name                 string
	Type                 string
	Description          string
	Fields               []*compiledField
	byName               map[string]*compiledField
	Required             []string
	AdditionalProperties bool
	// Sensitive is every field path marked sensitive, so audit and error
	// surfaces can redact them without walking the schema again.
	Sensitive []string
}

type compiledField struct {
	name      string
	typ       string
	required  bool
	items     string
	schema    string
	enum      map[string]struct{}
	enumOrder []string
	pattern   *regexp.Regexp
	min, max  *float64
	minLen    *int
	maxLen    *int
	format    string
	def       any
	sensitive bool
	// nested and elem are resolved after every schema is compiled, so schemas
	// may reference each other in any order.
	nested *CompiledSchema
	elem   *CompiledSchema
}

var schemaScalarTypes = map[string]struct{}{
	"string": {}, "int": {}, "integer": {}, "number": {}, "float": {},
	"bool": {}, "boolean": {}, "time": {}, "date": {}, "datetime": {},
	"any": {}, "object": {}, "array": {},
}

// compileSchemas compiles every schema block and then resolves cross-references
// between them, rejecting unknown targets and reference cycles. Doing it in two
// passes is what lets an author declare schemas in whatever order reads best.
func compileSchemas(specs []SchemaSpec) (map[string]*CompiledSchema, error) {
	compiled := make(map[string]*CompiledSchema, len(specs))
	for _, spec := range specs {
		name := strings.TrimSpace(spec.Name)
		if name == "" {
			return nil, fmt.Errorf("ref/platform: every schema needs a name")
		}
		if _, exists := compiled[name]; exists {
			return nil, fmt.Errorf("ref/platform: duplicate schema %q", name)
		}
		schema := &CompiledSchema{
			Name:                 name,
			Type:                 orDefault(spec.Kind, "object"),
			Description:          spec.Description,
			Required:             append([]string(nil), spec.Required...),
			AdditionalProperties: spec.AdditionalProperties == nil || *spec.AdditionalProperties,
			byName:               make(map[string]*compiledField, len(spec.Props)),
		}
		requiredByList := make(map[string]bool, len(spec.Required))
		for _, field := range spec.Required {
			requiredByList[field] = true
		}
		for _, fieldSpec := range spec.Props {
			field, err := compileField(name, fieldSpec)
			if err != nil {
				return nil, err
			}
			if requiredByList[field.name] {
				field.required = true
			}
			if _, exists := schema.byName[field.name]; exists {
				return nil, fmt.Errorf("ref/platform: schema %q declares field %q twice", name, field.name)
			}
			schema.Fields = append(schema.Fields, field)
			schema.byName[field.name] = field
			if field.sensitive {
				schema.Sensitive = append(schema.Sensitive, field.name)
			}
		}
		for requiredName := range requiredByList {
			if _, ok := schema.byName[requiredName]; !ok {
				return nil, fmt.Errorf("ref/platform: schema %q requires undeclared field %q", name, requiredName)
			}
		}
		compiled[name] = schema
	}

	for _, schema := range compiled {
		for _, field := range schema.Fields {
			if field.schema != "" {
				nested, ok := compiled[field.schema]
				if !ok {
					return nil, fmt.Errorf("ref/platform: schema %q field %q references unknown schema %q", schema.Name, field.name, field.schema)
				}
				field.nested = nested
			}
			if field.items != "" {
				if _, isScalar := schemaScalarTypes[field.items]; isScalar {
					continue
				}
				elem, ok := compiled[field.items]
				if !ok {
					return nil, fmt.Errorf("ref/platform: schema %q field %q references unknown item schema %q", schema.Name, field.name, field.items)
				}
				field.elem = elem
			}
		}
	}
	for name, schema := range compiled {
		if cycle := schemaCycle(schema, map[string]bool{}); cycle != "" {
			return nil, fmt.Errorf("ref/platform: schema %q participates in a reference cycle via %s", name, cycle)
		}
	}
	return compiled, nil
}

// schemaCycle walks required nested references only. An optional self-reference
// is legitimate (a comment with optional replies); a required one can never be
// satisfied by a finite document, so it is rejected.
func schemaCycle(schema *CompiledSchema, seen map[string]bool) string {
	if seen[schema.Name] {
		return schema.Name
	}
	seen[schema.Name] = true
	defer delete(seen, schema.Name)
	for _, field := range schema.Fields {
		if !field.required {
			continue
		}
		for _, next := range []*CompiledSchema{field.nested, field.elem} {
			if next == nil {
				continue
			}
			if cycle := schemaCycle(next, seen); cycle != "" {
				return schema.Name + " -> " + cycle
			}
		}
	}
	return ""
}

func compileField(schemaName string, spec SchemaFieldSpec) (*compiledField, error) {
	name := strings.TrimSpace(spec.Name)
	if name == "" {
		return nil, fmt.Errorf("ref/platform: schema %q has a field without a name", schemaName)
	}
	field := &compiledField{
		name:      name,
		typ:       strings.ToLower(orDefault(spec.Kind, "any")),
		required:  spec.Required,
		items:     strings.TrimSpace(spec.Items),
		schema:    strings.TrimSpace(spec.Schema),
		min:       spec.Min,
		max:       spec.Max,
		minLen:    spec.MinLength,
		maxLen:    spec.MaxLength,
		format:    strings.ToLower(strings.TrimSpace(spec.Format)),
		def:       spec.Default,
		sensitive: spec.Sensitive,
	}
	if field.schema != "" && field.typ == "any" {
		field.typ = "object"
	}
	if field.items != "" && field.typ == "any" {
		field.typ = "array"
	}
	if _, ok := schemaScalarTypes[field.typ]; !ok {
		// An unknown type name is treated as a reference to another schema,
		// which is what "type: Address" naturally means to an author.
		field.schema = field.typ
		field.typ = "object"
	}
	if len(spec.Enum) > 0 {
		field.enum = make(map[string]struct{}, len(spec.Enum))
		for _, value := range spec.Enum {
			field.enum[value] = struct{}{}
		}
		field.enumOrder = append([]string(nil), spec.Enum...)
	}
	if spec.Pattern != "" {
		pattern, err := regexp.Compile(spec.Pattern)
		if err != nil {
			return nil, fmt.Errorf("ref/platform: schema %q field %q pattern: %w", schemaName, name, err)
		}
		field.pattern = pattern
	}
	switch field.format {
	case "", "email", "uuid", "url", "date", "date_time", "datetime", "hostname", "ip":
	default:
		return nil, fmt.Errorf("ref/platform: schema %q field %q has unknown format %q", schemaName, name, field.format)
	}
	return field, nil
}

// Validate checks value against the schema, returning an invalid-input failure
// listing every problem rather than only the first. A caller fixing a payload
// wants the whole list, not one round trip per field.
//
// Validate also applies declared defaults, so a schema doubles as the place
// absent optional fields get their value. It returns the possibly-updated value
// rather than mutating in place, because the input map may be shared with
// facts that other nodes already read.
func (s *CompiledSchema) Validate(value any) (any, error) {
	if s == nil {
		return value, nil
	}
	var problems []string
	result := s.validateInto(value, "", &problems)
	if len(problems) > 0 {
		return nil, intent.Failure{
			Code:     "INVALID_INPUT",
			Category: intent.CategoryInvalidInput,
			Message:  fmt.Sprintf("%s: %s", s.Name, strings.Join(problems, "; ")),
		}
	}
	return result, nil
}

func (s *CompiledSchema) validateInto(value any, prefix string, problems *[]string) any {
	object, ok := value.(map[string]any)
	if !ok {
		if value == nil {
			object = map[string]any{}
		} else {
			*problems = append(*problems, fmt.Sprintf("%sexpected an object", prefix))
			return value
		}
	}
	out := make(map[string]any, len(object)+len(s.Fields))
	for key, item := range object {
		out[key] = item
	}
	for _, field := range s.Fields {
		path := prefix + field.name
		item, present := out[field.name]
		if (!present || item == nil) && field.def != nil {
			out[field.name] = field.def
			item, present = field.def, true
		}
		if !present || item == nil {
			if field.required {
				*problems = append(*problems, fmt.Sprintf("%s is required", path))
			}
			continue
		}
		out[field.name] = field.validate(item, path, problems)
	}
	if !s.AdditionalProperties {
		for key := range out {
			if _, declared := s.byName[key]; !declared {
				*problems = append(*problems, fmt.Sprintf("%s%s is not an allowed field", prefix, key))
			}
		}
	}
	return out
}

func (f *compiledField) validate(value any, path string, problems *[]string) any {
	switch f.typ {
	case "string":
		text, ok := value.(string)
		if !ok {
			*problems = append(*problems, fmt.Sprintf("%s must be a string", path))
			return value
		}
		f.validateString(text, path, problems)
		return text
	case "int", "integer":
		number, ok := ToFloat(value)
		if !ok {
			*problems = append(*problems, fmt.Sprintf("%s must be an integer", path))
			return value
		}
		if number != float64(int64(number)) {
			*problems = append(*problems, fmt.Sprintf("%s must be a whole number", path))
			return value
		}
		f.validateRange(number, path, problems)
		return int64(number)
	case "number", "float":
		number, ok := ToFloat(value)
		if !ok {
			*problems = append(*problems, fmt.Sprintf("%s must be a number", path))
			return value
		}
		f.validateRange(number, path, problems)
		return number
	case "bool", "boolean":
		switch typed := value.(type) {
		case bool:
			return typed
		case string:
			switch strings.ToLower(typed) {
			case "true", "1", "yes", "on":
				return true
			case "false", "0", "no", "off":
				return false
			}
		}
		*problems = append(*problems, fmt.Sprintf("%s must be true or false", path))
		return value
	case "time", "date", "datetime":
		switch typed := value.(type) {
		case time.Time:
			return typed
		case string:
			for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02"} {
				if parsed, err := time.Parse(layout, typed); err == nil {
					return parsed
				}
			}
		}
		*problems = append(*problems, fmt.Sprintf("%s must be a timestamp", path))
		return value
	case "array":
		items, ok := value.([]any)
		if !ok {
			*problems = append(*problems, fmt.Sprintf("%s must be an array", path))
			return value
		}
		if f.minLen != nil && len(items) < *f.minLen {
			*problems = append(*problems, fmt.Sprintf("%s needs at least %d items", path, *f.minLen))
		}
		if f.maxLen != nil && len(items) > *f.maxLen {
			*problems = append(*problems, fmt.Sprintf("%s allows at most %d items", path, *f.maxLen))
		}
		if f.elem == nil {
			return items
		}
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = f.elem.validateInto(item, fmt.Sprintf("%s[%d].", path, i), problems)
		}
		return out
	case "object":
		if f.nested != nil {
			return f.nested.validateInto(value, path+".", problems)
		}
		if _, ok := value.(map[string]any); !ok {
			*problems = append(*problems, fmt.Sprintf("%s must be an object", path))
		}
		return value
	default:
		return value
	}
}

func (f *compiledField) validateString(text, path string, problems *[]string) {
	if f.minLen != nil && len(text) < *f.minLen {
		*problems = append(*problems, fmt.Sprintf("%s must be at least %d characters", path, *f.minLen))
	}
	if f.maxLen != nil && len(text) > *f.maxLen {
		*problems = append(*problems, fmt.Sprintf("%s must be at most %d characters", path, *f.maxLen))
	}
	if f.enum != nil {
		if _, ok := f.enum[text]; !ok {
			*problems = append(*problems, fmt.Sprintf("%s must be one of %s", path, strings.Join(f.enumOrder, ", ")))
		}
	}
	if f.pattern != nil && !f.pattern.MatchString(text) {
		*problems = append(*problems, fmt.Sprintf("%s has an invalid format", path))
	}
	switch f.format {
	case "email":
		if _, err := mail.ParseAddress(text); err != nil {
			*problems = append(*problems, fmt.Sprintf("%s must be an email address", path))
		}
	case "uuid":
		if !uuidPattern.MatchString(text) {
			*problems = append(*problems, fmt.Sprintf("%s must be a UUID", path))
		}
	case "url":
		parsed, err := url.ParseRequestURI(text)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			*problems = append(*problems, fmt.Sprintf("%s must be an absolute URL", path))
		}
	case "date":
		if _, err := time.Parse("2006-01-02", text); err != nil {
			*problems = append(*problems, fmt.Sprintf("%s must be a date (YYYY-MM-DD)", path))
		}
	case "date_time", "datetime":
		if _, err := time.Parse(time.RFC3339, text); err != nil {
			*problems = append(*problems, fmt.Sprintf("%s must be an RFC3339 timestamp", path))
		}
	case "ip":
		if !ipPattern.MatchString(text) {
			*problems = append(*problems, fmt.Sprintf("%s must be an IP address", path))
		}
	case "hostname":
		if !hostnamePattern.MatchString(text) {
			*problems = append(*problems, fmt.Sprintf("%s must be a hostname", path))
		}
	}
}

func (f *compiledField) validateRange(number float64, path string, problems *[]string) {
	if f.min != nil && number < *f.min {
		*problems = append(*problems, fmt.Sprintf("%s must be at least %s", path, formatFloat(*f.min)))
	}
	if f.max != nil && number > *f.max {
		*problems = append(*problems, fmt.Sprintf("%s must be at most %s", path, formatFloat(*f.max)))
	}
}

var (
	uuidPattern     = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	ipPattern       = regexp.MustCompile(`^(\d{1,3}\.){3}\d{1,3}$|^[0-9a-fA-F:]+$`)
	hostnamePattern = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$`)
)

func orDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
