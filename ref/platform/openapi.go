package platform

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// OpenAPI builds an OpenAPI 3.1 document from the same route and shape
// declarations used by the runtime. The returned value can be JSON encoded.
func (p *Platform) OpenAPI() map[string]any {
	paths := map[string]any{}
	components := map[string]any{"schemas": map[string]any{}, "securitySchemes": map[string]any{
		"bearerAuth": map[string]any{"type": "http", "scheme": "bearer"},
		"apiKeyAuth": map[string]any{"type": "apiKey", "in": "header", "name": "X-API-Key"},
	}}
	schemaDefs := components["schemas"].(map[string]any)
	for _, shape := range p.Document.Shapes {
		schemaDefs[shape.Name] = openAPISchema(shape)
	}
	intents := make(map[string]IntentSpec, len(p.Document.Intents))
	for _, it := range p.Document.Intents {
		intents[it.Name] = it
	}
	for _, route := range p.Document.Routes {
		path := openAPIPath(route.Path)
		item, _ := paths[path].(map[string]any)
		if item == nil {
			item = map[string]any{}
			paths[path] = item
		}
		method := strings.ToLower(route.Method)
		if method == "" {
			method = "get"
		}
		op := map[string]any{"operationId": route.Name, "responses": map[string]any{}}
		if route.Description != "" {
			op["description"] = route.Description
		}
		if len(route.Tags) > 0 {
			op["tags"] = route.Tags
		}
		params := make([]any, 0, len(route.Parameters)+len(pathParams(route.Path)))
		declared := map[string]bool{}
		for _, param := range route.Parameters {
			where := strings.ToLower(param.In)
			declared[where+":"+param.Name] = true
			ps := map[string]any{"name": param.Name, "in": where, "required": param.Required, "schema": parameterSchema(param)}
			if param.Description != "" {
				ps["description"] = param.Description
			}
			params = append(params, ps)
		}
		for _, name := range pathParams(route.Path) {
			if !declared["path:"+name] {
				params = append(params, map[string]any{"name": name, "in": "path", "required": true, "schema": map[string]any{"type": "string"}})
			}
		}
		if len(params) > 0 {
			op["parameters"] = params
		}
		if route.Auth != "" {
			op["security"] = []any{map[string]any{"bearerAuth": []string{}}, map[string]any{"apiKeyAuth": []string{}}}
		}
		responses := op["responses"].(map[string]any)
		status := route.Status
		if status == 0 {
			status = 200
		}
		if strings.EqualFold(route.Mode, "async") && status == 200 {
			status = 202
		}
		response := map[string]any{"description": "Successful response"}
		if it, ok := intents[route.Intent]; ok && it.OutputSchema != "" {
			response["content"] = map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/" + it.OutputSchema}}}
		}
		responses[fmt.Sprint(status)] = response
		for code, description := range map[string]string{"400": "Invalid request", "401": "Unauthenticated", "403": "Forbidden", "404": "Not found", "409": "Conflict", "422": "Validation failed", "429": "Rate limited", "500": "Internal error", "503": "Unavailable"} {
			responses[code] = map[string]any{"description": description}
		}
		if it, ok := intents[route.Intent]; ok && it.InputSchema != "" && method != "get" && method != "head" {
			op["requestBody"] = map[string]any{"required": true, "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/" + it.InputSchema}}}}
		}
		if route.MaxBodyBytes > 0 {
			op["x-max-body-bytes"] = route.MaxBodyBytes
		}
		item[method] = op
	}
	return map[string]any{"openapi": "3.1.0", "info": map[string]any{"title": orDefault(p.Document.Name, "REF API"), "version": orDefault(p.Document.Version, "0.1.0")}, "paths": paths, "components": components}
}

// WriteOpenAPI writes the generated API contract as stable, indented JSON.
func (p *Platform) WriteOpenAPI(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(p.OpenAPI())
}

func openAPIPath(path string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if strings.HasPrefix(part, ":") {
			parts[i] = "{" + strings.TrimPrefix(part, ":") + "}"
		}
	}
	return strings.Join(parts, "/")
}
func parameterSchema(p HTTPParameterSpec) map[string]any {
	kind := p.Kind
	if kind == "" {
		kind = "string"
	}
	typ := openAPIType(kind)
	schema := map[string]any{"type": typ}
	if p.Format != "" {
		schema["format"] = p.Format
	}
	if len(p.Enum) > 0 {
		schema["enum"] = p.Enum
	}
	return schema
}
func openAPISchema(s SchemaSpec) map[string]any {
	typ := openAPIType(s.Kind)
	out := map[string]any{"type": typ}
	if s.Description != "" {
		out["description"] = s.Description
	}
	if typ != "object" {
		return out
	}
	props := map[string]any{}
	required := append([]string(nil), s.Required...)
	for _, f := range s.Props {
		props[f.Name] = openAPIField(f)
		if f.Required && !containsString(required, f.Name) {
			required = append(required, f.Name)
		}
	}
	out["properties"] = props
	if len(required) > 0 {
		out["required"] = required
	}
	if s.AdditionalProperties != nil {
		out["additionalProperties"] = *s.AdditionalProperties
	}
	return out
}
func openAPIField(f SchemaFieldSpec) map[string]any {
	kind := strings.ToLower(f.Kind)
	if kind == "" {
		if f.Schema != "" {
			kind = "object"
		} else if f.Items != "" {
			kind = "array"
		} else {
			kind = "any"
		}
	}
	if !isOpenAPIPrimitive(kind) {
		return map[string]any{"$ref": "#/components/schemas/" + f.Kind}
	}
	out := map[string]any{"type": openAPIType(kind)}
	if f.Description != "" {
		out["description"] = f.Description
	}
	if f.Format != "" {
		out["format"] = f.Format
	}
	if len(f.Enum) > 0 {
		out["enum"] = f.Enum
	}
	if f.Pattern != "" {
		out["pattern"] = f.Pattern
	}
	if f.Min != nil {
		out["minimum"] = *f.Min
	}
	if f.Max != nil {
		out["maximum"] = *f.Max
	}
	if f.MinLength != nil {
		out["minLength"] = *f.MinLength
	}
	if f.MaxLength != nil {
		out["maxLength"] = *f.MaxLength
	}
	if f.Default != nil {
		out["default"] = f.Default
	}
	if f.Required {
		out["x-required"] = true
	}
	if f.Sensitive {
		out["writeOnly"] = true
	}
	if f.Schema != "" {
		out = map[string]any{"$ref": "#/components/schemas/" + f.Schema}
	}
	if kind == "array" {
		item := strings.ToLower(f.Items)
		isPrim := isOpenAPIPrimitive(item)
		var schema map[string]any
		if isPrim {
			schema = map[string]any{"type": openAPIType(item)}
		} else if item != "" {
			schema = map[string]any{"$ref": "#/components/schemas/" + f.Items}
		} else {
			schema = map[string]any{}
		}
		out["items"] = schema
	}
	return out
}
func isOpenAPIPrimitive(t string) bool {
	switch strings.ToLower(t) {
	case "", "any", "string", "int", "integer", "number", "float", "bool", "boolean", "time", "date", "datetime", "object", "array":
		return true
	}
	return false
}
func openAPIType(t string) string {
	switch strings.ToLower(t) {
	case "", "any", "object":
		return "object"
	case "int", "integer":
		return "integer"
	case "float", "number":
		return "number"
	case "bool", "boolean":
		return "boolean"
	case "array":
		return "array"
	default:
		return "string"
	}
}
func containsString(items []string, target string) bool {
	for _, s := range items {
		if s == target {
			return true
		}
	}
	return false
}

func validateHTTPParameters(what string, route RouteSpec) error {
	method := strings.ToUpper(strings.TrimSpace(route.Method))
	switch method {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD", "TRACE":
	default:
		return fmt.Errorf("ref/platform: %s: unsupported HTTP method %q", what, route.Method)
	}
	if !strings.HasPrefix(route.Path, "/") {
		return fmt.Errorf("ref/platform: %s: route path must begin with /", what)
	}
	seen := map[string]bool{}
	pathNames := map[string]bool{}
	for _, name := range pathParams(route.Path) {
		pathNames[name] = true
	}
	for _, p := range route.Parameters {
		where := strings.ToLower(strings.TrimSpace(p.In))
		if p.Name == "" {
			return fmt.Errorf("ref/platform: %s: HTTP parameter needs a name", what)
		}
		if where != "path" && where != "query" && where != "header" && where != "cookie" {
			return fmt.Errorf("ref/platform: %s: parameter %q has invalid in %q", what, p.Name, p.In)
		}
		key := where + ":" + p.Name
		if seen[key] {
			return fmt.Errorf("ref/platform: %s: duplicate parameter %q in %q", what, p.Name, where)
		}
		seen[key] = true
		if where == "path" && (!pathNames[p.Name] || !p.Required) {
			return fmt.Errorf("ref/platform: %s: path parameter %q must appear in the path and be required", what, p.Name)
		}
		if p.Kind != "" && !isOpenAPIPrimitive(strings.ToLower(p.Kind)) {
			return fmt.Errorf("ref/platform: %s: parameter %q has unsupported primitive kind %q", what, p.Name, p.Kind)
		}
	}
	return nil
}
