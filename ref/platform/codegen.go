package platform

import (
	"fmt"
	"github.com/oarkflow/fh"
	"strings"
)

// TypeScriptTypes emits interfaces from the application's reusable HTTP shapes.
func (p *Platform) TypeScriptTypes() string {
	var b strings.Builder
	nl := string(rune(10))
	for _, shape := range p.Document.Shapes {
		fmt.Fprintf(&b, "export interface %s {%s", tsIdentifier(shape.Name), nl)
		for _, field := range shape.Props {
			required := field.Required || containsString(shape.Required, field.Name)
			optional := "?"
			if required {
				optional = ""
			}
			fmt.Fprintf(&b, "  %q%s: %s;%s", field.Name, optional, tsFieldType(field), nl)
		}
		if shape.AdditionalProperties == nil || *shape.AdditionalProperties {
			fmt.Fprintf(&b, "  [key: string]: unknown;%s", nl)
		}
		fmt.Fprintf(&b, "}%s%s", nl, nl)
	}
	return b.String()
}

// TypeScriptClient emits fetch functions with route and body types from the same declarations.
func (p *Platform) TypeScriptClient() string {
	var b strings.Builder
	nl := string(rune(10))

	b.WriteString(p.TypeScriptTypes())
	fmt.Fprintf(&b, "export function createClient(baseUrl: string, defaults: RequestInit = {}) {%s  return {%s", nl, nl)
	intents := map[string]IntentSpec{}
	for _, it := range p.Document.Intents {
		intents[it.Name] = it
	}
	for _, route := range p.Document.Routes {
		input, output := "Record<string, unknown>", "unknown"
		if it, ok := intents[route.Intent]; ok {
			if it.InputSchema != "" {
				input = tsIdentifier(it.InputSchema)
			}
			if it.OutputSchema != "" {
				output = tsIdentifier(it.OutputSchema)
			}
		}
		contractFields := make([]string, 0)
		for _, name := range pathParams(route.Path) {
			contractFields = append(contractFields, name+": string")
		}
		for _, param := range route.Parameters {
			if strings.EqualFold(param.In, "query") {
				required := "?"
				if param.Required {
					required = ""
				}
				contractFields = append(contractFields, param.Name+required+": "+tsParameterType(param))
			}
		}
		if len(contractFields) > 0 {
			input += " & { " + strings.Join(contractFields, "; ") + " }"
		}
		method := strings.ToUpper(route.Method)
		if method == "" {
			method = "GET"
		}
		body := "JSON.stringify(payload)"
		if method == "GET" || method == "HEAD" {
			body = "undefined"
		}
		fmt.Fprintf(&b, "    %s: async (input: %s, init: RequestInit = {}): Promise<%s> => {%s      let url = baseUrl + %q;%s      const values = input as unknown as Record<string, unknown>;%s      for (const [key, value] of Object.entries(values ?? {})) url = url.replace(':' + key, encodeURIComponent(String(value)));%s      const query = new URLSearchParams();%s", tsIdentifier(route.Name), input, output, nl, route.Path, nl, nl, nl, nl)
		for _, param := range route.Parameters {
			if strings.EqualFold(param.In, "query") {
				fmt.Fprintf(&b, "      if (values[%q] !== undefined) query.set(%q, String(values[%q]));%s", param.Name, param.Name, param.Name, nl)
			}
		}
		fmt.Fprintf(&b, "      const queryText = query.toString(); if (queryText) url += (url.includes('?') ? '&' : '?') + queryText;%s      const payload = { ...values };%s", nl, nl)
		if method == "GET" || method == "HEAD" { fmt.Fprintf(&b, "      void payload;%s", nl) }
		for _, name := range pathParams(route.Path) {
			fmt.Fprintf(&b, "      delete payload[%q];%s", name, nl)
		}
		for _, param := range route.Parameters {
			if strings.EqualFold(param.In, "query") {
				fmt.Fprintf(&b, "      delete payload[%q];%s", param.Name, nl)
			}
		}
		fmt.Fprintf(&b, "      const response = await fetch(url, { ...defaults, ...init, method: %q, headers: { 'content-type': 'application/json', ...defaults.headers, ...init.headers }, body: %s });%s      if (!response.ok) throw new Error('HTTP ' + response.status);%s      return response.json() as Promise<%s>;%s    },%s", method, body, nl, nl, output, nl, nl)
	}
	fmt.Fprintf(&b, "  };%s}%s", nl, nl)
	return b.String()
}
func tsFieldType(f SchemaFieldSpec) string {
	kind := strings.ToLower(f.Kind)
	if kind == "" {
		if f.Schema != "" {
			kind = "object"
		} else if f.Items != "" {
			kind = "array"
		}
	}
	switch kind {
	case "string", "time", "date", "datetime":
		return "string"
	case "int", "integer", "number", "float":
		return "number"
	case "bool", "boolean":
		return "boolean"
	case "array":
		if f.Items != "" {
			if isOpenAPIPrimitive(f.Items) {
				return tsPrimitive(f.Items) + "[]"
			}
			return tsIdentifier(f.Items) + "[]"
		}
		return "unknown[]"
	case "object", "any", "":
		if f.Schema != "" {
			return tsIdentifier(f.Schema)
		}
		return "Record<string, unknown>"
	default:
		return tsIdentifier(f.Kind)
	}
}
func tsPrimitive(kind string) string {
	switch strings.ToLower(kind) {
	case "string", "time", "date", "datetime":
		return "string"
	case "int", "integer", "number", "float":
		return "number"
	case "bool", "boolean":
		return "boolean"
	default:
		return "unknown"
	}
}
func tsIdentifier(value string) string {
	var b strings.Builder
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '$' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	out := b.String()
	if out == "" {
		return "generated"
	}
	if out[0] >= '0' && out[0] <= '9' {
		out = "_" + out
	}
	return out
}

// MountArtifacts mounts generated contracts at caller-selected paths. Keep these
// routes behind the application's normal access controls when contracts are private.
func (p *Platform) MountArtifacts(app *fh.App, openAPIPath, typesPath string) error {
	if app == nil {
		return fmt.Errorf("ref/platform: nil app")
	}
	if openAPIPath != "" {
		app.Get(openAPIPath, func(c fh.Ctx) error { return c.JSON(p.OpenAPI()) }).Name("ref.openapi")
	}
	if typesPath != "" {
		app.Get(typesPath, func(c fh.Ctx) error {
			c.Set("Content-Type", "text/typescript; charset=utf-8")
			return c.SendString(p.TypeScriptClient())
		}).Name("ref.typescript")
	}
	return nil
}

// GoContractSmokeTest emits a standalone Go test scaffold for deployed route
// smoke checks. It accepts REF_BASE_URL and optionally REF_TEST_BEARER_TOKEN.
func (p *Platform) GoContractSmokeTest(packageName string) string {
	if !validGoPackage(packageName) {
		packageName = "contracttest"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "package %s\n\nimport (\n\t\"net/http\"\n\t\"os\"\n\t\"strings\"\n\t\"testing\"\n)\n\n", packageName)
	b.WriteString("func TestREFRouteSmoke(t *testing.T) {\n base := os.Getenv(\"REF_BASE_URL\"); if base == \"\" { t.Skip(\"set REF_BASE_URL to the running application\") }\n client := &http.Client{}\n cases := []struct{ method, path string }{\n")
	for _, route := range p.Document.Routes {
		path := route.Path
		for _, name := range pathParams(path) {
			path = strings.ReplaceAll(path, ":"+name, "contract-test")
		}
		method := strings.ToUpper(route.Method)
		if method == "" {
			method = "GET"
		}
		fmt.Fprintf(&b, "  {%q, %q},\n", method, path)
	}
	b.WriteString(" }\n for _, tc := range cases { t.Run(tc.method+\" \"+tc.path, func(t *testing.T) { var body *strings.Reader; if tc.method == \"GET\" || tc.method == \"HEAD\" { body = strings.NewReader(\"\") } else { body = strings.NewReader(\"{}\") }; req, err := http.NewRequest(tc.method, strings.TrimRight(base, \"/\")+tc.path, body); if err != nil { t.Fatal(err) }; req.Header.Set(\"Content-Type\", \"application/json\"); if token := os.Getenv(\"REF_TEST_BEARER_TOKEN\"); token != \"\" { req.Header.Set(\"Authorization\", \"Bearer \"+token) }; resp, err := client.Do(req); if err != nil { t.Fatal(err) }; defer resp.Body.Close(); if resp.StatusCode >= 500 { t.Fatalf(\"%s returned HTTP %d\", tc.path, resp.StatusCode) } }) }\n}\n")
	return b.String()
}
func validGoPackage(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9') {
			return false
		}
	}
	switch name {
	case "break", "case", "chan", "const", "continue", "default", "defer", "else", "fallthrough", "for", "func", "go", "goto", "if", "import", "interface", "map", "package", "range", "return", "select", "struct", "switch", "type", "var":
		return false
	}
	return true
}

func tsParameterType(param HTTPParameterSpec) string {
	kind := strings.ToLower(param.Kind)
	if kind == "" {
		kind = "string"
	}
	return tsPrimitive(kind)
}
