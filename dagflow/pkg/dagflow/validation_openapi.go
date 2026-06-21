package dagflow

import (
	"fmt"
	"sort"
	"strings"
)

func ValidateConfig(cfg *Config, e *Engine) error {
	seenRoutes := map[string]RouteConfig{}
	for _, r := range FlattenRoutes(cfg) {
		key := strings.ToUpper(r.Method) + " " + normalizeRoutePath(r.Path)
		if prev, ok := seenRoutes[key]; ok {
			return fmt.Errorf("route conflict: %s conflicts with %s", r.ID, prev.ID)
		}
		seenRoutes[key] = r
		if r.Workflow == "" && r.Chain == "" && len(r.Workflows) == 0 {
			return fmt.Errorf("route %s has no target", r.ID)
		}
		if r.Workflow != "" {
			if _, err := e.workflow(r.Workflow); err != nil {
				return err
			}
		}
		if r.Chain != "" {
			if _, err := e.chain(r.Chain); err != nil {
				return err
			}
		}
	}
	for _, w := range e.ListWorkflowMetadata() {
		if w.First == "" {
			return fmt.Errorf("workflow %s missing first", w.ID)
		}
		ids := map[string]bool{}
		for _, n := range w.Nodes {
			ids[n.ID] = true
			if n.Type == NodeFunction && n.Handler == "" {
				return fmt.Errorf("node %s.%s missing handler", w.ID, n.ID)
			}
			if n.InputSchema != "" {
				if _, ok := e.Schema(n.InputSchema); !ok {
					return fmt.Errorf("node %s.%s references missing input schema %s", w.ID, n.ID, n.InputSchema)
				}
			}
			if n.OutputSchema != "" {
				if _, ok := e.Schema(n.OutputSchema); !ok {
					return fmt.Errorf("node %s.%s references missing output schema %s", w.ID, n.ID, n.OutputSchema)
				}
			}
		}
		if !ids[w.First] {
			return fmt.Errorf("workflow %s first node not found", w.ID)
		}
		for _, ed := range w.Edges {
			for _, s := range ed.Sources {
				if !ids[s] {
					return fmt.Errorf("edge %s source %s missing", ed.ID, s)
				}
			}
			for _, t := range ed.Targets {
				if !ids[t] {
					return fmt.Errorf("edge %s target %s missing", ed.ID, t)
				}
			}
		}
	}
	return nil
}

func normalizeRoutePath(p string) string {
	parts := strings.Split(strings.Trim(p, "/"), "/")
	for i, x := range parts {
		if strings.HasPrefix(x, ":") || (strings.HasPrefix(x, "{") && strings.HasSuffix(x, "}")) {
			parts[i] = ":"
		}
		if strings.HasPrefix(x, "*") {
			parts[i] = "*"
		}
	}
	return "/" + strings.Join(parts, "/")
}

func GenerateOpenAPI(cfg *Config) map[string]any {
	paths := map[string]any{}
	routes := FlattenRoutes(cfg)
	sort.Slice(routes, func(i, j int) bool { return routes[i].Path < routes[j].Path })
	for _, r := range routes {
		p := routeToOpenAPIPath(r.Path)
		m, _ := paths[p].(map[string]any)
		if m == nil {
			m = map[string]any{}
			paths[p] = m
		}
		m[strings.ToLower(r.Method)] = map[string]any{"operationId": r.ID, "tags": r.Tags, "x-workflow": r.Workflow, "x-chain": r.Chain, "x-mode": r.Mode, "requestBody": schemaRef(r.InputSchema), "responses": map[string]any{"200": map[string]any{"description": "OK", "content": map[string]any{"application/json": map[string]any{"schema": schemaRefValue(r.OutputSchema)}}}, "202": map[string]any{"description": "Accepted"}}}
	}
	return map[string]any{"openapi": "3.1.0", "info": map[string]any{"title": "DAGFlow API", "version": "1.0.0"}, "paths": paths}
}
func routeToOpenAPIPath(p string) string {
	parts := strings.Split(strings.Trim(p, "/"), "/")
	for i, x := range parts {
		if strings.HasPrefix(x, ":") {
			parts[i] = "{" + strings.TrimPrefix(x, ":") + "}"
		}
		if strings.HasPrefix(x, "*") {
			parts[i] = "{wildcard}"
		}
	}
	if len(parts) == 1 && parts[0] == "" {
		return "/"
	}
	return "/" + strings.Join(parts, "/")
}
func schemaRef(id string) any {
	if id == "" {
		return nil
	}
	return map[string]any{"content": map[string]any{"application/json": map[string]any{"schema": schemaRefValue(id)}}}
}
func schemaRefValue(id string) any {
	if id == "" {
		return map[string]any{"type": "object"}
	}
	return map[string]any{"$ref": "#/components/schemas/" + id}
}
