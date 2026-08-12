package fh

import (
	"fmt"
	"sort"
	"strings"
)

// OpenAPISpec generates an OpenAPI 3.0 specification map from all registered routes.
func (a *App) OpenAPISpec() map[string]any {
	routes := a.Routes()

	paths := make(map[string]map[string]any)
	tagSet := make(map[string]struct{})

	for _, r := range routes {
		if r.Path == "" || r.Method == "" {
			continue
		}
		// Convert fh route path to OpenAPI format (/users/:id -> /users/{id})
		openAPIPath, pathParams := convertRouteToOpenAPIPath(r.Path)

		pathItem, exists := paths[openAPIPath]
		if !exists {
			pathItem = make(map[string]any)
			paths[openAPIPath] = pathItem
		}

		methodLower := strings.ToLower(r.Method)
		op := make(map[string]any)

		if r.Name != "" {
			op["operationId"] = r.Name
		}

		if len(r.Tags) > 0 {
			op["tags"] = r.Tags
			for _, t := range r.Tags {
				tagSet[t] = struct{}{}
			}
		}

		if r.Deprecated {
			op["deprecated"] = true
		}

		if r.Meta != nil {
			if summary, ok := r.Meta["summary"].(string); ok {
				op["summary"] = summary
			}
			if desc, ok := r.Meta["description"].(string); ok {
				op["description"] = desc
			}
		}

		if len(pathParams) > 0 {
			parameters := make([]map[string]any, 0, len(pathParams))
			for _, p := range pathParams {
				paramObj := map[string]any{
					"name":     p.name,
					"in":       "path",
					"required": !p.optional,
					"schema": map[string]any{
						"type": p.paramType,
					},
				}
				if p.format != "" {
					paramObj["schema"].(map[string]any)["format"] = p.format
				}
				parameters = append(parameters, paramObj)
			}
			op["parameters"] = parameters
		}

		op["responses"] = map[string]any{
			"200": map[string]any{
				"description": "Successful response",
			},
		}

		if r.Security.AuthRequired || len(r.Security.Scopes) > 0 || len(r.Security.Roles) > 0 {
			op["security"] = []map[string]any{
				{
					"bearerAuth": r.Security.Scopes,
				},
			}
		}

		pathItem[methodLower] = op
	}

	tagsList := make([]map[string]any, 0, len(tagSet))
	for t := range tagSet {
		tagsList = append(tagsList, map[string]any{"name": t})
	}
	sort.Slice(tagsList, func(i, j int) bool {
		return tagsList[i]["name"].(string) < tagsList[j]["name"].(string)
	})

	appName := "fh API Documentation"
	appVersion := "1.0.0"

	spec := map[string]any{
		"openapi": "3.0.3",
		"info": map[string]any{
			"title":   appName,
			"version": appVersion,
		},
		"paths": paths,
	}

	if len(tagsList) > 0 {
		spec["tags"] = tagsList
	}

	spec["components"] = map[string]any{
		"securitySchemes": map[string]any{
			"bearerAuth": map[string]any{
				"type":         "http",
				"scheme":       "bearer",
				"bearerFormat": "JWT",
			},
		},
	}

	return spec
}

type openAPIParam struct {
	name      string
	paramType string
	format    string
	optional  bool
}

func convertRouteToOpenAPIPath(path string) (string, []openAPIParam) {
	segments := strings.Split(path, "/")
	outSegments := make([]string, 0, len(segments))
	var params []openAPIParam

	for _, seg := range segments {
		if seg == "" {
			outSegments = append(outSegments, "")
			continue
		}
		if seg[0] == ':' || seg[0] == '*' {
			rawName := seg[1:]
			cleanName, constraint, isOptional := parseParamConstraint(rawName)

			p := openAPIParam{
				name:      cleanName,
				paramType: "string",
				optional:  isOptional,
			}

			switch constraint.kind {
			case constraintInt, constraintUint:
				p.paramType = "integer"
			case constraintUUID:
				p.paramType = "string"
				p.format = "uuid"
			}

			params = append(params, p)
			outSegments = append(outSegments, fmt.Sprintf("{%s}", cleanName))
		} else {
			outSegments = append(outSegments, seg)
		}
	}

	return strings.Join(outSegments, "/"), params
}
