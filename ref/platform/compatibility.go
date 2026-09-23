package platform

import (
	"fmt"
	"strings"
)

// ContractChange describes a route or shape compatibility change.
type ContractChange struct {
	Breaking          bool
	Location, Message string
}

// CompareContracts compares externally visible routes and schemas. Optional new
// fields are compatible; removing fields, changing field types, adding required
// inputs, or changing a route shape is reported as breaking.
func CompareContracts(before, after *Platform) []ContractChange {
	if before == nil || after == nil {
		return []ContractChange{{Breaking: true, Location: "platform", Message: "both platform generations are required"}}
	}
	var changes []ContractChange
	oldRoutes, newRoutes := map[string]RouteSpec{}, map[string]RouteSpec{}
	for _, r := range before.Document.Routes {
		oldRoutes[strings.ToUpper(r.Method)+" "+r.Path] = r
	}
	for _, r := range after.Document.Routes {
		newRoutes[strings.ToUpper(r.Method)+" "+r.Path] = r
	}
	for key, old := range oldRoutes {
		next, ok := newRoutes[key]
		if !ok {
			changes = append(changes, ContractChange{true, "route " + key, "route was removed"})
			continue
		}
		oldIn, oldOut := routeShapes(before, old)
		newIn, newOut := routeShapes(after, next)
		if oldIn != newIn {
			changes = append(changes, ContractChange{true, "route " + key, fmt.Sprintf("request shape changed from %q to %q", oldIn, newIn)})
		}
		if oldOut != newOut {
			changes = append(changes, ContractChange{true, "route " + key, fmt.Sprintf("response shape changed from %q to %q", oldOut, newOut)})
		}
		compareParameters(key, old.Parameters, next.Parameters, &changes)
	}
	oldShapes, newShapes := shapeIndex(before.Document.Shapes), shapeIndex(after.Document.Shapes)
	inputShapes := collectContractInputs(before, after)
	for name, old := range oldShapes {
		next, ok := newShapes[name]
		if !ok {
			changes = append(changes, ContractChange{true, "shape " + name, "shape was removed"})
			continue
		}
		newFields := map[string]SchemaFieldSpec{}
		for _, f := range next.Props {
			newFields[f.Name] = f
		}
		oldReq := map[string]bool{}
		for _, n := range old.Required {
			oldReq[n] = true
		}
		newReq := map[string]bool{}
		for _, n := range next.Required {
			newReq[n] = true
		}
		for _, f := range old.Props {
			if f.Required {
				oldReq[f.Name] = true
			}
		}
		for _, f := range next.Props {
			if f.Required {
				newReq[f.Name] = true
			}
		}
		for _, f := range old.Props {
			n, ok := newFields[f.Name]
			if !ok {
				changes = append(changes, ContractChange{true, "shape " + name + "." + f.Name, "field was removed"})
				continue
			}
			if schemaFieldType(f) != schemaFieldType(n) {
				changes = append(changes, ContractChange{true, "shape " + name + "." + f.Name, "field type changed"})
			}
			if !oldReq[f.Name] && newReq[f.Name] {
				changes = append(changes, ContractChange{true, "shape " + name + "." + f.Name, "optional field became required"})
			}
			if len(f.Enum) > 0 && len(n.Enum) > 0 {
				for _, v := range f.Enum {
					if !containsString(n.Enum, v) {
						changes = append(changes, ContractChange{true, "shape " + name + "." + f.Name, "enum value " + v + " was removed"})
					}
				}
			}
		}
		for _, f := range next.Props {
			if _, ok := mapLookupField(old.Props, f.Name); !ok && newReq[f.Name] && inputShapes[name] {
				changes = append(changes, ContractChange{true, "shape " + name + "." + f.Name, "new required field was added"})
			}
		}
	}
	return changes
}

func routeShapes(p *Platform, r RouteSpec) (string, string) {
	for _, it := range p.Document.Intents {
		if it.Name == r.Intent {
			return it.InputSchema, it.OutputSchema
		}
	}
	return "", ""
}
func shapeIndex(shapes []SchemaSpec) map[string]SchemaSpec {
	m := map[string]SchemaSpec{}
	for _, s := range shapes {
		m[s.Name] = s
	}
	return m
}
func schemaFieldType(f SchemaFieldSpec) string {
	return strings.ToLower(f.Kind) + "|" + strings.ToLower(f.Items) + "|" + f.Schema
}
func mapLookupField(fields []SchemaFieldSpec, name string) (SchemaFieldSpec, bool) {
	for _, f := range fields {
		if f.Name == name {
			return f, true
		}
	}
	return SchemaFieldSpec{}, false
}
func compareParameters(route string, old, next []HTTPParameterSpec, out *[]ContractChange) {
	newMap := map[string]HTTPParameterSpec{}
	for _, p := range next {
		newMap[strings.ToLower(p.In)+":"+p.Name] = p
	}
	for _, p := range old {
		key := strings.ToLower(p.In) + ":" + p.Name
		n, ok := newMap[key]
		loc := "route " + route + " parameter " + key
		if !ok {
			*out = append(*out, ContractChange{true, loc, "parameter was removed"})
			continue
		}
		if p.Required != n.Required && n.Required {
			*out = append(*out, ContractChange{true, loc, "optional parameter became required"})
		}
		if schemaFieldType(SchemaFieldSpec{Kind: p.Kind, Format: p.Format}) != schemaFieldType(SchemaFieldSpec{Kind: n.Kind, Format: n.Format}) {
			*out = append(*out, ContractChange{true, loc, "parameter type changed"})
		}
	}
	oldMap := map[string]bool{}
	for _, p := range old {
		oldMap[strings.ToLower(p.In)+":"+p.Name] = true
	}
	for _, p := range next {
		key := strings.ToLower(p.In) + ":" + p.Name
		if !oldMap[key] && p.Required {
			*out = append(*out, ContractChange{true, "route " + route + " parameter " + key, "new required parameter was added"})
		}
	}
}

func collectContractInputs(platforms ...*Platform) map[string]bool {
	used := map[string]bool{}
	for _, p := range platforms {
		shapes := shapeIndex(p.Document.Shapes)
		seen := map[string]bool{}
		var visit func(string)
		visit = func(name string) {
			if name == "" || seen[name] {
				return
			}
			seen[name] = true
			used[name] = true
			shape, ok := shapes[name]
			if !ok {
				return
			}
			for _, field := range shape.Props {
				visit(field.Schema)
				if field.Items != "" && !isOpenAPIPrimitive(field.Items) {
					visit(field.Items)
				}
			}
		}
		for _, it := range p.Document.Intents {
			visit(it.InputSchema)
		}
	}
	return used
}
