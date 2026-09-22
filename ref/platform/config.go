package platform

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Config accessors.
//
// A node's and a resource's config arrive as map[string]any because BCL is
// dynamically shaped. Every read goes through a helper here so the error
// message is uniform ("config.timeout must be a duration (got int)") and so the
// dozens of resource and action factories do not each invent their own
// coercion rules — which is how one provider ends up accepting "10s" for a
// timeout and another silently reading it as zero.
//
// These are build-time helpers. They are called once per node while compiling a
// generation, never per request, so clarity beats micro-optimisation here.

// requiredString reads a non-empty string, failing when absent or blank.
func requiredString(config map[string]any, key string) (string, error) {
	value, _ := config[key].(string)
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("config.%s is required", key)
	}
	return value, nil
}

// configString reads a string with a fallback. A BCL bare identifier arrives as
// a map with a "$ident" key, so an author may write either `method POST` or
// `method "POST"` and get the same result.
func configString(config map[string]any, key, fallback string) string {
	switch value := config[key].(type) {
	case string:
		if value != "" {
			return value
		}
	case map[string]any:
		if text := identString(value); text != "" {
			return text
		}
	}
	return fallback
}

// configBool reads a boolean, accepting the string forms that arrive from
// templated or environment-sourced configuration.
func configBool(config map[string]any, key string, fallback bool) bool {
	value, ok := config[key]
	if !ok || value == nil {
		return fallback
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(typed))
		if err != nil {
			return fallback
		}
		return parsed
	default:
		return fallback
	}
}

// configDuration reads a duration. BCL may deliver a time.Duration, a string
// ("30s"), or a wrapper object; all three are accepted, and anything else is an
// error rather than a silent zero — a timeout that quietly became zero is worse
// than a deployment that refuses to start.
func configDuration(config map[string]any, key string, fallback time.Duration) (time.Duration, error) {
	value, ok := config[key]
	if !ok || value == nil {
		return fallback, nil
	}
	switch typed := value.(type) {
	case time.Duration:
		return typed, nil
	case string:
		parsed, err := time.ParseDuration(typed)
		if err != nil {
			return 0, fmt.Errorf("config.%s: %w", key, err)
		}
		return parsed, nil
	case int:
		return time.Duration(typed) * time.Second, nil
	case int64:
		return time.Duration(typed) * time.Second, nil
	case float64:
		return time.Duration(typed * float64(time.Second)), nil
	case map[string]any:
		if raw, ok := typed["$duration"].(string); ok {
			parsed, err := time.ParseDuration(raw)
			if err != nil {
				return 0, fmt.Errorf("config.%s: %w", key, err)
			}
			return parsed, nil
		}
		return 0, fmt.Errorf("config.%s must be a duration", key)
	default:
		return 0, fmt.Errorf("config.%s must be a duration (got %T)", key, value)
	}
}

// configInt reads an integer, rejecting a fractional number rather than
// truncating it.
func configInt(config map[string]any, key string, fallback int) (int, error) {
	value, ok := config[key]
	if !ok || value == nil {
		return fallback, nil
	}
	switch typed := value.(type) {
	case int:
		return typed, nil
	case int32:
		return int(typed), nil
	case int64:
		return int(typed), nil
	case float64:
		if typed != float64(int(typed)) {
			return 0, fmt.Errorf("config.%s must be a whole number", key)
		}
		return int(typed), nil
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		if err != nil {
			return 0, fmt.Errorf("config.%s must be an integer: %w", key, err)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("config.%s must be an integer (got %T)", key, value)
	}
}

func configInt64(config map[string]any, key string, fallback int64) (int64, error) {
	value, err := configInt(config, key, int(fallback))
	return int64(value), err
}

// configFloat reads a number.
func configFloat(config map[string]any, key string, fallback float64) (float64, error) {
	value, ok := config[key]
	if !ok || value == nil {
		return fallback, nil
	}
	number, converted := ToFloat(value)
	if !converted {
		return 0, fmt.Errorf("config.%s must be a number (got %T)", key, value)
	}
	return number, nil
}

// configExpr compiles an optional expression from config. A missing key yields
// a nil Expression, which every guard treats as "no condition".
func configExpr(config map[string]any, key string) (*Expression, error) {
	raw := configString(config, key, "")
	expr, err := CompileExpr(raw)
	if err != nil {
		return nil, fmt.Errorf("config.%s: %w", key, err)
	}
	return expr, nil
}

// requiredExpr compiles a mandatory expression.
func requiredExpr(config map[string]any, key string) (*Expression, error) {
	expr, err := configExpr(config, key)
	if err != nil {
		return nil, err
	}
	if expr == nil {
		return nil, fmt.Errorf("config.%s is required", key)
	}
	return expr, nil
}

// configTemplate compiles an optional template from config.
func configTemplate(config map[string]any, key, fallback string) (*Template, error) {
	raw := configString(config, key, fallback)
	if raw == "" {
		return nil, nil
	}
	tmpl, err := CompileTemplate(raw)
	if err != nil {
		return nil, fmt.Errorf("config.%s: %w", key, err)
	}
	return tmpl, nil
}

// configMap reads a nested object.
func configMap(config map[string]any, key string) map[string]any {
	nested, _ := config[key].(map[string]any)
	return nested
}

// configBlocks reads a repeated structured value out of a config map, accepting
// the first of the given names that is present.
//
// Inside a `config { }` block, repeated structure has to be written as a list of
// objects — `cases [ { … } { … } ]` — because BCL cannot carry repeated named
// blocks inside an untyped map (see bclcompat.go). Callers pass the plural list
// name and the singular block name, so a document written either way works and the
// error message can name the form that is expected.
func configBlocks(config map[string]any, names ...string) []map[string]any {
	for _, key := range names {
		switch value := config[key].(type) {
		case []map[string]any:
			return value
		case map[string]any:
			return []map[string]any{value}
		case []any:
			out := make([]map[string]any, 0, len(value))
			for _, item := range value {
				if object, ok := item.(map[string]any); ok {
					out = append(out, object)
				}
			}
			if len(out) > 0 {
				return out
			}
		}
	}
	return nil
}

// stringSlice reads a list of strings, accepting a single string (split on
// commas), a real list, or a list of BCL identifiers. Comma splitting matters
// because allowlists routinely arrive from a single environment variable.
func stringSlice(value any) []string {
	switch values := value.(type) {
	case nil:
		return nil
	case string:
		parts := strings.Split(values, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
		return out
	case []string:
		return append([]string(nil), values...)
	case []any:
		out := make([]string, 0, len(values))
		for _, item := range values {
			switch typed := item.(type) {
			case string:
				out = append(out, typed)
			case map[string]any:
				if text := identString(typed); text != "" {
					out = append(out, text)
				}
			default:
				out = append(out, Stringify(item))
			}
		}
		return out
	default:
		return nil
	}
}

// configStrings reads a list of strings from a config key.
func configStrings(config map[string]any, key string) []string {
	return stringSlice(config[key])
}

// stringMap reads a map of strings, rendering non-string values rather than
// dropping them — a header value written as a number is still a header value.
func stringMap(value any) map[string]string {
	result := map[string]string{}
	switch object := value.(type) {
	case map[string]string:
		for key, item := range object {
			result[key] = item
		}
	case map[string]any:
		for key, item := range object {
			switch typed := item.(type) {
			case string:
				result[key] = typed
			case map[string]any:
				if text := identString(typed); text != "" {
					result[key] = text
				}
			default:
				result[key] = Stringify(item)
			}
		}
	}
	return result
}

// identString unwraps a BCL bare identifier or reference object.
func identString(object map[string]any) string {
	for _, key := range []string{"$ident", "$ref", "$value"} {
		if text, ok := object[key].(string); ok && text != "" {
			return text
		}
	}
	return ""
}

// rejectUnknownConfig reports config keys a provider does not understand. A
// misspelled key is otherwise the most common way a production setting silently
// does nothing — the deployment starts, the timeout stays at its default, and
// nobody finds out until the incident.
func rejectUnknownConfig(what string, config map[string]any, known ...string) error {
	if len(config) == 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(known))
	for _, key := range known {
		allowed[key] = struct{}{}
	}
	var unknown []string
	for key := range config {
		if _, ok := allowed[key]; !ok {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	return fmt.Errorf("%s: unknown config %s", what, strings.Join(unknown, ", "))
}
