// Package rewrite performs bounded internal request rewrites.
package rewrite

import (
	"net"
	"strings"

	"github.com/oarkflow/fh"
)

// Rule matches a request and internally reroutes it to To. From uses exactly
// the router's syntax and matcher: static paths, :named parameters, and
// terminal * or *named wildcards. Targets may reference captures as :name,
// {name}, *name, {name}, or * for an unnamed wildcard.
type Rule struct {
	From string
	To   string

	Methods []string
	Host    string            // exact host or *.example.com; port is optional
	Headers map[string]string // "*" means the header only needs to exist
	Query   map[string]string // "*" means the query key only needs to exist
	When    func(*fh.Ctx) bool
}

type Config struct {
	Rules         []Rule
	Next          func(*fh.Ctx) bool
	PreserveQuery bool
}

func New(rules ...Rule) fh.HandlerFunc {
	return WithConfig(Config{Rules: rules, PreserveQuery: true})
}

type compiledRule struct {
	rule    Rule
	pattern *fh.RoutePattern
	methods map[string]struct{}
}

func WithConfig(cfg Config) fh.HandlerFunc {
	compiled := make([]compiledRule, len(cfg.Rules))
	for i, rule := range cfg.Rules {
		compiled[i] = compile(rule)
	}
	return func(c *fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}
		var params []fh.Param
		for _, rule := range compiled {
			if !constraintsMatch(rule, c) || !rule.pattern.Match(c.Path(), &params) {
				continue
			}
			target := expandTarget(rule.rule.To, params)
			if cfg.PreserveQuery && !strings.Contains(target, "?") {
				if uri := string(c.Header.URI); len(uri) > len(c.Path()) {
					target += uri[len(c.Path()):]
				}
			}
			return c.Rewrite(target)
		}
		return c.Next()
	}
}

func compile(rule Rule) compiledRule {
	if rule.From == "" || rule.To == "" || rule.To[0] != '/' {
		panic("rewrite: From and To must be absolute paths")
	}
	methods := make(map[string]struct{}, len(rule.Methods))
	for _, method := range rule.Methods {
		methods[strings.ToUpper(strings.TrimSpace(method))] = struct{}{}
	}
	return compiledRule{rule: rule, pattern: fh.CompileRoutePattern(rule.From), methods: methods}
}

func constraintsMatch(rule compiledRule, c *fh.Ctx) bool {
	if len(rule.methods) > 0 {
		if _, ok := rule.methods[c.Method()]; !ok {
			return false
		}
	}
	if rule.rule.Host != "" && !matchHost(rule.rule.Host, string(c.Header.Host)) {
		return false
	}
	for key, want := range rule.rule.Headers {
		got := c.Get(key)
		if got == "" || want != "*" && !strings.EqualFold(got, want) {
			return false
		}
	}
	for key, want := range rule.rule.Query {
		got := c.Query(key)
		if got == "" || want != "*" && got != want {
			return false
		}
	}
	return rule.rule.When == nil || rule.rule.When(c)
}

func expandTarget(target string, params []fh.Param) string {
	for _, param := range params {
		name, value := param.Key, param.Value
		if name == "*" {
			target = strings.ReplaceAll(target, "*", value)
			continue
		}
		target = strings.ReplaceAll(target, "{"+name+"}", value)
		target = strings.ReplaceAll(target, ":"+name, value)
		target = strings.ReplaceAll(target, "*"+name, value)
	}
	return target
}

func matchHost(pattern, host string) bool {
	patternHost, hostOnly := withoutPort(pattern), withoutPort(host)
	if strings.HasPrefix(patternHost, "*.") {
		suffix := strings.ToLower(patternHost[1:])
		hostOnly = strings.ToLower(hostOnly)
		return strings.HasSuffix(hostOnly, suffix) && len(hostOnly) > len(suffix)
	}
	if strings.Contains(pattern, ":") {
		return strings.EqualFold(pattern, host)
	}
	return strings.EqualFold(patternHost, hostOnly)
}

func withoutPort(host string) string {
	if value, _, err := net.SplitHostPort(host); err == nil {
		return value
	}
	return strings.Trim(host, "[]")
}
