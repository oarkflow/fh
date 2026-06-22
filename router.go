package fh

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type HandlerFunc func(*Ctx) error

// Handler is the Fiber-compatible name for a request handler.
type Handler = HandlerFunc

type Param struct {
	Key   string
	Value string
}

type Router struct {
	mu sync.RWMutex

	frozen atomic.Bool

	// If true, route params are converted from []byte to string without allocation.
	//
	// Fastest mode, but only safe if:
	//   - request path buffer lives until the handler finishes
	//   - params are not stored after request completion
	//   - params are not used asynchronously after request completion
	//
	// Default false is safer.
	UnsafeParams bool

	trees      map[string]*node
	named      map[string]namedRoute
	routeNames map[string]string
	routes     map[string]struct{}
}

type namedRoute struct {
	method string
	path   string
}

var (
	ErrRouteNotFound     = errors.New("fasthttp: named route not found")
	ErrRouteParamMissing = errors.New("fasthttp: required route parameter missing")
)

type node struct {
	static map[string]*node

	param     *node
	paramName string

	wild     *node
	wildName string

	handler *routeEndpoint
}

type routeEndpoint struct {
	fn        HandlerFunc
	paramKeys []string
}

func newRouter() *Router {
	return NewRouter()
}

func NewRouter() *Router {
	return &Router{
		trees:      make(map[string]*node, 16),
		named:      make(map[string]namedRoute, 16),
		routeNames: make(map[string]string, 16),
		routes:     make(map[string]struct{}, 16),
	}
}

// RoutePattern is an immutable, compiled route path matcher. It uses the same
// trie insertion, precedence, path cleanup, parameter, and wildcard logic as
// Router. It is useful for middleware that needs router-identical path matching.
type RoutePattern struct{ root *node }

// CompileRoutePattern compiles one router path pattern. Supported forms are
// static segments, :named parameters, and terminal * or *named wildcards.
func CompileRoutePattern(pattern string) *RoutePattern {
	pattern = normalizeRoutePath("MATCH", pattern)
	root := &node{}
	insertRoute(root, "MATCH", pattern, splitRouteSegments(pattern), 0, func(*Ctx) error { return nil }, nil)
	return &RoutePattern{root: root}
}

// Match matches path and appends captured values to params. Query strings are
// ignored. The returned parameter strings are safe copies.
func (p *RoutePattern) Match(path string, params *[]Param) bool {
	if p == nil || p.root == nil {
		return false
	}
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	if params == nil {
		var local []Param
		params = &local
	} else {
		*params = (*params)[:0]
	}
	return match(p.root, cleanLookupPath(s2b(path)), params, false) != nil
}

// AddNamed registers a route and gives it a name for reverse URL generation.
func (r *Router) AddNamed(method, path, name string, h HandlerFunc) {
	r.Add(method, path, h)
	r.Name(method, path, name)
}

// Name assigns a unique name to an already registered route.
func (r *Router) Name(method, path, name string) {
	method = strings.ToUpper(strings.TrimSpace(method))
	path = normalizeRoutePath(method, path)
	name = strings.TrimSpace(name)
	if name == "" {
		panic("fasthttp: route name must not be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen.Load() {
		panic("fasthttp: cannot name route after router is frozen")
	}
	key := method + " " + path
	if _, ok := r.routes[key]; !ok {
		panic(fmt.Sprintf("fasthttp: cannot name unregistered route %s", key))
	}
	if old, ok := r.named[name]; ok && (old.method != method || old.path != path) {
		panic(fmt.Sprintf("fasthttp: duplicate route name %q", name))
	}
	if oldName, ok := r.routeNames[key]; ok && oldName != name {
		delete(r.named, oldName)
	}
	r.named[name] = namedRoute{method: method, path: path}
	r.routeNames[key] = name
}

// URL builds a path for a named route. Unused values are appended as a
// deterministically ordered query string.
func (r *Router) URL(name string, values ...map[string]string) (string, error) {
	if !r.frozen.Load() {
		r.mu.RLock()
		defer r.mu.RUnlock()
	}
	route, ok := r.named[name]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrRouteNotFound, name)
	}
	params := map[string]string(nil)
	if len(values) > 0 {
		params = values[0]
	}
	used := make(map[string]struct{})
	segments := splitRouteSegments(route.path)
	for i, segment := range segments {
		if segment == "" || (segment[0] != ':' && segment[0] != '*') {
			continue
		}
		key := segment[1:]
		if key == "" {
			key = "*"
		}
		value, found := params[key]
		if !found {
			return "", fmt.Errorf("%w %q for route %q", ErrRouteParamMissing, key, name)
		}
		used[key] = struct{}{}
		if segment[0] == '*' {
			parts := strings.Split(value, "/")
			for j := range parts {
				parts[j] = url.PathEscape(parts[j])
			}
			segments[i] = strings.Join(parts, "/")
		} else {
			segments[i] = url.PathEscape(value)
		}
	}
	path := "/" + strings.Join(segments, "/")
	if route.path == "*" {
		path = "*"
	}
	query := make(url.Values)
	for key, value := range params {
		if _, ok := used[key]; !ok {
			query.Set(key, value)
		}
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return path, nil
}

// Freeze makes the router read-only.
// Call this after registering all routes and before serving traffic.
//
// After Freeze(), Find/FindBytes/Allowed/Methods use lock-free reads.
func (r *Router) Freeze() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.frozen.Store(true)
}

func (r *Router) Add(method, path string, h HandlerFunc) {
	method = strings.ToUpper(strings.TrimSpace(method))

	if !validTokenString(method) {
		panic("fasthttp: invalid route method")
	}

	if h == nil {
		panic("fasthttp: nil route handler")
	}

	path = normalizeRoutePath(method, path)

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.frozen.Load() {
		panic("fasthttp: cannot add route after router is frozen")
	}

	root := r.trees[method]
	if root == nil {
		root = &node{}
		r.trees[method] = root
	}

	segments := splitRouteSegments(path)
	insertRoute(root, method, path, segments, 0, h, nil)
	r.routes[method+" "+path] = struct{}{}
}

func (r *Router) Find(method, path string, params *[]Param) HandlerFunc {
	if r.frozen.Load() {
		return r.findNoLock(method, s2b(path), params)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.findNoLock(method, s2b(path), params)
}

func (r *Router) FindBytes(method, path []byte, params *[]Param) HandlerFunc {
	// Request parser already validates method as a token and normal HTTP clients
	// send canonical uppercase methods. Avoid strings.ToUpper on the hot path.
	if r.frozen.Load() {
		return r.findNoLockCanonical(b2s(method), path, params)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.findNoLockCanonical(b2s(method), path, params)
}

func (r *Router) findNoLock(method string, path []byte, params *[]Param) HandlerFunc {
	method = strings.ToUpper(method)
	return r.findNoLockCanonical(method, path, params)
}

func (r *Router) findNoLockCanonical(method string, path []byte, params *[]Param) HandlerFunc {

	var local []Param
	if params == nil {
		params = &local
	} else {
		*params = (*params)[:0]
	}

	root := r.trees[method]

	// HEAD falls back to GET if HEAD is not explicitly registered.
	if root == nil && method == "HEAD" {
		root = r.trees["GET"]
	}

	if root == nil {
		return nil
	}

	return match(root, cleanLookupPath(path), params, r.UnsafeParams)
}

// Allowed returns methods that match path in deterministic order.
//
// Behavior:
//   - if GET matches and HEAD is not registered, HEAD is included
//   - if any method matches, OPTIONS is included
func (r *Router) Allowed(path []byte) []string {
	if r.frozen.Load() {
		return r.allowedNoLock(path)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.allowedNoLock(path)
}

func (r *Router) allowedNoLock(path []byte) []string {
	canonical := []string{
		"GET",
		"HEAD",
		"POST",
		"PUT",
		"PATCH",
		"DELETE",
		"CONNECT",
		"OPTIONS",
		"TRACE",
	}

	lookupPath := cleanLookupPath(path)

	allowed := make([]string, 0, 8)
	seen := make(map[string]struct{}, len(r.trees)+2)

	var tmp []Param

	add := func(method string, root *node) {
		if root == nil {
			return
		}

		tmp = tmp[:0]

		if match(root, lookupPath, &tmp, r.UnsafeParams) != nil {
			if _, ok := seen[method]; !ok {
				seen[method] = struct{}{}
				allowed = append(allowed, method)
			}
		}
	}

	for _, method := range canonical {
		add(method, r.trees[method])
	}

	if _, hasGET := seen["GET"]; hasGET {
		if _, hasHEAD := seen["HEAD"]; !hasHEAD {
			seen["HEAD"] = struct{}{}
			allowed = append(allowed, "HEAD")
		}
	}

	extra := make([]string, 0)

	for method, root := range r.trees {
		if _, ok := seen[method]; ok {
			continue
		}

		tmp = tmp[:0]

		if match(root, lookupPath, &tmp, r.UnsafeParams) != nil {
			seen[method] = struct{}{}
			extra = append(extra, method)
		}
	}

	sort.Strings(extra)
	allowed = append(allowed, extra...)

	if len(allowed) > 0 {
		if _, hasOPTIONS := seen["OPTIONS"]; !hasOPTIONS {
			allowed = append(allowed, "OPTIONS")
		}
	}

	return allowed
}

func (r *Router) Methods() []string {
	if r.frozen.Load() {
		return r.methodsNoLock()
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.methodsNoLock()
}

func (r *Router) methodsNoLock() []string {
	methods := make([]string, 0, len(r.trees)+2)
	seen := make(map[string]struct{}, len(r.trees)+2)

	for method := range r.trees {
		methods = append(methods, method)
		seen[method] = struct{}{}
	}

	if _, hasGET := seen["GET"]; hasGET {
		if _, hasHEAD := seen["HEAD"]; !hasHEAD {
			seen["HEAD"] = struct{}{}
			methods = append(methods, "HEAD")
		}
	}

	if _, hasOPTIONS := seen["OPTIONS"]; !hasOPTIONS {
		methods = append(methods, "OPTIONS")
	}

	sort.Strings(methods)

	return methods
}

func insertRoute(
	n *node,
	method string,
	fullPath string,
	segments []string,
	index int,
	h HandlerFunc,
	paramKeys []string,
) {
	if index == len(segments) {
		if n.handler != nil {
			panic(fmt.Sprintf("fasthttp: duplicate route %s %s", method, fullPath))
		}

		n.handler = &routeEndpoint{
			fn:        h,
			paramKeys: append([]string(nil), paramKeys...),
		}

		return
	}

	seg := segments[index]

	if seg == "" {
		panic(fmt.Sprintf("fasthttp: empty path segment in route %s %s", method, fullPath))
	}

	switch {
	case seg[0] == ':':
		name := seg[1:]

		validateParamName(method, fullPath, name)

		if n.param == nil {
			n.param = &node{}
		}

		paramKeys = append(paramKeys, name)
		insertRoute(n.param, method, fullPath, segments, index+1, h, paramKeys)

	case seg[0] == '*':
		name := seg[1:]

		// Supports:
		//   /static/*      => key "*"
		//   /static/*path  => key "path"
		if name == "" {
			name = "*"
		} else {
			validateParamName(method, fullPath, name)
		}

		if index != len(segments)-1 {
			panic(fmt.Sprintf("fasthttp: wildcard must be final segment in route %s %s", method, fullPath))
		}

		if n.wild == nil {
			n.wild = &node{}
			n.wildName = name
		} else if n.wildName != name {
			panic(fmt.Sprintf(
				"fasthttp: conflicting wildcard name in route %s %s: existing *%s, new *%s",
				method,
				fullPath,
				n.wildName,
				name,
			))
		}

		paramKeys = append(paramKeys, name)
		insertRoute(n.wild, method, fullPath, segments, len(segments), h, paramKeys)

	default:
		validateStaticSegment(method, fullPath, seg)

		if n.static == nil {
			n.static = make(map[string]*node, 4)
		}

		child := n.static[seg]
		if child == nil {
			child = &node{}
			n.static[seg] = child
		}

		insertRoute(child, method, fullPath, segments, index+1, h, paramKeys)
	}
}

func match(n *node, path []byte, params *[]Param, unsafeParams bool) HandlerFunc {
	if len(path) == 0 {
		if h := endpointHandler(n.handler, params); h != nil {
			return h
		}

		// Allow /static/* to match /static and /static/
		// with an empty wildcard value.
		if n.wild != nil {
			mark := len(*params)

			*params = append(*params, Param{
				Value: "",
			})

			if h := endpointHandler(n.wild.handler, params); h != nil {
				return h
			}

			*params = (*params)[:mark]
		}

		return nil
	}

	seg, rest := nextSegment(path)

	// 1. Static wins.
	if len(n.static) > 0 {
		if child := n.static[b2s(seg)]; child != nil {
			mark := len(*params)

			if h := match(child, rest, params, unsafeParams); h != nil {
				return h
			}

			*params = (*params)[:mark]
		}
	}

	// 2. Param second.
	if n.param != nil && len(seg) > 0 {
		mark := len(*params)

		*params = append(*params, Param{
			Value: paramValue(seg, unsafeParams),
		})

		if h := match(n.param, rest, params, unsafeParams); h != nil {
			return h
		}

		*params = (*params)[:mark]
	}

	// 3. Wildcard last.
	if n.wild != nil {
		mark := len(*params)

		*params = append(*params, Param{
			Value: paramValue(path, unsafeParams),
		})

		if h := endpointHandler(n.wild.handler, params); h != nil {
			return h
		}

		*params = (*params)[:mark]
	}

	return nil
}

func endpointHandler(endpoint *routeEndpoint, params *[]Param) HandlerFunc {
	if endpoint == nil {
		return nil
	}

	if len(endpoint.paramKeys) != len(*params) {
		return nil
	}

	for i := range endpoint.paramKeys {
		(*params)[i].Key = endpoint.paramKeys[i]
	}

	return endpoint.fn
}

func normalizeRoutePath(method, path string) string {
	path = strings.TrimSpace(path)

	if path == "" {
		path = "/"
	}

	if method == "OPTIONS" && path == "*" {
		return "*"
	}

	if path[0] != '/' {
		panic("fasthttp: route path must begin with '/'")
	}

	if len(path) > 1 && strings.Contains(path, "//") {
		panic("fasthttp: route path must not contain duplicate slashes")
	}

	return path
}

func splitRouteSegments(path string) []string {
	if path == "" || path == "/" {
		return nil
	}

	if path == "*" {
		return []string{"*"}
	}

	if path[0] == '/' {
		path = path[1:]
	}

	return strings.Split(path, "/")
}

func cleanLookupPath(path []byte) []byte {
	for len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	return path
}

func nextSegment(path []byte) (seg, rest []byte) {
	for i := 0; i < len(path); i++ {
		if path[i] == '/' {
			seg = path[:i]
			rest = path[i+1:]

			for len(rest) > 0 && rest[0] == '/' {
				rest = rest[1:]
			}

			return seg, rest
		}
	}

	return path, nil
}

func validateStaticSegment(method, fullPath, seg string) {
	if seg == "" {
		panic(fmt.Sprintf("fasthttp: empty path segment in route %s %s", method, fullPath))
	}

	if seg[0] == ':' || seg[0] == '*' {
		return
	}

	if strings.ContainsAny(seg, ":*") {
		panic(fmt.Sprintf(
			"fasthttp: ':' or '*' must appear only at the beginning of a segment in route %s %s",
			method,
			fullPath,
		))
	}
}

func validateParamName(method, fullPath, name string) {
	if name == "" {
		panic(fmt.Sprintf("fasthttp: empty route parameter in route %s %s", method, fullPath))
	}

	for i := 0; i < len(name); i++ {
		c := name[i]

		ok :=
			c >= 'a' && c <= 'z' ||
				c >= 'A' && c <= 'Z' ||
				c >= '0' && c <= '9' ||
				c == '_'

		if !ok {
			panic(fmt.Sprintf(
				"fasthttp: invalid route parameter %q in route %s %s",
				name,
				method,
				fullPath,
			))
		}
	}
}

func validTokenString(s string) bool {
	if s == "" {
		return false
	}

	for i := 0; i < len(s); i++ {
		c := s[i]

		ok :=
			c == '!' ||
				c == '#' ||
				c == '$' ||
				c == '%' ||
				c == '&' ||
				c == '\'' ||
				c == '*' ||
				c == '+' ||
				c == '-' ||
				c == '.' ||
				c == '^' ||
				c == '_' ||
				c == '`' ||
				c == '|' ||
				c == '~' ||
				c >= '0' && c <= '9' ||
				c >= 'A' && c <= 'Z' ||
				c >= 'a' && c <= 'z'

		if !ok {
			return false
		}
	}

	return true
}

func hasParams(path string) bool {
	for i := 0; i < len(path); i++ {
		if path[i] == ':' || path[i] == '*' {
			return true
		}
	}

	return false
}

func paramValue(b []byte, unsafeMode bool) string {
	if unsafeMode {
		return b2s(b)
	}

	return string(b)
}
