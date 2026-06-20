package fasthttp

import (
	"sort"
	"strings"
	"unsafe"
)

type HandlerFunc func(*Ctx) error

type Param struct {
	Key   string
	Value string
}

// Allowed returns methods that match path in deterministic order.
func (r *Router) Allowed(path []byte) []string {
	canonical := []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "CONNECT", "OPTIONS", "TRACE"}
	allowed := make([]string, 0, 8)
	seen := make(map[string]struct{}, len(r.trees)+1)
	var params []Param
	add := func(method string, root *node) {
		params = params[:0]
		if root != nil && match(root, path, &params) != nil {
			if _, ok := seen[method]; !ok {
				seen[method] = struct{}{}
				allowed = append(allowed, method)
			}
		}
	}
	for _, method := range canonical {
		add(method, r.trees[method])
	}
	if _, ok := seen["GET"]; ok {
		if _, hasHead := seen["HEAD"]; !hasHead {
			seen["HEAD"] = struct{}{}
			allowed = append(allowed, "HEAD")
		}
	}
	extra := make([]string, 0)
	for method, root := range r.trees {
		if _, ok := seen[method]; ok {
			continue
		}
		before := len(allowed)
		add(method, root)
		if len(allowed) > before {
			extra = append(extra, method)
			allowed = allowed[:before]
		}
	}
	sort.Strings(extra)
	allowed = append(allowed, extra...)
	if len(allowed) > 0 {
		if _, ok := seen["OPTIONS"]; !ok {
			allowed = append(allowed, "OPTIONS")
		}
	}
	return allowed
}

type node struct {
	path     string
	children []*node
	handler  *routeEndpoint
	isParam  bool
	isWild   bool
}

type routeEndpoint struct {
	fn        HandlerFunc
	paramKeys []string
}

type Router struct {
	trees map[string]*node
}

func (r *Router) Methods() []string {
	methods := make([]string, 0, len(r.trees)+2)
	for method := range r.trees {
		methods = append(methods, method)
	}
	sort.Strings(methods)
	has := func(want string) bool {
		for _, method := range methods {
			if method == want {
				return true
			}
		}
		return false
	}
	if has("GET") && !has("HEAD") {
		methods = append(methods, "HEAD")
	}
	if !has("OPTIONS") {
		methods = append(methods, "OPTIONS")
	}
	sort.Strings(methods)
	return methods
}

func newRouter() *Router {
	return &Router{
		trees: make(map[string]*node, 8),
	}
}

func (r *Router) Add(method, path string, h HandlerFunc) {
	method = strings.ToUpper(method)
	if !validToken([]byte(method)) {
		panic("fasthttp: invalid route method")
	}
	if h == nil {
		panic("fasthttp: nil route handler")
	}
	if path == "" {
		path = "/"
	}
	if path[0] != '/' && !(method == "OPTIONS" && path == "*") && method != "CONNECT" {
		panic("fasthttp: route path must begin with '/'")
	}
	if r.trees[method] == nil {
		r.trees[method] = &node{}
	}
	insert(r.trees[method], path, h, nil)
}

func hasParams(path string) bool {
	for i := 0; i < len(path); i++ {
		if path[i] == ':' || path[i] == '*' {
			return true
		}
	}
	return false
}

func (r *Router) FindBytes(method, path []byte, params *[]Param) HandlerFunc {
	root := r.trees[string(method)]
	if root == nil {
		return nil
	}
	return match(root, path, params)
}

func (r *Router) Find(method, path string, params *[]Param) HandlerFunc {
	root := r.trees[method]
	if root == nil {
		return nil
	}
	return match(root, []byte(path), params)
}

func insert(n *node, path string, h HandlerFunc, paramKeys []string) {
	if path == "" || path == "/" {
		n.handler = &routeEndpoint{fn: h, paramKeys: append([]string(nil), paramKeys...)}
		return
	}

	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	seg, rest := splitPath(path)
	if strings.HasPrefix(seg, ":") {
		if len(seg) == 1 {
			panic("fasthttp: empty route parameter")
		}
		paramKeys = append(paramKeys, seg[1:])
	} else if seg == "*" {
		paramKeys = append(paramKeys, "*")
		if rest != "" {
			panic("fasthttp: wildcard must be the final route segment")
		}
	}

	for _, child := range n.children {
		if child.path == seg || (child.isParam && strings.HasPrefix(seg, ":")) ||
			(child.isWild && seg == "*") {
			if rest == "" {
				child.handler = &routeEndpoint{fn: h, paramKeys: append([]string(nil), paramKeys...)}
			} else {
				insert(child, rest, h, paramKeys)
			}
			return
		}
	}

	child := &node{path: seg}
	if strings.HasPrefix(seg, ":") {
		child.isParam = true
	} else if seg == "*" {
		child.isWild = true
	}
	n.children = append(n.children, child)

	if rest == "" {
		child.handler = &routeEndpoint{fn: h, paramKeys: append([]string(nil), paramKeys...)}
	} else {
		insert(child, rest, h, paramKeys)
	}
}

func match(n *node, path []byte, params *[]Param) HandlerFunc {
	if len(path) == 0 || (len(path) == 1 && path[0] == '/') {
		return endpointHandler(n.handler, params)
	}
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	seg, rest := splitPathBytes(path)

	// Static routes have precedence regardless of registration order.
	for _, child := range n.children {
		if child.isParam || child.isWild {
			continue
		}
		if strEqBytes(child.path, seg) {
			if len(rest) == 0 {
				if child.handler != nil {
					return endpointHandler(child.handler, params)
				}
			} else if h := match(child, rest, params); h != nil {
				return h
			}
		}
	}
	// Parameter branches may need to backtrack when a deeper segment fails.
	for _, child := range n.children {
		if !child.isParam {
			continue
		}
		mark := len(*params)
		*params = append(*params, Param{Value: b2s(seg)})
		if len(rest) == 0 {
			if child.handler != nil {
				return endpointHandler(child.handler, params)
			}
		} else if h := match(child, rest, params); h != nil {
			return h
		}
		*params = (*params)[:mark]
	}
	for _, child := range n.children {
		if child.isWild {
			mark := len(*params)
			*params = append(*params, Param{Value: b2s(path)})
			if child.handler != nil {
				return endpointHandler(child.handler, params)
			}
			*params = (*params)[:mark]
		}
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

func splitPath(path string) (seg, rest string) {
	i := strings.IndexByte(path, '/')
	if i < 0 {
		return path, ""
	}
	return path[:i], path[i:]
}

func splitPathBytes(path []byte) (seg, rest []byte) {
	for i, c := range path {
		if c == '/' {
			return path[:i], path[i:]
		}
	}
	return path, nil
}

// b2s converts []byte to string without allocation.
func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// strEqBytes reports whether a string and a byte slice are equal — zero alloc.
func strEqBytes(s string, b []byte) bool {
	if len(s) != len(b) {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] != b[i] {
			return false
		}
	}
	return true
}
