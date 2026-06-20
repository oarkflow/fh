package fasthttp

// Group is a set of routes sharing a common path prefix and middleware.
type Group struct {
	app        *App
	prefix     string
	middleware []HandlerFunc
}

// Use adds middleware scoped to this group.
func (g *Group) Use(handlers ...HandlerFunc) *Group {
	g.app.buildMu.Lock()
	defer g.app.buildMu.Unlock()
	g.app.assertMutable()
	g.middleware = append(g.middleware, handlers...)
	return g
}

// Group creates a sub-group under this group's prefix.
func (g *Group) Group(prefix string, handlers ...HandlerFunc) *Group {
	g.app.buildMu.Lock()
	defer g.app.buildMu.Unlock()
	g.app.assertMutable()
	return &Group{
		app:        g.app,
		prefix:     g.prefix + prefix,
		middleware: append(append([]HandlerFunc{}, g.middleware...), handlers...),
	}
}

func (g *Group) add(method, path string, handlers ...HandlerFunc) *Group {
	all := make([]HandlerFunc, 0, len(g.middleware)+len(handlers))
	all = append(all, g.middleware...)
	all = append(all, handlers...)
	g.app.Add(method, g.prefix+path, all...)
	return g
}

func (g *Group) Get(path string, handlers ...HandlerFunc) *Group {
	return g.add("GET", path, handlers...)
}

func (g *Group) Post(path string, handlers ...HandlerFunc) *Group {
	return g.add("POST", path, handlers...)
}

func (g *Group) Put(path string, handlers ...HandlerFunc) *Group {
	return g.add("PUT", path, handlers...)
}

func (g *Group) Delete(path string, handlers ...HandlerFunc) *Group {
	return g.add("DELETE", path, handlers...)
}

func (g *Group) Patch(path string, handlers ...HandlerFunc) *Group {
	return g.add("PATCH", path, handlers...)
}

func (g *Group) Head(path string, handlers ...HandlerFunc) *Group {
	return g.add("HEAD", path, handlers...)
}

func (g *Group) Options(path string, handlers ...HandlerFunc) *Group {
	return g.add("OPTIONS", path, handlers...)
}

func (g *Group) Connect(path string, handlers ...HandlerFunc) *Group {
	return g.add("CONNECT", path, handlers...)
}
func (g *Group) Trace(path string, handlers ...HandlerFunc) *Group {
	return g.add("TRACE", path, handlers...)
}

func (g *Group) All(path string, handlers ...HandlerFunc) *Group {
	for _, m := range []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE"} {
		g.add(m, path, handlers...)
	}
	return g
}
