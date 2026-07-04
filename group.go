package fh

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

func (g *Group) addTyped(method, path string, handler any, middleware ...HandlerFunc) *Group {
	all := make([]HandlerFunc, 0, len(g.middleware)+len(middleware))
	all = append(all, g.middleware...)
	all = append(all, middleware...)
	g.app.addTyped(method, g.prefix+path, handler, all...)
	return g
}

// Name names the most recently registered route in this group.
func (g *Group) Name(name string) *Group {
	g.app.Name(name)
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
func (g *Group) Query(path string, handlers ...HandlerFunc) *Group {
	return g.add("QUERY", path, handlers...)
}

func (g *Group) All(path string, handlers ...HandlerFunc) *Group {
	for _, m := range []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE", "QUERY"} {
		g.add(m, path, handlers...)
	}
	return g
}

func (g *Group) GetTyped(path string, handler any, middleware ...HandlerFunc) *Group {
	return g.addTyped("GET", path, handler, middleware...)
}
func (g *Group) HeadTyped(path string, handler any, middleware ...HandlerFunc) *Group {
	return g.addTyped("HEAD", path, handler, middleware...)
}
func (g *Group) PostTyped(path string, handler any, middleware ...HandlerFunc) *Group {
	return g.addTyped("POST", path, handler, middleware...)
}
func (g *Group) PutTyped(path string, handler any, middleware ...HandlerFunc) *Group {
	return g.addTyped("PUT", path, handler, middleware...)
}
func (g *Group) PatchTyped(path string, handler any, middleware ...HandlerFunc) *Group {
	return g.addTyped("PATCH", path, handler, middleware...)
}
func (g *Group) DeleteTyped(path string, handler any, middleware ...HandlerFunc) *Group {
	return g.addTyped("DELETE", path, handler, middleware...)
}
func (g *Group) OptionsTyped(path string, handler any, middleware ...HandlerFunc) *Group {
	return g.addTyped("OPTIONS", path, handler, middleware...)
}
func (g *Group) ConnectTyped(path string, handler any, middleware ...HandlerFunc) *Group {
	return g.addTyped("CONNECT", path, handler, middleware...)
}
func (g *Group) TraceTyped(path string, handler any, middleware ...HandlerFunc) *Group {
	return g.addTyped("TRACE", path, handler, middleware...)
}
func (g *Group) QueryTyped(path string, handler any, middleware ...HandlerFunc) *Group {
	return g.addTyped("QUERY", path, handler, middleware...)
}
func (g *Group) AllTyped(path string, handler any, middleware ...HandlerFunc) *Group {
	for _, m := range []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE", "QUERY"} {
		g.addTyped(m, path, handler, middleware...)
	}
	return g
}
