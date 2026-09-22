package platform

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Registry is the explicit trust boundary between configuration and code.
//
// BCL may select any registered resource kind, action and node type, and may
// configure them freely. It can never introduce native code, open a connection
// kind nobody registered, or execute an action nobody wrote. Everything an
// application author can reach is something a Go author deliberately put here.
//
// A Registry is also the platform's catalog: it knows the name, family,
// summary and configuration shape of every capability, which is what a visual
// builder needs to offer a palette without hard-coding one.
type Registry struct {
	mu        sync.RWMutex
	resources map[string]ResourceFactory
	actions   map[string]ActionFactory
	nodeTypes map[string]NodeTypeInfo
	edgeTypes map[string]EdgeTypeInfo
	kinds     map[string]ResourceKindInfo
	actionDoc map[string]ActionInfo
}

// NodeTypeInfo is catalog metadata for one semantic node family. Execution is
// always supplied by the node's registered `uses` action; a node type says what
// the node means, not how it runs.
type NodeTypeInfo struct {
	Name    string `json:"name"`
	Family  string `json:"family"`
	Summary string `json:"summary"`
	// DefaultAction is the action an editor should pre-select for this type.
	DefaultAction string `json:"default_action,omitempty"`
	// ResourceKinds lists the resource kind prefixes a node of this type
	// normally names, e.g. "database." — used to filter the resource picker.
	ResourceKinds []string `json:"resource_kinds,omitempty"`
	// Terminal marks a type that normally ends a graph or a process branch.
	Terminal bool `json:"terminal,omitempty"`
	// Durable marks a type that only makes sense inside a process, because it
	// parks and waits. Using one in a request intent is rejected at compile
	// time rather than silently blocking a request thread.
	Durable bool `json:"durable,omitempty"`
}

// EdgeTypeInfo is catalog metadata for one process edge type.
type EdgeTypeInfo struct {
	Name    string `json:"name"`
	Family  string `json:"family"`
	Summary string `json:"summary"`
	// Multi reports whether the type uses Sources/Targets rather than From/To.
	Multi bool `json:"multi,omitempty"`
	// Parks reports whether traversing this edge suspends the run.
	Parks bool `json:"parks,omitempty"`
	// ErrorPath reports that the edge fires on failure rather than success.
	ErrorPath bool `json:"error_path,omitempty"`
	// Fields lists the EdgeSpec fields this type actually reads, so an editor
	// shows four relevant inputs instead of thirty mostly-irrelevant ones.
	Fields []string `json:"fields,omitempty"`
}

// ResourceKindInfo documents one resource provider for the catalog.
type ResourceKindInfo struct {
	Name    string `json:"name"`
	Family  string `json:"family"`
	Summary string `json:"summary"`
	// Config documents the keys this provider reads.
	Config []ConfigField `json:"config,omitempty"`
	// Provides names the SPI contracts this provider satisfies, so the compiler
	// can tell an author that cache.file cannot back a rate limiter before the
	// deployment starts rather than after.
	Provides []string `json:"provides,omitempty"`
}

// ActionInfo documents one action for the catalog.
type ActionInfo struct {
	Name    string `json:"name"`
	Family  string `json:"family"`
	Summary string `json:"summary"`
	// ResourceKind is the resource family this action requires, empty when it
	// needs none.
	ResourceKind string        `json:"resource_kind,omitempty"`
	Config       []ConfigField `json:"config,omitempty"`
	// Provides describes what the action publishes, for editor hints.
	Provides string `json:"provides,omitempty"`
	// Kind is the REF node kind this action should normally declare.
	Kind string `json:"kind,omitempty"`
}

// ConfigField documents one configuration key.
type ConfigField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required,omitempty"`
	Summary  string `json:"summary,omitempty"`
	Default  string `json:"default,omitempty"`
}

// ---------------------------------------------------------------------------
// Process-wide driver registration
// ---------------------------------------------------------------------------

var (
	driverMu        sync.RWMutex
	driverResources = map[string]ResourceFactory{}
	driverActions   = map[string]ActionFactory{}
	driverKinds     = map[string]ResourceKindInfo{}
	driverActionDoc = map[string]ActionInfo{}
)

// RegisterResourceDriver installs a resource provider into every Registry
// created afterwards. This is the supported extension point: a host that wants
// Redis-backed caching, Kafka queues or S3 storage implements the matching
// contract in ref/platform/spi and registers it here, without this package
// growing a dependency on any client library.
//
//	platform.RegisterResourceDriver("cache.redis",
//	    platform.ResourceFactoryFunc(openRedisCache),
//	    platform.ResourceKindInfo{Family: "cache", Summary: "Redis cache"})
//
// Registering a kind twice panics, because two providers silently competing for
// one name is a configuration ambiguity no deployment should start with.
func RegisterResourceDriver(kind string, factory ResourceFactory, info ...ResourceKindInfo) {
	kind = normalizeName(kind)
	if kind == "" || factory == nil {
		panic("ref/platform: resource driver needs a kind and a factory")
	}
	driverMu.Lock()
	defer driverMu.Unlock()
	if _, exists := driverResources[kind]; exists {
		panic("ref/platform: duplicate resource driver " + kind)
	}
	driverResources[kind] = factory
	doc := ResourceKindInfo{Name: kind}
	if len(info) > 0 {
		doc = info[0]
		doc.Name = kind
	}
	driverKinds[kind] = doc
}

// RegisterActionDriver installs an action into every Registry created
// afterwards, for hosts that ship reusable domain actions alongside their
// connectors.
func RegisterActionDriver(name string, factory ActionFactory, info ...ActionInfo) {
	name = normalizeName(name)
	if name == "" || factory == nil {
		panic("ref/platform: action driver needs a name and a factory")
	}
	driverMu.Lock()
	defer driverMu.Unlock()
	if _, exists := driverActions[name]; exists {
		panic("ref/platform: duplicate action driver " + name)
	}
	driverActions[name] = factory
	doc := ActionInfo{Name: name}
	if len(info) > 0 {
		doc = info[0]
		doc.Name = name
	}
	driverActionDoc[name] = doc
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

// NewRegistry returns a registry seeded with every built-in node type, edge
// type, resource provider and action, plus whatever drivers the host
// registered with RegisterResourceDriver and RegisterActionDriver.
func NewRegistry() *Registry {
	r := &Registry{
		resources: make(map[string]ResourceFactory),
		actions:   make(map[string]ActionFactory),
		nodeTypes: make(map[string]NodeTypeInfo),
		edgeTypes: make(map[string]EdgeTypeInfo),
		kinds:     make(map[string]ResourceKindInfo),
		actionDoc: make(map[string]ActionInfo),
	}
	registerBuiltins(r)

	driverMu.RLock()
	defer driverMu.RUnlock()
	for kind, factory := range driverResources {
		if _, exists := r.resources[kind]; !exists {
			r.resources[kind] = factory
			r.kinds[kind] = driverKinds[kind]
		}
	}
	for name, factory := range driverActions {
		if _, exists := r.actions[name]; !exists {
			r.actions[name] = factory
			r.actionDoc[name] = driverActionDoc[name]
		}
	}
	return r
}

// NewEmptyRegistry returns a registry with nothing registered. Use it to build
// a locked-down generation that may only reach a hand-picked capability set —
// a tenant-authored application, for instance, which should not be able to open
// a database connection just because the platform can.
func NewEmptyRegistry() *Registry {
	return &Registry{
		resources: make(map[string]ResourceFactory),
		actions:   make(map[string]ActionFactory),
		nodeTypes: make(map[string]NodeTypeInfo),
		edgeTypes: make(map[string]EdgeTypeInfo),
		kinds:     make(map[string]ResourceKindInfo),
		actionDoc: make(map[string]ActionInfo),
	}
}

// RegisterNodeType adds catalog metadata for a semantic node family.
func (r *Registry) RegisterNodeType(info NodeTypeInfo) error {
	name := normalizeName(info.Name)
	if name == "" {
		return fmt.Errorf("ref/platform: node type is required")
	}
	info.Name = name
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.nodeTypes[name]; exists {
		return fmt.Errorf("ref/platform: duplicate node type %q", name)
	}
	r.nodeTypes[name] = info
	return nil
}

// RegisterEdgeType adds catalog metadata for a process edge type. Edge
// semantics are implemented by ref/process; this registers the name an author
// may write and the fields an editor should offer.
func (r *Registry) RegisterEdgeType(info EdgeTypeInfo) error {
	name := normalizeName(info.Name)
	if name == "" {
		return fmt.Errorf("ref/platform: edge type is required")
	}
	info.Name = name
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.edgeTypes[name]; exists {
		return fmt.Errorf("ref/platform: duplicate edge type %q", name)
	}
	r.edgeTypes[name] = info
	return nil
}

// RegisterResource installs a resource provider on this registry only.
func (r *Registry) RegisterResource(kind string, factory ResourceFactory, info ...ResourceKindInfo) error {
	kind = normalizeName(kind)
	if kind == "" || factory == nil {
		return fmt.Errorf("ref/platform: resource kind and factory are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.resources[kind]; exists {
		return fmt.Errorf("ref/platform: duplicate resource factory %q", kind)
	}
	r.resources[kind] = factory
	doc := ResourceKindInfo{Name: kind}
	if len(info) > 0 {
		doc = info[0]
		doc.Name = kind
	}
	r.kinds[kind] = doc
	return nil
}

// RegisterAction installs an action on this registry only.
func (r *Registry) RegisterAction(name string, factory ActionFactory, info ...ActionInfo) error {
	name = normalizeName(name)
	if name == "" || factory == nil {
		return fmt.Errorf("ref/platform: action kind and factory are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.actions[name]; exists {
		return fmt.Errorf("ref/platform: duplicate action factory %q", name)
	}
	r.actions[name] = factory
	doc := ActionInfo{Name: name}
	if len(info) > 0 {
		doc = info[0]
		doc.Name = name
	}
	r.actionDoc[name] = doc
	return nil
}

// Replace swaps an already-registered action for another. It is how a host
// overrides one built-in — a stricter password hasher, an audited HTTP client —
// without forking the catalog. Replacing something that was never registered is
// an error, so a typo does not quietly add a new capability instead.
func (r *Registry) Replace(name string, factory ActionFactory) error {
	name = normalizeName(name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.actions[name]; !exists {
		return fmt.Errorf("ref/platform: cannot replace unregistered action %q", name)
	}
	r.actions[name] = factory
	return nil
}

func (r *Registry) resource(kind string) (ResourceFactory, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.resources[normalizeName(kind)]
	return factory, ok
}

func (r *Registry) action(name string) (ActionFactory, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.actions[normalizeName(name)]
	return factory, ok
}

func (r *Registry) nodeType(name string) (NodeTypeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.nodeTypes[normalizeName(name)]
	return info, ok
}

// ---------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------

// Catalog is the machine-readable description of everything this registry can
// run. Serve it from an admin route and a visual builder can populate its whole
// palette — node types, edge types, connectors, actions and their config —
// without a second source of truth that drifts from the code.
type Catalog struct {
	NodeTypes     []NodeTypeInfo     `json:"node_types"`
	EdgeTypes     []EdgeTypeInfo     `json:"edge_types"`
	ResourceKinds []ResourceKindInfo `json:"resource_kinds"`
	Actions       []ActionInfo       `json:"actions"`
}

// Catalog returns the sorted catalog. Sorting is deliberate: a stable order
// makes the output diffable, so a review can see a capability appear or vanish.
func (r *Registry) Catalog() Catalog {
	r.mu.RLock()
	defer r.mu.RUnlock()

	catalog := Catalog{
		NodeTypes:     make([]NodeTypeInfo, 0, len(r.nodeTypes)),
		EdgeTypes:     make([]EdgeTypeInfo, 0, len(r.edgeTypes)),
		ResourceKinds: make([]ResourceKindInfo, 0, len(r.kinds)),
		Actions:       make([]ActionInfo, 0, len(r.actions)),
	}
	for _, info := range r.nodeTypes {
		catalog.NodeTypes = append(catalog.NodeTypes, info)
	}
	for _, info := range r.edgeTypes {
		catalog.EdgeTypes = append(catalog.EdgeTypes, info)
	}
	for _, info := range r.kinds {
		catalog.ResourceKinds = append(catalog.ResourceKinds, info)
	}
	for name := range r.actions {
		info, ok := r.actionDoc[name]
		if !ok || info.Name == "" {
			info = ActionInfo{Name: name}
		}
		catalog.Actions = append(catalog.Actions, info)
	}
	sort.Slice(catalog.NodeTypes, func(i, j int) bool { return catalog.NodeTypes[i].Name < catalog.NodeTypes[j].Name })
	sort.Slice(catalog.EdgeTypes, func(i, j int) bool { return catalog.EdgeTypes[i].Name < catalog.EdgeTypes[j].Name })
	sort.Slice(catalog.ResourceKinds, func(i, j int) bool { return catalog.ResourceKinds[i].Name < catalog.ResourceKinds[j].Name })
	sort.Slice(catalog.Actions, func(i, j int) bool { return catalog.Actions[i].Name < catalog.Actions[j].Name })
	return catalog
}

// ActionNames returns every registered action name, sorted. Useful in tests
// that assert the catalog has not silently lost a capability.
func (r *Registry) ActionNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.actions))
	for name := range r.actions {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ResourceKinds returns every registered resource kind, sorted.
func (r *Registry) ResourceKinds() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	kinds := make([]string, 0, len(r.resources))
	for kind := range r.resources {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

func normalizeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}
