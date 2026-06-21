package dagflow

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/oarkflow/bcl"
)

type Config struct {
	Server            ServerConfig       `bcl:"server,block,omitempty"`
	GlobalMiddlewares []string           `bcl:"global_middlewares,omitempty"`
	Conditions        []ConditionConfig  `bcl:"condition,block"`
	Middlewares       []MiddlewareConfig `bcl:"middleware,block"`
	Workflows         []WorkflowConfig   `bcl:"workflow,block"`
	Chains            []ChainConfig      `bcl:"chain,block"`
	Routes            []RouteConfig      `bcl:"route,block"`
	RouteGroups       []RouteGroupConfig `bcl:"route_group,block"`
	Schemas           []SchemaConfig     `bcl:"schema,block"`
	Scripts           []ScriptConfig     `bcl:"script,block"`
}

type ServerConfig struct {
	Address string `bcl:"address,omitempty"`
}

type ConditionConfig struct {
	ID          string   `bcl:",id"`
	Description string   `bcl:"description,omitempty"`
	Expr        string   `bcl:"expr,omitempty"`
	All         []string `bcl:"all,omitempty"`
	Any         []string `bcl:"any,omitempty"`
	None        []string `bcl:"none,omitempty"`
	Not         []string `bcl:"not,omitempty"`
}

type ConditionSpec struct {
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	Expr        string   `json:"expr,omitempty"`
	All         []string `json:"all,omitempty"`
	Any         []string `json:"any,omitempty"`
	None        []string `json:"none,omitempty"`
	Not         []string `json:"not,omitempty"`
}

type MiddlewareConfig struct {
	ID        string `bcl:",id"`
	Type      string `bcl:"type,ident"`
	Header    string `bcl:"header,omitempty"`
	Value     string `bcl:"value,omitempty"`
	Message   string `bcl:"message,omitempty"`
	Status    int    `bcl:"status,omitempty"`
	Limit     int    `bcl:"limit,omitempty"`
	Window    string `bcl:"window,omitempty"`
	MaxBytes  int64  `bcl:"max_bytes,omitempty"`
	When      string `bcl:"when,omitempty"`
	Condition string `bcl:"condition,omitempty"`
}

type WorkflowConfig struct {
	ID              string                  `bcl:",id"`
	Name            string                  `bcl:"name,omitempty"`
	Version         string                  `bcl:"version,omitempty"`
	First           string                  `bcl:"first"`
	Debug           bool                    `bcl:"debug,omitempty"`
	MaxVisits       int                     `bcl:"max_visits,omitempty"`
	Mode            RunMode                 `bcl:"mode,ident,omitempty"`
	MigrationPolicy WorkflowMigrationPolicy `bcl:"migration_policy,ident,omitempty"`
	Nodes           []NodeConfig            `bcl:"node,block"`
	Edges           []EdgeConfig            `bcl:"edge,block"`
}

type NodeConfig struct {
	ID              string               `bcl:",id"`
	Type            NodeType             `bcl:"type,ident"`
	Handler         string               `bcl:"handler,omitempty"`
	Workflow        string               `bcl:"workflow,omitempty"`
	Mode            RunMode              `bcl:"mode,ident,omitempty"`
	Await           *bool                `bcl:"await,omitempty"`
	Timeout         string               `bcl:"timeout,omitempty"`
	Retry           int                  `bcl:"retry,omitempty"`
	RetryPolicy     RetryPolicyConfig    `bcl:"retry_policy,block,omitempty"`
	Last            bool                 `bcl:"last,omitempty"`
	Pause           bool                 `bcl:"pause,omitempty"`
	When            string               `bcl:"when,omitempty"`
	Condition       string               `bcl:"condition,omitempty"`
	SkipOnFalse     bool                 `bcl:"skip_on_false,omitempty"`
	ContinueOnError bool                 `bcl:"continue_on_error,omitempty"`
	Compensate      string               `bcl:"compensate,omitempty"`
	OnError         string               `bcl:"on_error,omitempty"`
	OnTimeout       string               `bcl:"on_timeout,omitempty"`
	Pool            string               `bcl:"pool,omitempty"`
	Priority        int                  `bcl:"priority,omitempty"`
	RateLimit       RateLimitConfig      `bcl:"rate_limit,block,omitempty"`
	CircuitBreaker  CircuitBreakerConfig `bcl:"circuit_breaker,block,omitempty"`
	Params          map[string]any       `bcl:"params,omitempty"`
	Script          string               `bcl:"script,omitempty"`
	InputSchema     string               `bcl:"input_schema,omitempty"`
	OutputSchema    string               `bcl:"output_schema,omitempty"`
	FailurePolicy   FailurePolicyConfig  `bcl:"failure_policy,block,omitempty"`
}

type RetryPolicyConfig struct {
	MaxAttempts  int    `bcl:"max_attempts,omitempty"`
	Strategy     string `bcl:"strategy,omitempty"`
	InitialDelay string `bcl:"initial_delay,omitempty"`
	MaxDelay     string `bcl:"max_delay,omitempty"`
	Jitter       bool   `bcl:"jitter,omitempty"`
}

type RateLimitConfig struct {
	Limit  int    `bcl:"limit,omitempty"`
	Window string `bcl:"window,omitempty"`
}

type CircuitBreakerConfig struct {
	FailureThreshold int    `bcl:"failure_threshold,omitempty"`
	ResetAfter       string `bcl:"reset_after,omitempty"`
}

type EdgeConfig struct {
	ID             string            `bcl:",id"`
	From           string            `bcl:"from,omitempty"`
	To             string            `bcl:"to,omitempty"`
	Sources        []string          `bcl:"sources,omitempty"`
	Targets        []string          `bcl:"targets,omitempty"`
	Type           EdgeType          `bcl:"type,ident"`
	When           string            `bcl:"when,omitempty"`
	Condition      string            `bcl:"condition,omitempty"`
	Strategy       string            `bcl:"strategy,omitempty"`
	MaxConcurrency int               `bcl:"max_concurrency,omitempty"`
	FailFast       bool              `bcl:"fail_fast,omitempty"`
	Await          *bool             `bcl:"await,omitempty"`
	Timeout        string            `bcl:"timeout,omitempty"`
	Quorum         int               `bcl:"quorum,omitempty"`
	CancelLosers   bool              `bcl:"cancel_losers,omitempty"`
	Map            map[string]string `bcl:"map,omitempty"`
}

type ChainConfig struct {
	ID        string   `bcl:",id"`
	Name      string   `bcl:"name,omitempty"`
	Workflows []string `bcl:"workflows"`
	Debug     bool     `bcl:"debug,omitempty"`
	When      string   `bcl:"when,omitempty"`
	Condition string   `bcl:"condition,omitempty"`
}

type RouteConfig struct {
	ID           string    `bcl:",id" json:"id"`
	Method       string    `bcl:"method" json:"method"`
	Path         string    `bcl:"path" json:"path"`
	Workflow     string    `bcl:"workflow,omitempty" json:"workflow,omitempty"`
	Chain        string    `bcl:"chain,omitempty" json:"chain,omitempty"`
	Workflows    []string  `bcl:"workflows,omitempty" json:"workflows,omitempty"`
	Mode         RouteMode `bcl:"mode,ident,omitempty" json:"mode,omitempty"`
	Middlewares  []string  `bcl:"middlewares,omitempty" json:"middlewares,omitempty"`
	Envelope     bool      `bcl:"envelope,omitempty" json:"envelope,omitempty"`
	When         string    `bcl:"when,omitempty" json:"when,omitempty"`
	Condition    string    `bcl:"condition,omitempty" json:"condition,omitempty"`
	InputSchema  string    `bcl:"input_schema,omitempty" json:"input_schema,omitempty"`
	OutputSchema string    `bcl:"output_schema,omitempty" json:"output_schema,omitempty"`
	Tags         []string  `bcl:"tags,omitempty" json:"tags,omitempty"`
}

type FailurePolicyConfig struct {
	Strategy     string `bcl:"strategy,ident,omitempty"`
	ErrorNode    string `bcl:"error_node,omitempty"`
	FallbackNode string `bcl:"fallback_node,omitempty"`
}

type SchemaConfig struct {
	ID       string              `bcl:",id"`
	Type     string              `bcl:"type,ident,omitempty"`
	Required []string            `bcl:"required,omitempty"`
	Fields   []SchemaFieldConfig `bcl:"field,block"`
}

type SchemaFieldConfig struct {
	ID       string `bcl:",id"`
	Type     string `bcl:"type,ident"`
	Required bool   `bcl:"required,omitempty"`
	Format   string `bcl:"format,omitempty"`
}

type ScriptConfig struct {
	ID     string `bcl:",id"`
	Source string `bcl:"source"`
}

type RouteGroupConfig struct {
	ID          string             `bcl:",id"`
	Prefix      string             `bcl:"prefix,omitempty"`
	Middlewares []string           `bcl:"middlewares,omitempty"`
	Routes      []RouteConfig      `bcl:"route,block"`
	Groups      []RouteGroupConfig `bcl:"route_group,block"`
}

func LoadBCL(path string) (*Config, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return LoadBCLDir(path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return decodeBCL(data)
}

func LoadBCLDir(dir string) (*Config, error) {
	var files []string
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".bcl") || strings.HasSuffix(path, ".hcl") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no .bcl files found in %s", dir)
	}

	// Decode every file independently, then merge the fragments deterministically.
	// This avoids a real-world BCL/HCL edge case where one bad/large concatenated
	// document can silently drop later top-level blocks, which previously made
	// routes fail validation with "workflow <id> not found" even though the
	// workflow existed in another file. Per-file decoding also makes the error
	// message point at the exact file that failed.
	merged := &Config{}
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		frag, err := decodeBCL(data)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", f, err)
		}
		mergeConfig(merged, frag)
	}
	return merged, nil
}

func mergeConfig(dst, src *Config) {
	if src == nil {
		return
	}
	if src.Server.Address != "" {
		dst.Server = src.Server
	}
	dst.GlobalMiddlewares = append(dst.GlobalMiddlewares, src.GlobalMiddlewares...)
	dst.Conditions = append(dst.Conditions, src.Conditions...)
	dst.Middlewares = append(dst.Middlewares, src.Middlewares...)
	dst.Workflows = append(dst.Workflows, src.Workflows...)
	dst.Chains = append(dst.Chains, src.Chains...)
	dst.Routes = append(dst.Routes, src.Routes...)
	dst.RouteGroups = append(dst.RouteGroups, src.RouteGroups...)
	dst.Schemas = append(dst.Schemas, src.Schemas...)
	dst.Scripts = append(dst.Scripts, src.Scripts...)
}

func decodeBCL(data []byte) (*Config, error) {
	var cfg Config
	if err := bcl.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func ConfigHash(cfg *Config) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%#v", cfg)))
	return hex.EncodeToString(h[:])
}

func buildCondition(c ConditionConfig) ConditionSpec {
	return ConditionSpec{Name: c.ID, Description: c.Description, Expr: c.Expr, All: append([]string(nil), c.All...), Any: append([]string(nil), c.Any...), None: append([]string(nil), c.None...), Not: append([]string(nil), c.Not...)}
}
func buildChain(c ChainConfig) *Chain {
	return &Chain{ID: c.ID, Name: c.Name, Workflows: append([]string(nil), c.Workflows...), Debug: c.Debug, When: c.When, Condition: c.Condition}
}

func buildWorkflow(c WorkflowConfig) (*Workflow, error) {
	if c.ID == "" || c.First == "" {
		return nil, errors.New("workflow id and first are required")
	}
	if c.Mode == "" {
		c.Mode = ModeInline
	}
	wf := &Workflow{ID: c.ID, Name: c.Name, Version: c.Version, First: c.First, Debug: c.Debug, MaxVisits: c.MaxVisits, Mode: c.Mode, MigrationPolicy: c.MigrationPolicy, Nodes: map[string]*Node{}, Outgoing: map[string][]*Edge{}, Incoming: map[string][]*Edge{}, FanIn: map[string][]*Edge{}, Metadata: map[string]any{}}
	if wf.MaxVisits <= 0 {
		wf.MaxVisits = 256
	}
	for _, nc := range c.Nodes {
		if nc.ID == "" {
			return nil, fmt.Errorf("workflow %s has node without id", c.ID)
		}
		if _, exists := wf.Nodes[nc.ID]; exists {
			return nil, fmt.Errorf("workflow %s duplicate node %s", c.ID, nc.ID)
		}
		d, err := parseDuration(nc.Timeout)
		if err != nil {
			return nil, fmt.Errorf("node %s timeout: %w", nc.ID, err)
		}
		if nc.Type == "" {
			nc.Type = NodeFunction
		}
		if nc.Mode == "" {
			nc.Mode = wf.Mode
		}
		await := true
		if nc.Await != nil {
			await = *nc.Await
		}
		if nc.Mode != ModeInline && nc.Mode != ModeBackground && nc.Mode != ModeDistributed {
			return nil, fmt.Errorf("node %s unsupported mode %q", nc.ID, nc.Mode)
		}
		if nc.Type == NodeWorkflow && nc.Workflow == "" {
			return nil, fmt.Errorf("workflow node %s requires workflow", nc.ID)
		}
		if (nc.Type == NodeFunction || nc.Type == NodePage) && nc.Handler == "" && !nc.Pause && nc.Type != NodePage {
			return nil, fmt.Errorf("node %s requires handler", nc.ID)
		}
		if nc.Type == NodePage && nc.Handler == "" {
			nc.Pause = true
		}
		rp, err := buildRetryPolicy(nc.Retry, nc.RetryPolicy)
		if err != nil {
			return nil, fmt.Errorf("node %s retry_policy: %w", nc.ID, err)
		}
		var rl *RateLimitPolicy
		if nc.RateLimit.Limit > 0 {
			wd, err := parseDuration(nc.RateLimit.Window)
			if err != nil {
				return nil, err
			}
			if wd <= 0 {
				wd = time.Minute
			}
			rl = &RateLimitPolicy{Limit: nc.RateLimit.Limit, Window: wd}
		}
		var cb *CircuitBreakerPolicy
		if nc.CircuitBreaker.FailureThreshold > 0 {
			rd, err := parseDuration(nc.CircuitBreaker.ResetAfter)
			if err != nil {
				return nil, err
			}
			if rd <= 0 {
				rd = 30 * time.Second
			}
			cb = &CircuitBreakerPolicy{FailureThreshold: nc.CircuitBreaker.FailureThreshold, ResetAfter: rd}
		}
		wf.Nodes[nc.ID] = &Node{ID: nc.ID, Type: nc.Type, Handler: nc.Handler, Workflow: nc.Workflow, Mode: nc.Mode, Await: await, Timeout: d, Retry: nc.Retry, RetryPolicy: rp, Last: nc.Last, Pause: nc.Pause, When: nc.When, Condition: nc.Condition, SkipOnFalse: nc.SkipOnFalse, ContinueOnError: nc.ContinueOnError, Compensate: nc.Compensate, OnError: nc.OnError, OnTimeout: nc.OnTimeout, Pool: nc.Pool, Priority: nc.Priority, RateLimit: rl, CircuitBreaker: cb, Params: nc.Params, Script: nc.Script, InputSchema: nc.InputSchema, OutputSchema: nc.OutputSchema, FailurePolicy: FailurePolicy{Strategy: nc.FailurePolicy.Strategy, ErrorNode: nc.FailurePolicy.ErrorNode, FallbackNode: nc.FailurePolicy.FallbackNode}}
	}
	if wf.Nodes[wf.First] == nil {
		return nil, fmt.Errorf("workflow %s first node %s not found", c.ID, c.First)
	}
	seen := map[string]bool{}
	for i, ec := range c.Edges {
		if ec.ID == "" {
			ec.ID = fmt.Sprintf("edge_%d", i)
		}
		if seen[ec.ID] {
			return nil, fmt.Errorf("workflow %s duplicate edge %s", c.ID, ec.ID)
		}
		seen[ec.ID] = true
		if ec.Type == "" {
			ec.Type = EdgeSimple
		}
		await := true
		if ec.Await != nil {
			await = *ec.Await
		}
		timeout, err := parseDuration(ec.Timeout)
		if err != nil {
			return nil, fmt.Errorf("edge %s timeout: %w", ec.ID, err)
		}
		sources := append([]string(nil), ec.Sources...)
		if ec.From != "" {
			sources = append(sources, ec.From)
		}
		targets := append([]string(nil), ec.Targets...)
		if ec.To != "" {
			targets = append(targets, ec.To)
		}
		if len(sources) == 0 || len(targets) == 0 {
			return nil, fmt.Errorf("edge %s requires from/to or sources/targets", ec.ID)
		}
		for _, s := range sources {
			if wf.Nodes[s] == nil {
				return nil, fmt.Errorf("edge %s references missing source node %s", ec.ID, s)
			}
		}
		for _, t := range targets {
			if wf.Nodes[t] == nil {
				return nil, fmt.Errorf("edge %s references missing target node %s", ec.ID, t)
			}
		}
		if !supportedEdge(ec.Type) {
			return nil, fmt.Errorf("edge %s unsupported type %q", ec.ID, ec.Type)
		}
		edge := &Edge{ID: ec.ID, From: sources[0], To: targets[0], Sources: sources, Targets: targets, Type: ec.Type, When: ec.When, Condition: ec.Condition, Strategy: ec.Strategy, MaxConcurrency: ec.MaxConcurrency, FailFast: ec.FailFast, Await: await, Timeout: timeout, Quorum: ec.Quorum, CancelLosers: ec.CancelLosers, Map: ec.Map}
		wf.Edges = append(wf.Edges, edge)
		for _, s := range sources {
			wf.Outgoing[s] = append(wf.Outgoing[s], edge)
		}
		for _, t := range targets {
			wf.Incoming[t] = append(wf.Incoming[t], edge)
		}
		if edge.Type == EdgeFanIn || edge.Type == EdgeJoin {
			for _, s := range sources {
				wf.FanIn[s] = append(wf.FanIn[s], edge)
			}
		}
	}
	return wf, nil
}

func supportedEdge(t EdgeType) bool {
	switch t {
	case EdgeSimple, EdgeBranch, EdgeIterator, EdgeError, EdgeFanOut, EdgeFanIn, EdgeParallel, EdgeJoin, EdgeRace, EdgeFallback, EdgeCompensate, EdgeTimeout, EdgeRetry:
		return true
	}
	return false
}

func buildRetryPolicy(legacy int, c RetryPolicyConfig) (RetryPolicy, error) {
	maxAttempts := c.MaxAttempts
	if maxAttempts <= 0 && legacy > 0 {
		maxAttempts = legacy + 1
	}
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	strategy := c.Strategy
	if strategy == "" {
		strategy = "fixed"
	}
	initial, err := parseDuration(c.InitialDelay)
	if err != nil {
		return RetryPolicy{}, err
	}
	maxd, err := parseDuration(c.MaxDelay)
	if err != nil {
		return RetryPolicy{}, err
	}
	if initial <= 0 {
		initial = 50 * time.Millisecond
	}
	if maxd <= 0 {
		maxd = 5 * time.Second
	}
	return RetryPolicy{MaxAttempts: maxAttempts, Strategy: strategy, InitialDelay: initial, MaxDelay: maxd, Jitter: c.Jitter}, nil
}

func FlattenRoutes(cfg *Config) []RouteConfig {
	out := append([]RouteConfig(nil), cfg.Routes...)
	for _, g := range cfg.RouteGroups {
		out = append(out, flattenGroup(g, "", nil)...)
	}
	return out
}

func flattenGroup(g RouteGroupConfig, parentPrefix string, parentMW []string) []RouteConfig {
	prefix := joinPath(parentPrefix, g.Prefix)
	mw := append(append([]string(nil), parentMW...), g.Middlewares...)
	var out []RouteConfig
	for _, r := range g.Routes {
		r.Path = joinPath(prefix, r.Path)
		r.Middlewares = append(append([]string(nil), mw...), r.Middlewares...)
		if r.ID == "" && g.ID != "" {
			r.ID = g.ID + ":" + r.Method + ":" + r.Path
		}
		out = append(out, r)
	}
	for _, child := range g.Groups {
		out = append(out, flattenGroup(child, prefix, mw)...)
	}
	return out
}

func joinPath(a, b string) string {
	if a == "" {
		a = "/"
	}
	if b == "" {
		b = "/"
	}
	p := strings.TrimRight(a, "/") + "/" + strings.TrimLeft(b, "/")
	if p == "/" {
		return p
	}
	return strings.TrimRight(p, "/")
}

func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}
