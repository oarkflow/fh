package dagflow

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/oarkflow/bcl"
)

type Config struct {
	Server            ServerConfig                `bcl:"server,block,omitempty"`
	GlobalMiddlewares []string                    `bcl:"global_middlewares,omitempty"`
	Conditions        []ConditionConfig           `bcl:"condition,block"`
	Middlewares       []MiddlewareConfig          `bcl:"middleware,block"`
	Workflows         []WorkflowConfig            `bcl:"workflow,block"`
	Chains            []ChainConfig               `bcl:"chain,block"`
	Routes            []RouteConfig               `bcl:"route,block"`
	RouteGroups       []RouteGroupConfig          `bcl:"route_group,block"`
	Schemas           []SchemaConfig              `bcl:"schema,block"`
	Scripts           []ScriptConfig              `bcl:"script,block"`
	Notifications     []NotificationChannelConfig `bcl:"notification_channel,block"`
	Queues            []QueueConfig               `bcl:"queue,block"`
	Consumers         []QueueConsumerConfig       `bcl:"consumer,block"`
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
	ID              string                   `bcl:",id"`
	Name            string                   `bcl:"name,omitempty"`
	Version         string                   `bcl:"version,omitempty"`
	First           string                   `bcl:"first"`
	Debug           bool                     `bcl:"debug,omitempty"`
	MaxVisits       int                      `bcl:"max_visits,omitempty"`
	Mode            RunMode                  `bcl:"mode,ident,omitempty"`
	MigrationPolicy WorkflowMigrationPolicy  `bcl:"migration_policy,ident,omitempty"`
	InputData       DataConfig               `bcl:"input_data,block,omitempty"`
	OutputData      DataConfig               `bcl:"output_data,block,omitempty"`
	Notifications   []NotificationRuleConfig `bcl:"notification,block"`
	Rules           []TaskRuleConfig         `bcl:"rule,block"`
	Nodes           []NodeConfig             `bcl:"node,block"`
	Edges           []EdgeConfig             `bcl:"edge,block"`
}

type NodeConfig struct {
	ID              string                   `bcl:",id"`
	Type            NodeType                 `bcl:"type,ident"`
	Handler         string                   `bcl:"handler,omitempty"`
	Workflow        string                   `bcl:"workflow,omitempty"`
	Mode            RunMode                  `bcl:"mode,ident,omitempty"`
	Await           *bool                    `bcl:"await,omitempty"`
	Timeout         string                   `bcl:"timeout,omitempty"`
	Retry           int                      `bcl:"retry,omitempty"`
	RetryPolicy     RetryPolicyConfig        `bcl:"retry_policy,block,omitempty"`
	Last            bool                     `bcl:"last,omitempty"`
	Pause           bool                     `bcl:"pause,omitempty"`
	When            string                   `bcl:"when,omitempty"`
	Condition       string                   `bcl:"condition,omitempty"`
	SkipOnFalse     bool                     `bcl:"skip_on_false,omitempty"`
	ContinueOnError bool                     `bcl:"continue_on_error,omitempty"`
	Compensate      string                   `bcl:"compensate,omitempty"`
	OnError         string                   `bcl:"on_error,omitempty"`
	OnTimeout       string                   `bcl:"on_timeout,omitempty"`
	Pool            string                   `bcl:"pool,omitempty"`
	Priority        int                      `bcl:"priority,omitempty"`
	RateLimit       RateLimitConfig          `bcl:"rate_limit,block,omitempty"`
	CircuitBreaker  CircuitBreakerConfig     `bcl:"circuit_breaker,block,omitempty"`
	Params          map[string]any           `bcl:"params,omitempty"`
	Script          string                   `bcl:"script,omitempty"`
	InputSchema     string                   `bcl:"input_schema,omitempty"`
	OutputSchema    string                   `bcl:"output_schema,omitempty"`
	FailurePolicy   FailurePolicyConfig      `bcl:"failure_policy,block,omitempty"`
	InputData       DataConfig               `bcl:"input_data,block,omitempty"`
	OutputData      DataConfig               `bcl:"output_data,block,omitempty"`
	Notifications   []NotificationRuleConfig `bcl:"notification,block"`
	Rules           []TaskRuleConfig         `bcl:"rule,block"`
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
	Data           DataConfig        `bcl:"data,block,omitempty"`
}

type DataConfig struct {
	Source       string                `bcl:"source,omitempty" json:"source,omitempty"`
	Map          map[string]string     `bcl:"map,omitempty" json:"map,omitempty"`
	Set          map[string]any        `bcl:"set,omitempty" json:"set,omitempty"`
	Defaults     map[string]any        `bcl:"defaults,omitempty" json:"defaults,omitempty"`
	Env          map[string]string     `bcl:"env,omitempty" json:"env,omitempty"`
	Services     map[string]string     `bcl:"services,omitempty" json:"services,omitempty"`
	Integrations map[string]string     `bcl:"integrations,omitempty" json:"integrations,omitempty"`
	Pick         []string              `bcl:"pick,omitempty" json:"pick,omitempty"`
	Omit         []string              `bcl:"omit,omitempty" json:"omit,omitempty"`
	Rename       map[string]string     `bcl:"rename,omitempty" json:"rename,omitempty"`
	Transforms   []DataTransformConfig `bcl:"transform,block" json:"transforms,omitempty"`
	Filters      []DataFilterConfig    `bcl:"filter,block" json:"filters,omitempty"`
	Append       map[string]string     `bcl:"append,omitempty" json:"append,omitempty"`
	Prepend      map[string]string     `bcl:"prepend,omitempty" json:"prepend,omitempty"`
	Flatten      []string              `bcl:"flatten,omitempty" json:"flatten,omitempty"`
	Strict       bool                  `bcl:"strict,omitempty" json:"strict,omitempty"`
}

type DataTransformConfig struct {
	Field string `bcl:"field,omitempty" json:"field,omitempty"`
	Expr  string `bcl:"expr,omitempty" json:"expr,omitempty"`
	Op    string `bcl:"op,ident,omitempty" json:"op,omitempty"`
	Arg   string `bcl:"arg,omitempty" json:"arg,omitempty"`
}

type DataFilterConfig struct {
	Expr string `bcl:"expr,omitempty" json:"expr,omitempty"`
	Mode string `bcl:"mode,ident,omitempty" json:"mode,omitempty"`
}

type NotificationChannelConfig = NotificationChannel

type NotificationRuleConfig struct {
	ID        string            `bcl:",id" json:"id"`
	Enabled   *bool             `bcl:"enabled,omitempty" json:"enabled,omitempty"`
	Events    []string          `bcl:"events,omitempty" json:"events,omitempty"`
	Channels  []string          `bcl:"channels,omitempty" json:"channels,omitempty"`
	When      string            `bcl:"when,omitempty" json:"when,omitempty"`
	Condition string            `bcl:"condition,omitempty" json:"condition,omitempty"`
	Title     string            `bcl:"title,omitempty" json:"title,omitempty"`
	Message   string            `bcl:"message,omitempty" json:"message,omitempty"`
	Severity  string            `bcl:"severity,omitempty" json:"severity,omitempty"`
	Data      DataConfig        `bcl:"data,block,omitempty" json:"data,omitempty"`
	Headers   map[string]string `bcl:"headers,omitempty" json:"headers,omitempty"`
}

type TaskRuleConfig struct {
	ID        string           `bcl:",id" json:"id"`
	Enabled   *bool            `bcl:"enabled,omitempty" json:"enabled,omitempty"`
	Events    []string         `bcl:"events,omitempty" json:"events,omitempty"`
	When      string           `bcl:"when,omitempty" json:"when,omitempty"`
	Condition string           `bcl:"condition,omitempty" json:"condition,omitempty"`
	Action    TaskActionConfig `bcl:"action,block" json:"action"`
	Message   string           `bcl:"message,omitempty" json:"message,omitempty"`
	Data      DataConfig       `bcl:"data,block,omitempty" json:"data,omitempty"`
}

type ChainConfig struct {
	ID        string   `bcl:",id"`
	Name      string   `bcl:"name,omitempty"`
	Workflows []string `bcl:"workflows"`
	Debug     bool     `bcl:"debug,omitempty"`
	When      string   `bcl:"when,omitempty"`
	Condition string   `bcl:"condition,omitempty"`
}

type ResponseConfig struct {
	Status  int               `bcl:"status,omitempty" json:"status,omitempty"`
	Header  map[string]string `bcl:"header,omitempty" json:"header,omitempty"`
	Headers map[string]string `bcl:"headers,omitempty" json:"headers,omitempty"`
	Data    DataConfig        `bcl:"data,block,omitempty" json:"data,omitempty"`
}

type RouteConfig struct {
	ID           string         `bcl:",id" json:"id"`
	Method       string         `bcl:"method" json:"method"`
	Path         string         `bcl:"path" json:"path"`
	Workflow     string         `bcl:"workflow,omitempty" json:"workflow,omitempty"`
	Queue        string         `bcl:"queue,omitempty" json:"queue,omitempty"`
	Chain        string         `bcl:"chain,omitempty" json:"chain,omitempty"`
	Workflows    []string       `bcl:"workflows,omitempty" json:"workflows,omitempty"`
	Mode         RouteMode      `bcl:"mode,ident,omitempty" json:"mode,omitempty"`
	Middlewares  []string       `bcl:"middlewares,omitempty" json:"middlewares,omitempty"`
	Envelope     bool           `bcl:"envelope,omitempty" json:"envelope,omitempty"`
	When         string         `bcl:"when,omitempty" json:"when,omitempty"`
	Condition    string         `bcl:"condition,omitempty" json:"condition,omitempty"`
	InputSchema  string         `bcl:"input_schema,omitempty" json:"input_schema,omitempty"`
	OutputSchema string         `bcl:"output_schema,omitempty" json:"output_schema,omitempty"`
	Tags         []string       `bcl:"tags,omitempty" json:"tags,omitempty"`
	Data         DataConfig     `bcl:"data,block,omitempty" json:"data,omitempty"`
	Response     ResponseConfig `bcl:"response,block,omitempty" json:"response,omitempty"`
	ResponseData DataConfig     `bcl:"response_data,block,omitempty" json:"response_data,omitempty"`
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
	cfg, err := decodeBCL(data)
	if err != nil {
		return nil, err
	}
	supplementConfigFromBCL(path, data, cfg)
	return cfg, nil
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
		supplementConfigFromBCL(f, data, frag)
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
	dst.Notifications = append(dst.Notifications, src.Notifications...)
	dst.Queues = append(dst.Queues, src.Queues...)
	dst.Consumers = append(dst.Consumers, src.Consumers...)
}

func decodeBCL(data []byte) (*Config, error) {
	var cfg Config
	if err := bcl.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func supplementConfigFromBCL(filename string, data []byte, cfg *Config) {
	if cfg == nil || len(data) == 0 {
		return
	}
	supplementTaskRulesFromBCL(data, cfg)
	for _, sc := range parseSchemaBlocks(data) {
		merged := false
		for i := range cfg.Schemas {
			if cfg.Schemas[i].ID == sc.ID {
				if cfg.Schemas[i].Type == "" {
					cfg.Schemas[i].Type = sc.Type
				}
				if len(cfg.Schemas[i].Required) == 0 {
					cfg.Schemas[i].Required = sc.Required
				}
				if len(cfg.Schemas[i].Fields) == 0 {
					cfg.Schemas[i].Fields = sc.Fields
				}
				merged = true
				break
			}
		}
		if !merged {
			cfg.Schemas = append(cfg.Schemas, sc)
		}
	}
}

func supplementTaskRulesFromBCL(data []byte, cfg *Config) {
	if cfg == nil {
		return
	}
	text := string(data)
	workflowBlocks := extractNamedBlocks(text, "workflow")
	for _, wb := range workflowBlocks {
		wf := findWorkflowConfig(cfg, wb.name)
		if wf == nil {
			continue
		}
		for _, rb := range extractDirectNamedBlocks(wb.body, "rule") {
			rc := parseTaskRuleBlock(rb)
			mergeWorkflowRuleConfig(wf, rc)
		}
		for _, nb := range extractDirectNamedBlocks(wb.body, "node") {
			nc := findNodeConfig(wf, nb.name)
			if nc == nil {
				continue
			}
			for _, rb := range extractDirectNamedBlocks(nb.body, "rule") {
				rc := parseTaskRuleBlock(rb)
				mergeNodeRuleConfig(nc, rc)
			}
		}
	}
}

func findWorkflowConfig(cfg *Config, id string) *WorkflowConfig {
	for i := range cfg.Workflows {
		if cfg.Workflows[i].ID == id {
			return &cfg.Workflows[i]
		}
	}
	return nil
}

func findNodeConfig(wf *WorkflowConfig, id string) *NodeConfig {
	if wf == nil {
		return nil
	}
	for i := range wf.Nodes {
		if wf.Nodes[i].ID == id {
			return &wf.Nodes[i]
		}
	}
	return nil
}

func mergeWorkflowRuleConfig(wf *WorkflowConfig, rc TaskRuleConfig) {
	if wf == nil || rc.ID == "" {
		return
	}
	for i := range wf.Rules {
		if wf.Rules[i].ID == rc.ID {
			mergeTaskRuleConfig(&wf.Rules[i], rc)
			return
		}
	}
	wf.Rules = append(wf.Rules, rc)
}

func mergeNodeRuleConfig(nc *NodeConfig, rc TaskRuleConfig) {
	if nc == nil || rc.ID == "" {
		return
	}
	for i := range nc.Rules {
		if nc.Rules[i].ID == rc.ID {
			mergeTaskRuleConfig(&nc.Rules[i], rc)
			return
		}
	}
	nc.Rules = append(nc.Rules, rc)
}

func mergeTaskRuleConfig(dst *TaskRuleConfig, src TaskRuleConfig) {
	if dst == nil {
		return
	}
	if src.When != "" {
		dst.When = src.When
	}
	if src.Condition != "" {
		dst.Condition = src.Condition
	}
	if src.Message != "" {
		dst.Message = src.Message
	}
	if len(src.Events) > 0 {
		dst.Events = src.Events
	}
	if src.Enabled != nil {
		dst.Enabled = src.Enabled
	}
	if src.Action.Type != "" {
		dst.Action = src.Action
	}
}

func parseTaskRuleBlock(b namedBCLBlock) TaskRuleConfig {
	rc := TaskRuleConfig{ID: b.name}
	rc.When = parseBCLStringLikeLine(b.body, "when")
	rc.Condition = parseBCLStringLikeLine(b.body, "condition")
	rc.Message = parseBCLStringLikeLine(b.body, "message")
	if ev := parseBCLStringArrayLine(b.body, "events"); len(ev) > 0 {
		rc.Events = ev
	}
	if m := boolLineRE("enabled").FindStringSubmatch(b.body); len(m) == 2 {
		v := strings.EqualFold(m[1], "true")
		rc.Enabled = &v
	}
	if abs := extractDirectNamedBlocks(b.body, "action"); len(abs) > 0 {
		rc.Action = parseTaskActionBlock(abs[0].body)
	} else if abody, ok := extractDirectAnonymousBlock(b.body, "action"); ok {
		rc.Action = parseTaskActionBlock(abody)
	}
	return rc
}

func parseTaskActionBlock(body string) TaskActionConfig {
	return TaskActionConfig{
		Type:      TaskActionType(parseBCLIdentOrStringLine(body, "type")),
		Channels:  parseBCLStringArrayLine(body, "channels"),
		Reason:    parseBCLStringLikeLine(body, "reason"),
		Mode:      parseBCLIdentOrStringLine(body, "mode"),
		Approvers: parseBCLStringArrayLine(body, "approvers"),
		Timeout:   parseBCLStringLikeLine(body, "timeout"),
	}
}

func parseBCLStringLikeLine(body, key string) string {
	lines := strings.Split(body, "\n")
	prefix := key + " "
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, prefix) {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(trimmed, prefix))
		return normalizeBCLExpr(value)
	}
	return ""
}

func parseBCLIdentOrStringLine(body, key string) string {
	v := parseBCLStringLikeLine(body, key)
	return strings.Trim(v, `"`)
}

func parseBCLStringArrayLine(body, key string) []string {
	if m := arrayLineRE(key).FindStringSubmatch(body); len(m) == 2 {
		return parseStringArray(m[1])
	}
	return nil
}

func extractDirectNamedBlocks(text, kind string) []namedBCLBlock {
	var out []namedBCLBlock
	forEachDirectBlock(text, func(name, blockKind, body string) {
		if blockKind == kind && name != "" {
			out = append(out, namedBCLBlock{name: name, body: body})
		}
	})
	return out
}

func extractDirectAnonymousBlock(text, kind string) (string, bool) {
	var found string
	ok := false
	forEachDirectBlock(text, func(name, blockKind, body string) {
		if !ok && blockKind == kind && name == "" {
			found = body
			ok = true
		}
	})
	return found, ok
}

func forEachDirectBlock(text string, fn func(name, kind, body string)) {
	depth := 0
	inString := false
	inRaw := false
	esc := false
	for i := 0; i < len(text); i++ {
		c := text[i]
		if inRaw {
			if c == '`' {
				inRaw = false
			}
			continue
		}
		if inString {
			if esc {
				esc = false
				continue
			}
			if c == '\\' {
				esc = true
				continue
			}
			if c == '"' {
				inString = false
			}
			continue
		}
		switch c {
		case '`':
			inRaw = true
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && (isIdentStart(c)) && (i == 0 || !isIdentPart(text[i-1])) {
				kindStart := i
				j := i + 1
				for j < len(text) && isIdentPart(text[j]) {
					j++
				}
				kind := text[kindStart:j]
				k := skipSpaces(text, j)
				name := ""
				if k < len(text) && text[k] == '"' {
					nameStart := k + 1
					nameEnd := scanQuotedStringEnd(text, k)
					if nameEnd < 0 {
						continue
					}
					name = text[nameStart:nameEnd]
					k = skipSpaces(text, nameEnd+1)
				}
				if k < len(text) && text[k] == '{' {
					close := matchingBrace(text, k)
					if close >= 0 {
						fn(name, kind, text[k+1:close])
						i = close
					}
				}
			}
		}
	}
}

func skipSpaces(text string, i int) int {
	for i < len(text) && (text[i] == ' ' || text[i] == '\t' || text[i] == '\r' || text[i] == '\n') {
		i++
	}
	return i
}

func scanQuotedStringEnd(text string, quote int) int {
	esc := false
	for i := quote + 1; i < len(text); i++ {
		if esc {
			esc = false
			continue
		}
		if text[i] == '\\' {
			esc = true
			continue
		}
		if text[i] == '"' {
			return i
		}
	}
	return -1
}

func isIdentStart(c byte) bool {
	return c == '_' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z'
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || c >= '0' && c <= '9' || c == '-' || c == '_'
}

func parseSchemaBlocks(data []byte) []SchemaConfig {
	text := string(data)
	blocks := extractNamedBlocks(text, "schema")
	out := make([]SchemaConfig, 0, len(blocks))
	for _, b := range blocks {
		sc := SchemaConfig{ID: b.name}
		if m := identLineRE("type").FindStringSubmatch(b.body); len(m) == 2 {
			sc.Type = strings.Trim(m[1], `"`)
		}
		if m := arrayLineRE("required").FindStringSubmatch(b.body); len(m) == 2 {
			sc.Required = parseStringArray(m[1])
		}
		for _, fb := range extractNamedBlocks(b.body, "field") {
			fc := SchemaFieldConfig{ID: fb.name}
			if m := identLineRE("type").FindStringSubmatch(fb.body); len(m) == 2 {
				fc.Type = strings.Trim(m[1], `"`)
			}
			if m := boolLineRE("required").FindStringSubmatch(fb.body); len(m) == 2 {
				fc.Required = strings.EqualFold(m[1], "true")
			}
			if m := stringLineRE("format").FindStringSubmatch(fb.body); len(m) == 2 {
				fc.Format = m[1]
			}
			sc.Fields = append(sc.Fields, fc)
		}
		if sc.ID != "" {
			out = append(out, sc)
		}
	}
	return out
}

type namedBCLBlock struct {
	name string
	body string
}

func extractNamedBlocks(text, kind string) []namedBCLBlock {
	var out []namedBCLBlock
	needle := kind + " \""
	for off := 0; ; {
		i := strings.Index(text[off:], needle)
		if i < 0 {
			break
		}
		start := off + i + len(needle)
		endName := strings.IndexByte(text[start:], '"')
		if endName < 0 {
			break
		}
		name := text[start : start+endName]
		braceSearch := start + endName + 1
		braceRel := strings.IndexByte(text[braceSearch:], '{')
		if braceRel < 0 {
			break
		}
		open := braceSearch + braceRel
		close := matchingBrace(text, open)
		if close < 0 {
			break
		}
		out = append(out, namedBCLBlock{name: name, body: text[open+1 : close]})
		off = close + 1
	}
	return out
}

func matchingBrace(text string, open int) int {
	depth := 0
	inString := false
	inRaw := false
	esc := false
	for i := open; i < len(text); i++ {
		c := text[i]
		if inRaw {
			if c == '`' {
				inRaw = false
			}
			continue
		}
		if inString {
			if esc {
				esc = false
				continue
			}
			if c == '\\' {
				esc = true
				continue
			}
			if c == '"' {
				inString = false
			}
			continue
		}
		switch c {
		case '`':
			inRaw = true
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func identLineRE(key string) *regexp.Regexp {
	return regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(key) + `\s+([A-Za-z0-9_\-.]+|"[^"]*")\s*$`)
}
func stringLineRE(key string) *regexp.Regexp {
	return regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(key) + `\s+"([^"]*)"\s*$`)
}
func boolLineRE(key string) *regexp.Regexp {
	return regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(key) + `\s+(true|false)\s*$`)
}
func arrayLineRE(key string) *regexp.Regexp {
	return regexp.MustCompile(`(?ms)^\s*` + regexp.QuoteMeta(key) + `\s+\[(.*?)\]`)
}

func parseStringArray(raw string) []string {
	var out []string
	for _, m := range regexp.MustCompile(`"([^"]*)"`).FindAllStringSubmatch(raw, -1) {
		out = append(out, m[1])
	}
	return out
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

func buildNotificationRule(c NotificationRuleConfig) NotificationRule {
	return NotificationRule{ID: c.ID, Enabled: c.Enabled, Events: append([]string(nil), c.Events...), Channels: append([]string(nil), c.Channels...), When: c.When, Condition: c.Condition, Title: c.Title, Message: c.Message, Severity: c.Severity, Data: buildDataSpec(c.Data), Headers: c.Headers}
}

func buildTaskRule(c TaskRuleConfig) TaskRule {
	return TaskRule{ID: c.ID, Enabled: c.Enabled, Events: append([]string(nil), c.Events...), When: c.When, Condition: c.Condition, Action: c.Action, Message: c.Message, Data: buildDataSpec(c.Data)}
}

func buildDataSpec(c DataConfig) DataSpec {
	out := DataSpec{Source: c.Source, Map: c.Map, Set: c.Set, Defaults: c.Defaults, Env: c.Env, Services: c.Services, Integrations: c.Integrations, Pick: append([]string(nil), c.Pick...), Omit: append([]string(nil), c.Omit...), Rename: c.Rename, Append: c.Append, Prepend: c.Prepend, Flatten: append([]string(nil), c.Flatten...), Strict: c.Strict}
	for _, tr := range c.Transforms {
		out.Transforms = append(out.Transforms, DataTransform{Field: tr.Field, Expr: tr.Expr, Op: tr.Op, Arg: tr.Arg})
	}
	for _, fl := range c.Filters {
		out.Filters = append(out.Filters, DataFilter{Expr: fl.Expr, Mode: fl.Mode})
	}
	return out
}

func buildWorkflow(c WorkflowConfig) (*Workflow, error) {
	if c.ID == "" || c.First == "" {
		return nil, errors.New("workflow id and first are required")
	}
	if c.Mode == "" {
		c.Mode = ModeInline
	}
	wf := &Workflow{ID: c.ID, Name: c.Name, Version: c.Version, First: c.First, Debug: c.Debug, MaxVisits: c.MaxVisits, Mode: c.Mode, InputData: buildDataSpec(c.InputData), OutputData: buildDataSpec(c.OutputData), MigrationPolicy: c.MigrationPolicy, Nodes: map[string]*Node{}, Outgoing: map[string][]*Edge{}, Incoming: map[string][]*Edge{}, FanIn: map[string][]*Edge{}, Metadata: map[string]any{}}
	if wf.MaxVisits <= 0 {
		wf.MaxVisits = 256
	}
	for _, nr := range c.Notifications {
		wf.Notifications = append(wf.Notifications, buildNotificationRule(nr))
	}
	for _, r := range c.Rules {
		wf.Rules = append(wf.Rules, buildTaskRule(r))
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
		wf.Nodes[nc.ID] = &Node{ID: nc.ID, Type: nc.Type, Handler: nc.Handler, Workflow: nc.Workflow, Mode: nc.Mode, Await: await, Timeout: d, Retry: nc.Retry, RetryPolicy: rp, Last: nc.Last, Pause: nc.Pause, When: nc.When, Condition: nc.Condition, SkipOnFalse: nc.SkipOnFalse, ContinueOnError: nc.ContinueOnError, Compensate: nc.Compensate, OnError: nc.OnError, OnTimeout: nc.OnTimeout, Pool: nc.Pool, Priority: nc.Priority, RateLimit: rl, CircuitBreaker: cb, Params: nc.Params, Script: nc.Script, InputSchema: nc.InputSchema, OutputSchema: nc.OutputSchema, FailurePolicy: FailurePolicy{Strategy: nc.FailurePolicy.Strategy, ErrorNode: nc.FailurePolicy.ErrorNode, FallbackNode: nc.FailurePolicy.FallbackNode}, InputData: buildDataSpec(nc.InputData), OutputData: buildDataSpec(nc.OutputData)}
		for _, nr := range nc.Notifications {
			wf.Nodes[nc.ID].Notifications = append(wf.Nodes[nc.ID].Notifications, buildNotificationRule(nr))
		}
		for _, r := range nc.Rules {
			wf.Nodes[nc.ID].Rules = append(wf.Nodes[nc.ID].Rules, buildTaskRule(r))
		}
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
		edge := &Edge{ID: ec.ID, From: sources[0], To: targets[0], Sources: sources, Targets: targets, Type: ec.Type, When: ec.When, Condition: ec.Condition, Strategy: ec.Strategy, MaxConcurrency: ec.MaxConcurrency, FailFast: ec.FailFast, Await: await, Timeout: timeout, Quorum: ec.Quorum, CancelLosers: ec.CancelLosers, Map: ec.Map, Data: buildDataSpec(ec.Data)}
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
