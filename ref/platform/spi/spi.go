// Package spi carries the portable adapter contracts of the REF no-code
// platform. It deliberately imports nothing outside the standard library so a
// host or a contrib module can implement a Redis cache, a Kafka queue, an S3
// object store or an SES mailer while depending on this package alone — never
// on the BCL compiler, the REF engine, or fh itself.
//
// Every contract here is satisfied by at least one built-in provider in
// ref/platform, so an adapter author has a working reference implementation to
// compare against. Optional capabilities are expressed as separate, narrow
// interfaces (CachePrefix, QueueDelay, ...) which the platform probes for with
// a type assertion: an adapter that does not implement one simply does not
// enable the features that need it, and the BCL compiler says so at load time
// rather than failing at request time.
package spi

import (
	"context"
	"errors"
	"io"
	"slices"
	"time"
)

// ErrUnsupported is what an adapter returns from a method it cannot honour.
// The platform maps it to a 501-shaped failure rather than a 500, so an
// operator can tell "this backend cannot do that" from "this backend broke".
var ErrUnsupported = errors.New("spi: operation is not supported by this provider")

// ErrConflict reports an optimistic-concurrency or unique-constraint loss.
// Stores must return it (or wrap it) so the process engine can retry instead
// of failing a run.
var ErrConflict = errors.New("spi: conflicting concurrent write")

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

// Cache is the portable cache contract. fh's own kv.Store satisfies it
// unchanged, as can Redis, Memcached, DynamoDB or a SQL table.
//
// A missing or expired key is (nil, false, nil) — not an error. A ttl <= 0
// means "no expiry of its own".
type Cache interface {
	Get(key string) ([]byte, bool, error)
	Set(key string, value []byte, ttl time.Duration) error
	Delete(key string) error
}

// CacheContext is the richer contract an adapter may also implement. When a
// provider satisfies it the platform prefers these methods, so request
// cancellation and deadlines actually reach the backend. Adapters over a
// network service should implement it; in-process caches need not.
type CacheContext interface {
	GetContext(ctx context.Context, key string) ([]byte, bool, error)
	SetContext(ctx context.Context, key string, value []byte, ttl time.Duration) error
	DeleteContext(ctx context.Context, key string) error
}

// CachePrefix enables the cache.invalidate_prefix action. It reports how many
// entries it removed. Providers that cannot enumerate keys must not implement
// it; BCL referencing invalidate_prefix against such a provider fails to
// compile rather than silently invalidating nothing.
type CachePrefix interface {
	DeletePrefix(prefix string) (int, error)
}

// ---------------------------------------------------------------------------
// Coordination
// ---------------------------------------------------------------------------

// Locker is a mutual-exclusion primitive with a mandatory lease so a crashed
// holder cannot wedge a key forever. Acquire reports whether the lock was
// taken; it must never block indefinitely.
//
// The returned token must be presented to Release and Refresh: it prevents a
// process whose lease already expired from releasing the new holder's lock.
type Locker interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (token string, acquired bool, err error)
	Release(ctx context.Context, key, token string) error
	Refresh(ctx context.Context, key, token string, ttl time.Duration) (bool, error)
}

// RateLimiter is a fixed- or sliding-window counter. Allow reports whether the
// call is within limit, how many remain, and when the window resets. It must
// be atomic across replicas for any provider that claims to be shared.
type RateLimiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (allowed bool, remaining int, resetAt time.Time, err error)
}

// CircuitBreaker guards an unreliable dependency. Allow reports whether the
// circuit is closed (or half-open and admitting a probe); Record feeds the
// outcome back.
type CircuitBreaker interface {
	Allow(ctx context.Context, key string) (bool, error)
	Record(ctx context.Context, key string, success bool) error
}

// ---------------------------------------------------------------------------
// Queues
// ---------------------------------------------------------------------------

// JobQueue is the publication surface used by the queue.publish action and by
// the process engine when it schedules its own advance work. fh.DurableQueue
// satisfies it.
type JobQueue interface {
	Enqueue(jobType string, payload any, headers ...map[string]string) (string, error)
}

// QueueDelay is the optional contract behind queue.publish_delayed, process
// timers and retry backoff. A provider without it cannot host timers, and the
// compiler rejects a process whose store needs them.
type QueueDelay interface {
	EnqueueDelayed(jobType string, payload any, runAt time.Time, headers ...map[string]string) (string, error)
}

// QueueConsume lets the platform bind job types to intents and start
// consumption. fh.DurableQueue satisfies it.
type QueueConsume interface {
	Register(jobType string, handler func(context.Context, *Job) error)
	Start() error
}

// Job is the transport-neutral shape a consumer receives. Providers that have
// a richer native job type convert into this at the boundary.
type Job struct {
	ID          string
	Type        string
	Payload     []byte
	Headers     map[string]string
	Attempts    int
	MaxAttempts int
	EnqueuedAt  time.Time
}

// ---------------------------------------------------------------------------
// Object storage
// ---------------------------------------------------------------------------

// ObjectStore is the portable blob contract: local filesystem, a SQL blob
// table, S3, GCS or Azure Blob.
type ObjectStore interface {
	Put(ctx context.Context, key string, body io.Reader, contentType string) (ObjectInfo, error)
	Get(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string, limit int) ([]ObjectInfo, error)
}

// ObjectPresigner is optional. Providers that cannot mint time-limited URLs
// must not implement it; storage.presign then fails to compile against them.
type ObjectPresigner interface {
	Presign(ctx context.Context, key string, method string, ttl time.Duration) (string, error)
}

// ObjectInfo is the metadata every provider can report.
type ObjectInfo struct {
	Key         string    `json:"key"`
	Size        int64     `json:"size"`
	ContentType string    `json:"content_type,omitempty"`
	ETag        string    `json:"etag,omitempty"`
	ModifiedAt  time.Time `json:"modified_at,omitzero"`
}

// ---------------------------------------------------------------------------
// Outbound services
// ---------------------------------------------------------------------------

// Mailer sends transactional mail. net/smtp backs the built-in provider; SES,
// Postmark and SendGrid adapters implement the same two methods.
type Mailer interface {
	Send(ctx context.Context, msg Mail) (string, error)
}

// Mail is a single message. Body and HTML may both be set for a multipart
// alternative; at least one is required.
type Mail struct {
	From        string            `json:"from,omitempty"`
	To          []string          `json:"to"`
	CC          []string          `json:"cc,omitempty"`
	BCC         []string          `json:"bcc,omitempty"`
	ReplyTo     string            `json:"reply_to,omitempty"`
	Subject     string            `json:"subject"`
	Body        string            `json:"body,omitempty"`
	HTML        string            `json:"html,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Attachments []MailAttachment  `json:"attachments,omitempty"`
}

// MailAttachment is an inline attachment. Content is raw bytes, never base64.
type MailAttachment struct {
	Filename    string `json:"filename"`
	ContentType string `json:"content_type,omitempty"`
	Content     []byte `json:"content"`
}

// Notifier is a generic outbound channel: chat, SMS, push, or a webhook. It is
// what notify.send routes to once a channel name resolves to a provider.
type Notifier interface {
	Notify(ctx context.Context, msg Notification) (string, error)
}

// Notification is channel-agnostic on purpose: Target is whatever the channel
// addresses (a phone number, a chat room, a device token, a URL).
type Notification struct {
	Target  string            `json:"target"`
	Subject string            `json:"subject,omitempty"`
	Body    string            `json:"body"`
	Data    map[string]any    `json:"data,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

// ---------------------------------------------------------------------------
// Model providers
// ---------------------------------------------------------------------------

// LLM is the chat and embedding contract. The built-in provider speaks the
// OpenAI-shaped and Anthropic-shaped HTTP APIs over an allowlisted host.
type LLM interface {
	Chat(ctx context.Context, req ChatRequest) (ChatResponse, error)
}

// Embedder is split out because plenty of deployments have a chat model and an
// embedding model from different providers.
type Embedder interface {
	Embed(ctx context.Context, model string, inputs []string) ([][]float32, error)
}

// ChatRequest is provider-neutral. Model may be empty to accept the
// resource's configured default.
type ChatRequest struct {
	Model       string         `json:"model,omitempty"`
	System      string         `json:"system,omitempty"`
	Messages    []ChatMessage  `json:"messages"`
	Temperature *float64       `json:"temperature,omitempty"`
	MaxTokens   int            `json:"max_tokens,omitempty"`
	Stop        []string       `json:"stop,omitempty"`
	JSONSchema  map[string]any `json:"json_schema,omitempty"`
}

// ChatMessage roles are "system", "user" and "assistant".
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatResponse reports the text plus whatever usage the provider disclosed, so
// a cost policy can be enforced on real numbers.
type ChatResponse struct {
	Text         string `json:"text"`
	Model        string `json:"model,omitempty"`
	FinishReason string `json:"finish_reason,omitempty"`
	InputTokens  int    `json:"input_tokens,omitempty"`
	OutputTokens int    `json:"output_tokens,omitempty"`
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

// SearchIndex keeps documents queryable. The built-in provider is SQL-backed;
// OpenSearch, Typesense and Meilisearch adapters fit the same three methods.
type SearchIndex interface {
	Index(ctx context.Context, collection string, docs []SearchDoc) error
	Delete(ctx context.Context, collection string, ids []string) error
	Search(ctx context.Context, collection string, q SearchQuery) (SearchResult, error)
}

// SearchDoc is one indexable record. Fields is what gets matched; Payload is
// returned verbatim on a hit.
type SearchDoc struct {
	ID      string         `json:"id"`
	Fields  map[string]any `json:"fields"`
	Payload map[string]any `json:"payload,omitempty"`
	Tenant  string         `json:"tenant,omitempty"`
}

// SearchQuery is intentionally small: full-text terms plus exact filters,
// which is the intersection every backend supports well.
type SearchQuery struct {
	Text    string         `json:"text,omitempty"`
	Filters map[string]any `json:"filters,omitempty"`
	Tenant  string         `json:"tenant,omitempty"`
	Limit   int            `json:"limit,omitempty"`
	Offset  int            `json:"offset,omitempty"`
	OrderBy string         `json:"order_by,omitempty"`
	Desc    bool           `json:"desc,omitempty"`
}

// SearchResult carries the page plus the unpaged total when the provider knows
// it (-1 when it does not).
type SearchResult struct {
	Hits  []SearchHit `json:"hits"`
	Total int         `json:"total"`
}

// SearchHit pairs a document with its relevance score.
type SearchHit struct {
	ID      string         `json:"id"`
	Score   float64        `json:"score"`
	Payload map[string]any `json:"payload,omitempty"`
}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

// Principal is the serializable identity the platform exposes to no-code
// nodes. It is deliberately flat and JSON-safe: it crosses a queue boundary
// into a worker and gets persisted on a process run.
type Principal struct {
	ID       string         `json:"id"`
	Username string         `json:"username,omitempty"`
	Email    string         `json:"email,omitempty"`
	TenantID string         `json:"tenant_id,omitempty"`
	Roles    []string       `json:"roles,omitempty"`
	Scopes   []string       `json:"scopes,omitempty"`
	Claims   map[string]any `json:"claims,omitempty"`
}

// HasRole reports whether the principal holds role, case-sensitively.
func (p Principal) HasRole(role string) bool { return slices.Contains(p.Roles, role) }

// HasScope reports whether the principal holds scope, case-sensitively.
func (p Principal) HasScope(scope string) bool { return slices.Contains(p.Scopes, scope) }

// Credentials is what a transport extracted from a request before any identity
// is known. An Authenticator consumes it; nothing else should.
type Credentials struct {
	BearerToken string            `json:"-"`
	APIKey      string            `json:"-"`
	Username    string            `json:"-"`
	Password    string            `json:"-"`
	SessionID   string            `json:"-"`
	Headers     map[string]string `json:"-"`
	RemoteIP    string            `json:"remote_ip,omitempty"`
	TLS         bool              `json:"tls,omitempty"`
}

// Authenticator turns credentials into a principal, or fails. It must not
// consult authorization rules — that is Authorizer's job — and it must treat
// an absent credential as a failure rather than an anonymous success.
type Authenticator interface {
	Authenticate(ctx context.Context, creds Credentials) (Principal, error)
}

// Authorizer answers permission questions for a principal. Implementations
// resolve role hierarchies and wildcards; the platform never does that itself.
type Authorizer interface {
	HasPermission(ctx context.Context, p Principal, permission string) (bool, error)
	Permissions(ctx context.Context, p Principal) ([]string, error)
}

// SecretStore resolves a named secret at load time. Values must never be
// logged or echoed back through an introspection surface.
type SecretStore interface {
	Secret(name string) (string, bool, error)
}

// ---------------------------------------------------------------------------
// Durable process storage
// ---------------------------------------------------------------------------

// ProcessStoreRef is a marker for the store contract the durable process
// engine needs. The full interface lives in ref/process, which owns the row
// shapes; it is referenced here only so a driver author can discover it. See
// ref/process.Store.
type ProcessStoreRef interface {
	ProcessStoreMarker()
}
